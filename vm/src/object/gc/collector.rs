use super::header::Color;
use crate::common::lock::{PyMutex, PyMutexGuard, PyRwLock, PyRwLockWriteGuard};
use crate::common::rc::PyRc;
use crate::object::gc::utils::{GcResult, GcStatus};
use crate::object::gc::GcObj;
use crate::object::gc::GcObjRef;
use crate::object::Traverse;
use crate::PyObject;
use std::ptr::NonNull;
use std::time::Instant;

/// The global cycle collector, which collect cycle references for PyInner<T>
#[cfg(feature = "threading")]
pub static GLOBAL_COLLECTOR: once_cell::sync::Lazy<PyRc<Collector>> =
    once_cell::sync::Lazy::new(|| PyRc::new(Default::default()));

#[cfg(not(feature = "threading"))]
thread_local! {
    pub static GLOBAL_COLLECTOR: PyRc<Collector> = PyRc::new(Default::default());
}

/// only use for roots's pointer to object, mark `NonNull` as safe to send
/// which is true only if when we are holding a gc pause lock
/// but essentially, we are using raw pointer here only because we know only one thread can be doing GC at a time
#[repr(transparent)]
struct WrappedPtr(NonNull<PyObject>);
unsafe impl Send for WrappedPtr {}
impl std::ops::Deref for WrappedPtr {
    type Target = NonNull<PyObject>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for WrappedPtr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl From<NonNull<PyObject>> for WrappedPtr {
    fn from(ptr: NonNull<PyObject>) -> Self {
        Self(ptr)
    }
}

impl From<WrappedPtr> for NonNull<PyObject> {
    fn from(w: WrappedPtr) -> Self {
        w.0
    }
}

pub struct Collector {
    roots: PyMutex<Vec<WrappedPtr>>,
    /// for stop the world, will be try to check lock every time deref ObjecteRef
    /// to achive pausing
    pause: PyRwLock<()>,
    last_gc_time: PyMutex<Instant>,
    is_enabled: PyMutex<bool>,
    /// acquire this to prevent a new gc to happen before this gc is completed
    /// but also resume-the-world early
    cleanup_cycle: PyMutex<()>,
}

impl std::fmt::Debug for Collector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CcSync")
            .field(
                "roots",
                &format!("[{} objects in buffer]", self.roots_len()),
            )
            .field("pause", &self.pause)
            .field("last_gc_time", &self.last_gc_time)
            .finish()
    }
}

impl Default for Collector {
    fn default() -> Self {
        Self {
            roots: Default::default(),
            pause: Default::default(),
            last_gc_time: PyMutex::new(Instant::now()),
            is_enabled: PyMutex::new(true),
            cleanup_cycle: PyMutex::new(()),
        }
    }
}

// core of gc algorithm
impl Collector {
    /// `force` means a explicit call to `gc.collect()`, which will try to acquire a lock for a little longer
    /// and have a higher chance to actually collect garbage
    #[allow(unused)]
    fn collect_cycles(&self, force: bool) -> GcResult {
        // acquire stop-the-world lock
        let lock = {
            #[cfg(feature = "threading")]
            {
                // if can't access pause lock for a second, return because gc is not that emergency,
                // also normal call to `gc.collect()` can usually acquire that lock unless something is wrong
                if force {
                    // if is forced to gc, wait a while for write lock
                    match self.pause.try_write_for(std::time::Duration::from_secs(1)) {
                        Some(v) => v,
                        None => {
                            warn!("Can't acquire lock to stop the world after waiting.");
                            return (0, 0).into();
                        }
                    }
                } else {
                    // if not forced to gc, a non-blocking check to see if gc is possible
                    match self.pause.try_write() {
                        Some(v) => v,
                        None => {
                            warn!("Fast GC fail to acquire write lock.");
                            return (0, 0).into();
                        }
                    }
                }
            }
            // also when no threading, there is actually no need to get a lock,(because every thread have it's own gc)
            // but for the sake of using same code(and defendsive), we acquire one anyway
            #[cfg(not(feature = "threading"))]
            {
                // when not threading, no deadlock should occur?
                let _force = force;
                self.pause.try_write().unwrap()
            }
        };

        // if is empty or last gc's cleanup is not done, return early
        if self.roots_len() == 0 || self.cleanup_cycle.is_locked() {
            return (0, 0).into();
        }

        // the three main step of GC
        // 1. mark roots: which get trial DECREF object so cycles get zero rc
        // 2. scan roots: get non-cycle object back to normal ref cnt
        // 3. collect roots: collect cycles starting from roots
        let freed = Self::mark_roots(&mut *self.roots.lock());
        Self::scan_roots(&mut *self.roots.lock());
        let ret_cycle = self.collect_roots(self.roots.lock(), lock);
        (freed, ret_cycle).into()
    }

    fn mark_roots<R>(mut roots: R) -> usize
    where
        R: AsMut<Vec<WrappedPtr>>,
    {
        let mut freed = 0;
        let old_roots: Vec<_> = { roots.as_mut().drain(..).collect() };
        let mut new_roots = old_roots
            .into_iter()
            .filter(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                let header = obj.header();
                if header.color() == Color::Purple {
                    drop(header);
                    Self::mark_gray(obj);
                    true
                } else {
                    obj.header().set_buffered(false);
                    if obj.header().color() == Color::Black && obj.header().rc() == 0 {
                        freed += 1;
                        unsafe {
                            // only dealloc here, because already drop(only) in Object's impl Drop
                            // PyObject::dealloc_only(ptr.cast::<PyObject>());
                            let ret = PyObject::dealloc_only(**ptr);
                            debug_assert!(ret);
                            // obj is dangling after this line?
                        }
                    }
                    false
                }
            })
            .collect();
        roots.as_mut().append(&mut new_roots);
        freed
    }

    fn mark_gray(obj: GcObjRef) {
        if obj.header().color() != Color::Gray {
            obj.header().set_color(Color::Gray);
            obj.traverse(&mut |ch| {
                if ch.header().is_leaked() {
                    return;
                }
                ch.header().dec();
                Self::mark_gray(ch);
            });
        }
    }

    fn scan_roots<R>(mut roots: R)
    where
        R: AsMut<Vec<WrappedPtr>>,
    {
        roots
            .as_mut()
            .iter()
            .map(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                Self::scan(obj);
            })
            .count();
    }

    fn scan(obj: GcObjRef) {
        if obj.header().color() == Color::Gray {
            if obj.header().rc() > 0 {
                Self::scan_black(obj)
            } else {
                obj.header().set_color(Color::White);
                obj.traverse(&mut |ch| {
                    if ch.header().is_leaked() {
                        return;
                    }
                    Self::scan(ch);
                });
            }
        }
    }

    fn scan_black(obj: GcObjRef) {
        obj.header().set_color(Color::Black);
        obj.traverse(&mut |ch| {
            if ch.header().is_leaked() {
                return;
            }
            ch.header().inc();
            if ch.header().color() != Color::Black {
                debug_assert!(
                    ch.header().color() == Color::Gray || ch.header().color() == Color::White
                );
                Self::scan_black(ch)
            }
        });
    }

    fn collect_roots(
        &self,
        mut roots_lock: PyMutexGuard<Vec<WrappedPtr>>,
        lock: PyRwLockWriteGuard<()>,
    ) -> usize {
        // Collecting the nodes into this Vec is difference from the original
        // Bacon-Rajan paper. We need this because we have destructors(RAII) and
        // running them during traversal will cause cycles to be broken which
        // ruins the rest of our traversal.
        let mut white = Vec::new();
        let roots: Vec<_> = { roots_lock.drain(..).collect() };
        // future inc/dec will accesss roots so drop lock in here
        drop(roots_lock);
        // release gc pause lock in here, for after this line no white garbage will be access by mutator
        roots
            .into_iter()
            .map(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                obj.header().set_buffered(false);
                Self::collect_white(obj, &mut white);
            })
            .count();
        let len_white = white.len();
        if !white.is_empty() {
            info!("Cyclic garbage collected, count={}", white.len());
        }

        // mark the end of GC, but another gc can only begin after acquire cleanup_cycle lock
        // because a dead cycle can't actively change object graph anymore
        let _cleanup_lock = self.cleanup_cycle.lock();
        // unlock fair so high freq gc wouldn't stop the world forever
        #[cfg(feature = "threading")]
        PyRwLockWriteGuard::unlock_fair(lock);
        #[cfg(not(feature = "threading"))]
        drop(lock);

        self.free_cycles(white);

        len_white
    }
    fn collect_white(obj: GcObjRef, white: &mut Vec<NonNull<GcObj>>) {
        if obj.header().color() == Color::White && !obj.header().buffered() {
            obj.header().set_color(Color::Black);
            obj.header().set_in_cycle(true);
            obj.traverse(&mut |ch| {
                if ch.header().is_leaked() {
                    return;
                }
                Self::collect_white(ch, white)
            });
            white.push(NonNull::from(obj));
        }
    }
    /// TODO: change to use weak_ref count to prevent premature dealloc in cycles
    /// free everything in white, safe to use even when those objects form cycle refs
    fn free_cycles(&self, white: Vec<NonNull<PyObject>>) -> usize {
        // TODO: maybe never run __del__ anyway, for running a __del__ function is an implementation detail!!!!
        // TODO: impl PEP 442
        // 0. count&mark cycle with indexies
        // 0.5. add back one ref for all thing in white
        // 1. clear weakref
        // 2. run del
        // 3. check if cycle is still isolated(using mark_roots&scan_roots), remember to acquire gc's exclusive lock to prevent graph from change
        // (atomic op required, maybe acquire a lock on them?
        //or if a object dead immediate before even incref, it will be wrongly revived, but if rc is added back, that should be ok)
        // 4. drop the still isolated cycle(which is confirmed garbage), then dealloc them

        // Run drop on each of nodes.
        white.iter().for_each(|i| {
            // Calling drop() will decrement the reference count on any of our live children.
            // However, during trial deletion the reference count was already decremented
            // so we'll end up decrementing twice. To avoid that, we increment the count
            // just before calling drop() so that it balances out. This is another difference
            // from the original paper caused by having destructors that we need to run.

            let obj = unsafe { i.as_ref() };
            obj.traverse(&mut |ch| {
                if ch.header().is_leaked() {
                    return;
                }
                if ch.header().rc() > 0 {
                    ch.header().inc();
                }
            });
        });
        // drop all for once at seperate loop to avoid certain cycle ref double drop bug
        let can_deallocs: Vec<_> = white
            .iter()
            .map(|i| unsafe { PyObject::drop_clr_wr(*i) })
            .collect();
        // drop first, deallocate later so to avoid heap corruption
        // cause by circular ref and therefore
        // access pointer of already dropped value's memory region
        white
            .iter()
            .zip(can_deallocs)
            .map(|(i, can_dealloc)| {
                if can_dealloc {
                    let ret = unsafe { PyObject::dealloc_only(*i) };
                    debug_assert!(ret);
                }
            })
            .count();
        info!("Cyclic garbage collected, count={}", white.len());
        white.len()
    }
}

// utility function for `Collector`
impl Collector {
    fn roots_len(&self) -> usize {
        self.roots.lock().len()
    }

    // basic inc/dec operation
    pub fn increment(&self, obj: &PyObject) {
        if obj.header().is_leaked() {
            return;
        }
        // acquire exclusive access to obj's header

        // TODO: this might not be necessary
        // prevent starting a gc in the middle of change header state
        // let _lock_gc = obj.header().try_pausing();

        let mut header = obj.header();
        header.inc_black();
    }

    /// if the last ref to a object call decrement() on object,
    /// then this object should be considered freed. However it's backing alllocating
    /// _might_ not be freed immediately.
    /// the actual detail of drop is decoupled from this function for cleaner code
    pub fn decrement(&self, obj: &PyObject) -> GcStatus {
        let mut header = obj.header();
        if header.is_leaked() {
            // a leaked object should always keep
            return GcStatus::ShouldKeep;
        }

        // acquire exclusive access to obj's header, so no decrement in the middle of increment of vice versa

        // prevent RAII Drop to drop below zero
        if header.rc() > 0 {
            let rc = header.dec();
            if rc == 0 {
                self.release(obj)
            } else if obj.is_traceable() {
                // only buffer traceable(and not leaked) object for that is where we can detect cycles
                self.possible_root(obj);
                GcStatus::ShouldKeep
            } else {
                // if is not traceable, which could be actually acyclic or not, keep them anyway
                GcStatus::ShouldKeep
            }
        } else {
            // FIXME(discord9): confirm if already rc==0 then should do nothing
            GcStatus::DoNothing
        }
    }
}

/// dealing with drop and marking roots
impl Collector {
    fn release(&self, obj: &PyObject) -> GcStatus {
        // because drop obj itself will drop all ObjRef store by object itself once more,
        // so balance out in here
        // by doing nothing
        // instead of minus one and do:
        // ```ignore
        // obj.trace(&mut |ch| {
        //   self.decrement(ch);
        // });
        //```
        let mut header = obj.header();
        header.set_color(Color::Black);

        // TODO(discord9): just drop in here, not by the caller, which is cleaner
        // before it is free in here,
        // but now change to passing message to allow it to drop outside
        match (header.buffered(), header.in_cycle()) {
            (true, _) => GcStatus::BufferedDrop,
            (_, true) => GcStatus::GarbageCycle,
            (false, false) => GcStatus::ShouldDrop,
        }
    }

    fn possible_root(&self, obj: &PyObject) {
        let mut header = obj.header();
        if header.color() != Color::Purple {
            header.set_color(Color::Purple);
            // prevent add to buffer for multiple times
            let mut roots = self.roots.lock();
            if !header.buffered() {
                header.set_buffered(true);
                roots.push(NonNull::from(obj).into());
            }
        }
    }
}
