use rustpython_common::{
    lock::{PyMutex, PyRwLock, PyRwLockReadGuard, PyRwLockWriteGuard},
    rc::PyRc,
};
use std::{
    cell::Cell,
    ptr::NonNull,
    time::{Duration, Instant},
};

use crate::{
    object::gc::{Color, GcResult, Trace},
    PyObject,
};

use super::{GcObj, GcObjRef, GcStatus, TraceHelper};

pub static LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// The global cycle collector, which collect cycle references for PyInner<T>
#[cfg(feature = "threading")]
pub static GLOBAL_COLLECTOR: once_cell::sync::Lazy<PyRc<Collector>> =
    once_cell::sync::Lazy::new(|| PyRc::new(Default::default()));

#[cfg(not(feature = "threading"))]
thread_local! {
    pub static GLOBAL_COLLECTOR: PyRc<Collector> = PyRc::new(Default::default());
}

/// only use for roots's pointer to object, mark `NonNull` as safe to send
#[repr(transparent)]
struct WrappedPtr(NonNull<PyObject>);
unsafe impl Send for WrappedPtr {}
unsafe impl Sync for WrappedPtr {}
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
        }
    }
}

// core of gc algorithm
impl Collector {
    /*
    unsafe fn drop_dealloc(obj: NonNull<PyObject>) {
        PyObject::drop_slow(obj)
    }
     */
    unsafe fn drop_only(obj: NonNull<PyObject>) -> bool {
        PyObject::drop_only(obj)
    }
    unsafe fn dealloc_only(obj: NonNull<PyObject>) -> bool {
        PyObject::dealloc_only(obj)
    }
    fn collect_cycles(&self, force: bool) -> GcResult {
        if Self::IS_GC_THREAD.with(|v| v.get()) {
            return (0, 0).into();
            // already call collect_cycle() once
        }

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
                            warn!("Can't acquire lock to stop the world, stop gc now.");
                            return (0, 0).into();
                        }
                    }
                } else {
                    match self.pause.try_write() {
                        Some(v) => v,
                        None => {
                            warn!("Fast GC fail to acquire write lock, stop gc now.");
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
        // order of acquire lock and check IS_GC_THREAD here is important
        // This prevent set multiple IS_GC_THREAD thread local variable to true
        // using write() to gain exclusive access
        Self::IS_GC_THREAD.with(|v| v.set(true));
        let freed = self.mark_roots();
        self.scan_roots();
        let ret_cycle = self.collect_roots(lock);
        (freed, ret_cycle).into()
    }

    fn mark_roots(&self) -> usize {
        let mut freed = 0;
        let old_roots: Vec<_> = { self.roots.lock().drain(..).collect() };
        let mut new_roots = old_roots
            .into_iter()
            .filter(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                if obj.header().color() == Color::Purple {
                    self.mark_gray(obj);
                    true
                } else {
                    obj.header().set_buffered(false);
                    if obj.header().color() == Color::Black && obj.header().rc() == 0 {
                        freed += 1;
                        unsafe {
                            // only dealloc here, because already drop(only) in Object's impl Drop
                            // PyObject::dealloc_only(ptr.cast::<PyObject>());
                            Self::dealloc_only(**ptr);
                            // obj is dangling after this line?
                        }
                    }
                    false
                }
            })
            .collect();
        (*self.roots.lock()).append(&mut new_roots);
        freed
    }

    fn mark_gray(&self, obj: GcObjRef) {
        if obj.header().color() != Color::Gray {
            obj.header().set_color(Color::Gray);
            obj.trace(&mut |ch| {
                if ch.header().is_leaked() {
                    return;
                }
                ch.header().dec();
                self.mark_gray(ch);
            });
        }
    }

    fn scan_roots(&self) {
        self.roots
            .lock()
            .iter()
            .map(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                self.scan(obj);
            })
            .count();
    }

    fn scan(&self, obj: GcObjRef) {
        if obj.header().color() == Color::Gray {
            if obj.header().rc() > 0 {
                self.scan_black(obj)
            } else {
                obj.header().set_color(Color::White);
                obj.trace(&mut |ch| {
                    if ch.header().is_leaked() {
                        return;
                    }
                    self.scan(ch);
                });
            }
        }
    }

    fn scan_black(&self, obj: GcObjRef) {
        obj.header().set_color(Color::Black);
        obj.trace(&mut |ch| {
            if ch.header().is_leaked() {
                return;
            }
            ch.header().inc();
            if ch.header().color() != Color::Black {
                debug_assert!(
                    ch.header().color() == Color::Gray || ch.header().color() == Color::White
                );
                self.scan_black(ch)
            }
        });
    }

    fn collect_roots(&self, lock: PyRwLockWriteGuard<()>) -> usize {
        // Collecting the nodes into this Vec is difference from the original
        // Bacon-Rajan paper. We need this because we have destructors(RAII) and
        // running them during traversal will cause cycles to be broken which
        // ruins the rest of our traversal.
        let mut white = Vec::new();
        let roots: Vec<_> = { self.roots.lock().drain(..).collect() };
        // release gc pause lock in here, for after this line no white garbage will be access by mutator
        roots
            .into_iter()
            .map(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                obj.header().set_buffered(false);
                self.collect_white(obj, &mut white);
            })
            .count();
        let len_white = white.len();
        if !white.is_empty() {
            warn!("Cyclic garbage collected, count={}", white.len());
        }
        // Run drop on each of nodes.
        for i in &white {
            // Calling drop() will decrement the reference count on any of our live children.
            // However, during trial deletion the reference count was already decremented
            // so we'll end up decrementing twice. To avoid that, we increment the count
            // just before calling drop() so that it balances out. This is another difference
            // from the original paper caused by having destructors that we need to run.
            let obj = unsafe { i.as_ref() };
            obj.trace(&mut |ch| {
                if ch.header().is_leaked() {
                    return;
                }
                debug_assert!(!ch.header().is_dealloc());
                if ch.header().rc() > 0 {
                    ch.header().inc();
                }
            });
        }

        // drop all for once in seperate loop to avoid certain cycle ref double drop bug
        // TODO: check and add detailed explain(could be something with __del__ func do)
        let can_dealloc: Vec<_> = white
            .iter()
            .map(|i| {
                unsafe {
                    Self::drop_only(*i)
                    // PyObject::drop_only(i.cast::<PyObject>());
                }
            })
            .collect();

        // drop first, deallocate later so to avoid heap corruption
        // cause by circular ref and therefore
        // access pointer of already dropped value's memory region
        white
            .iter()
            .zip(can_dealloc)
            .map(|(i, can_dealloc)| {
                if can_dealloc {
                    unsafe {
                        Self::dealloc_only(*i);
                    }
                }
            })
            .count();

        // mark the end of GC here so another gc can begin(if end early could lead to stack overflow)
        Self::IS_GC_THREAD.with(|v| v.set(false));
        drop(lock);

        len_white
    }
    fn collect_white(&self, obj: GcObjRef, white: &mut Vec<NonNull<GcObj>>) {
        if obj.header().color() == Color::White && !obj.header().buffered() {
            obj.header().set_color(Color::Black);
            obj.header().set_in_cycle(true);
            obj.trace(&mut |ch| {
                if ch.header().is_leaked() {
                    return;
                }
                self.collect_white(ch, white)
            });
            white.push(NonNull::from(obj));
        }
    }
}

// inc/dec
impl Collector {
    pub fn increment(&self, obj: &PyObject) {
        if obj.header().is_leaked() {
            return;
        }
        // acquire exclusive access to obj's header
        #[cfg(feature = "threading")]
        let _lock = obj.header().exclusive();
        obj.header().do_pausing();
        obj.header().inc();
        obj.header().set_color(Color::Black);
    }

    /// if the last ref to a object call decrement() on object,
    /// then this object should be considered freed.
    pub fn decrement(&self, obj: &PyObject) -> GcStatus {
        if obj.header().is_leaked() {
            // a leaked object should always keep
            return GcStatus::ShouldKeep;
        }

        // acquire exclusive access to obj's header
        #[cfg(feature = "threading")]
        let _lock = obj.header().exclusive();
        // prevent RAII Drop to drop below zero
        if obj.header().rc() > 0 {
            obj.header().do_pausing();
            let rc = obj.header().dec();
            if rc == 0 {
                self.release(obj)
            } else if TraceHelper::is_traceable(obj.inner_typeid()) && !obj.header().is_leaked() {
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
        obj.header().set_color(Color::Black);

        // TODO(discord9): just drop in here, not by the caller, which is cleaner
        // before it is free in here,
        // but now change to passing message to allow it to drop outside
        match (obj.header().buffered(), obj.header().is_in_cycle()) {
            (true, _) => GcStatus::BufferedDrop,
            (_, true) => GcStatus::GarbageCycle,
            (false, false) => GcStatus::ShouldDrop,
        }
    }

    fn possible_root(&self, obj: &PyObject) {
        if obj.header().color() != Color::Purple {
            obj.header().set_color(Color::Purple);
            // prevent add to buffer for multiple times
            let mut roots = self.roots.lock();
            let header = obj.header();
            if !header.buffered() {
                header.set_buffered(true);
                roots.push(NonNull::from(obj).into());
            }
        }
    }
}

// methods about gc condition
impl Collector {
    #[inline]
    fn roots_len(&self) -> usize {
        self.roots.lock().len()
    }

    #[inline]
    pub(crate) fn is_enabled(&self) -> bool {
        *self.is_enabled.lock()
    }
    #[inline]
    pub(crate) fn enable(&self) {
        *self.is_enabled.lock() = true
    }
    #[inline]
    pub(crate) fn disable(&self) {
        *self.is_enabled.lock() = false
    }

    #[inline]
    pub fn force_gc(&self) -> GcResult {
        self.collect_cycles(true)
    }

    #[inline]
    #[allow(unreachable_code)]
    pub fn should_gc(&self) -> bool {
        if !self.is_enabled() {
            return false;
        }
        // if can't acquire lock, some other thread is already in gc
        if self.pause.try_write().is_none() {
            return false;
        }
        // FIXME(discord9): better condition, could be important
        if self.roots_len() > 700 {
            if Self::IS_GC_THREAD.with(|v| v.get()) {
                // Already in gc, return early
                return false;
            }
            let mut last_gc_time = self.last_gc_time.lock();
            if last_gc_time.elapsed().as_millis() >= 100 {
                *last_gc_time = Instant::now();
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// _suggest_(may or may not) collector to collect garbage. return number of cyclic garbage being collected
    #[inline]
    pub fn fast_try_gc(&self) -> GcResult {
        if self.should_gc() {
            self.collect_cycles(false)
        } else {
            (0, 0).into()
        }
    }
}

// methods about stopping the world
impl Collector {
    thread_local! {
        /// only set to true when start a gc in thread, assume any drop() impl doesn't create new thread, so gc only work in this one thread.
        pub static IS_GC_THREAD: Cell<bool> = Cell::new(false);
    }

    /// This function will block if is a garbage collect is happening
    pub fn do_pausing(&self) {
        // if there is no multi-thread, there is no need to pause,
        // for running different vm in different thread will make sure them have no depend whatsoever
        #[cfg(feature = "threading")]
        {
            if Self::IS_GC_THREAD.with(|v| v.get()) {
                // if is same thread, then this thread is already stop by gc itself,
                // no need to block.
                // and any call to do_pausing is probably from drop() or what so allow it to continue execute.
                return;
            }

            // however when gc-ing the object graph must stay the same so check and try to lock until gc is done
            // timeout is to prevent dead lock(which is worse than panic?)
            let _lock = self
                .pause
                .try_read_recursive_for(LOCK_TIMEOUT)
                .expect("Could be in deadlock!");
        }
        // when not threading, one could still run multiple vm on multiple thread(which have a GC per thread)
        // but when call `gc()`, it automatically pause the world for this thread.
        // so nothing need to be done, pausing is only useful for threading
    }

    /// similar to do_pausing,
    ///
    /// but instead return a ReadGuard for covering more critical region if needed
    pub fn try_pausing(&self) -> Option<PyRwLockReadGuard<()>> {
        #[cfg(feature = "threading")]
        {
            if Self::IS_GC_THREAD.with(|v| v.get()) {
                // if is same thread, then this thread is already stop by gc itself,
                // no need to block.
                // and any call to do_pausing is probably from drop() or what so allow it to continue execute.
                return None;
            }
            Some(
                self.pause
                    .try_read_recursive_for(LOCK_TIMEOUT)
                    .expect("Could be in deadlock"),
            )
        }
        #[cfg(not(feature = "threading"))]
        return None;
    }
}
