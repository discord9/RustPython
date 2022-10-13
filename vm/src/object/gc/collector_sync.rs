use std::any::Any;
use std::time::Instant;
use std::{fmt, ops::Deref, ptr::NonNull};

use crate::object::gc::{
    deadlock_handler, trace::TraceHelper, Color, GcObj, GcObjPtr, GcObjRef, GcResult, GcStatus,
    GcTrace, LOCK_TIMEOUT,
};
use crate::PyObject;

use rustpython_common::{
    lock::{PyMutex, PyRwLock, PyRwLockWriteGuard},
    rc::PyRc,
};

use std::cell::Cell;

/// The global cycle collector, which collect cycle references for PyInner<T>
#[cfg(feature = "threading")]
pub static GLOBAL_COLLECTOR: once_cell::sync::Lazy<PyRc<CcSync>> =
    once_cell::sync::Lazy::new(|| {
        PyRc::new(CcSync {
            roots: PyMutex::new(Vec::new()),
            pause: PyRwLock::new(()),
            last_gc_time: PyMutex::new(Instant::now()),
            is_enabled: PyMutex::new(true),
        })
    });

#[cfg(not(feature = "threading"))]
thread_local! {
    pub static GLOBAL_COLLECTOR: PyRc<CcSync> = PyRc::new(CcSync {
        roots: PyMutex::new(Vec::new()),
        pause: PyRwLock::new(()),
        last_gc_time: PyMutex::new(Instant::now()),
        is_enabled: PyMutex::new(true),
    });
}

/// only use for roots's pointer to object, mark `NonNull` as safe to send
#[repr(transparent)]
pub(crate) struct WrappedPtr<T: ?Sized>(NonNull<T>);
unsafe impl<T: ?Sized> Send for WrappedPtr<T> {}
unsafe impl<T: ?Sized> Sync for WrappedPtr<T> {}
impl<T: ?Sized> Deref for WrappedPtr<T> {
    type Target = NonNull<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> fmt::Debug for WrappedPtr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl<T: ?Sized> From<NonNull<T>> for WrappedPtr<T> {
    fn from(ptr: NonNull<T>) -> Self {
        Self(ptr)
    }
}

impl<T: ?Sized> From<WrappedPtr<T>> for NonNull<T> {
    fn from(w: WrappedPtr<T>) -> Self {
        w.0
    }
}

#[repr(C)]
pub struct CcSync {
    roots: PyMutex<Vec<WrappedPtr<GcObj>>>,
    /// for stop the world, will be try to check lock every time deref ObjecteRef
    /// to achive pausing
    pub pause: PyRwLock<()>,
    last_gc_time: PyMutex<Instant>,
    is_enabled: PyMutex<bool>,
}

impl std::fmt::Debug for CcSync {
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

// TODO: change to use PyInner<Erased> directly

impl CcSync {
    thread_local! {
        /// assume any drop() impl doesn't create new thread, so gc only work in this one thread.
        pub static IS_GC_THREAD: Cell<bool> = Cell::new(false);
    }
    /// _suggest_(may or may not) collector to collect garbage. return number of cyclic garbage being collected
    #[inline]
    pub fn gc(&self) -> GcResult {
        if self.should_gc() {
            self.force_gc()
        } else {
            (0, 0).into()
        }
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
        if self.is_enabled() {
            self.collect_cycles()
        } else {
            GcResult::new((0, 0))
        }
    }

    #[inline]
    fn roots_len(&self) -> usize {
        self.roots.lock().len()
    }

    /// TODO: change to use roots'len or what to determine
    #[inline]
    #[allow(unreachable_code)]
    pub fn should_gc(&self) -> bool {
        // TODO(discord9): remove later, just for debug
        // #[cfg(debug_assertions)]
        // return true;
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
    pub fn increment(&self, obj: GcObjRef) {
        if obj.header().is_leaked() {
            // by define a leaked object's rc should not change?
            return;
        }
        obj.header().do_pausing();
        obj.header().inc();
        obj.header().set_color(Color::Black);
    }

    /// # Safety
    /// if the last ref to a object call decrement() on object,
    /// then this object should be considered freed.
    pub unsafe fn decrement(&self, obj: GcObjRef) -> GcStatus {
        if obj.header().is_leaked() {
            // a leaked object should always keep
            return GcStatus::ShouldKeep;
        }
        // prevent RAII Drop to drop below zero
        if obj.header().rc() > 0 {
            obj.header().do_pausing();
            // acquire exclusive access to obj
            #[cfg(feature = "threading")]
            let _lock = obj.header().exclusive.lock();

            let rc = obj.header().dec();
            if rc == 0 {
                self.release(obj)
            } else if TraceHelper::is_traceable(obj.inner_typeid()) {
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

    unsafe fn release(&self, obj: GcObjRef) -> GcStatus {
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
        if !obj.header().buffered() {
            GcStatus::ShouldDrop
        } else {
            GcStatus::BufferedDrop
        }
    }

    fn possible_root(&self, obj: GcObjRef) {
        if obj.header().color() != Color::Purple {
            obj.header().set_color(Color::Purple);
            // prevent add to buffer for multiple times
            let mut buffered = obj.header().buffered.lock();
            if !*buffered {
                // let _lock = obj.header().try_pausing();
                // lock here to serialize access to root&gc
                let mut roots = self.roots.lock();
                *buffered = true;
                roots.push(NonNull::from(obj).into());
            }
        }
    }

    /// The core of garbage collect process, return `(acyclic garbage collected, cyclic garbage collected)`.
    fn collect_cycles(&self) -> GcResult {
        if Self::IS_GC_THREAD.with(|v| v.get()) {
            return (0, 0).into();
            // already call collect_cycle() once
        }

        debug!("GC begin.");
        // order of acquire lock and check IS_GC_THREAD here is important
        // This prevent set multiple IS_GC_THREAD thread local variable to true
        // using write() to gain exclusive access
        let lock = {
            #[cfg(feature = "threading")]
            {
                self.pause
                    .try_write_for(LOCK_TIMEOUT)
                    .unwrap_or_else(|| deadlock_handler())
            }

            // also when no threading, there is actually no need to get a lock,(because every thread have it's own gc)
            // but for the sake of using same code(and defendsive), we acquire one anyway
            #[cfg(not(feature = "threading"))]
            {
                self.pause.write()
            }
        };
        Self::IS_GC_THREAD.with(|v| v.set(true));
        debug!("mark begin.");
        let freed = self.mark_roots();
        debug!("mark done.");
        debug!("scan begin.");
        self.scan_roots();
        debug!("scan done.");
        // drop lock in here (where the lock should be check in every deref() for ObjectRef)
        // to not stop the world
        // what's left for collection should already be in garbage cycle,
        // no mutator will operate on them
        debug!("collect begin.");
        let ret = (freed, self.collect_roots(lock)).into();
        debug!("collect done.");
        debug!("GC done.");
        ret
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
                    if obj.header().color() == Color::Black && obj.rc() == 0 {
                        freed += 1;
                        unsafe {
                            // only dealloc here, because already drop(only) in Object's impl Drop
                            PyObject::dealloc_only(ptr.cast::<PyObject>());
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
            warn!("Collect cyclic garbage in white.len()={}", white.len());
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
                if ch.header().rc() > 0 {
                    ch.header().inc();
                }
            });
            unsafe {
                PyObject::drop_only(i.cast::<PyObject>());
            }
        }

        // drop first, deallocate later so to avoid heap corruption
        // cause by circular ref and therefore
        // access pointer of already dropped value's memory region
        for i in &white {
            unsafe {
                PyObject::dealloc_only(i.cast::<PyObject>());
            }
        }

        // mark the end of GC here so another gc can begin(if end early could lead to stack overflow)
        Self::IS_GC_THREAD.with(|v| v.set(false));
        drop(lock);

        len_white
    }
    fn collect_white(&self, obj: GcObjRef, white: &mut Vec<NonNull<GcObj>>) {
        if obj.header().color() == Color::White && !obj.header().buffered() {
            obj.header().set_color(Color::BlackFree);
            obj.trace(&mut |ch| self.collect_white(ch, white));
            white.push(NonNull::from(obj));
        }
    }
    fn mark_gray(&self, obj: GcObjRef) {
        if obj.header().color() != Color::Gray {
            obj.header().set_color(Color::Gray);
            obj.trace(&mut |ch| {
                ch.header().dec();
                self.mark_gray(ch);
            });
        }
    }
    fn scan(&self, obj: GcObjRef) {
        if obj.header().color() == Color::Gray {
            if obj.rc() > 0 {
                self.scan_black(obj)
            } else {
                obj.header().set_color(Color::White);
                obj.trace(&mut |ch| {
                    self.scan(ch);
                });
            }
        }
    }
    fn scan_black(&self, obj: GcObjRef) {
        obj.header().set_color(Color::Black);
        obj.trace(&mut |ch| {
            ch.header().inc();
            if ch.header().color() != Color::Black {
                self.scan_black(ch)
            }
        });
    }
}
