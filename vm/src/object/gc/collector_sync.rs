use std::sync::Arc;
use std::time::Instant;
use std::{
    alloc::{dealloc, Layout},
    fmt,
    ops::Deref,
    ptr::{self, NonNull},
};

use crate::object::gc::header::Color;
use crate::object::gc::trace::GcObjPtr;
use crate::object::gc::GcStatus;
use crate::object::{Erased, PyInner};
use crate::PyObject;

use rustpython_common::lock::{PyMutex, PyRwLock, PyRwLockWriteGuard};

use std::cell::Cell;
thread_local! {
    /// assume any drop() impl doesn't create new thread, so gc only work in this one thread.
    pub static IS_GC_THREAD: Cell<bool> = Cell::new(false);
}
/// The global cycle collector, which collect cycle references for PyInner<T>

#[cfg(feature = "threading")]
pub static GLOBAL_COLLECTOR: once_cell::sync::Lazy<Arc<CcSync>> =
    once_cell::sync::Lazy::new(|| {
        Arc::new(CcSync {
            roots: PyMutex::new(Vec::new()),
            pause: PyRwLock::new(()),
            last_gc_time: PyMutex::new(Instant::now()),
        })
    });

#[cfg(not(feature = "threading"))]
use rustpython_common::rc::PyRc;
#[cfg(not(feature = "threading"))]
thread_local! {
    pub static GLOBAL_COLLECTOR: PyRc<CcSync> = PyRc::new(CcSync {
        roots: PyMutex::new(Vec::new()),
        pause: PyRwLock::new(()),
        last_gc_time: PyMutex::new(Instant::now()),
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

pub struct CcSync {
    roots: PyMutex<Vec<WrappedPtr<dyn GcObjPtr>>>,
    /// for stop the world, will be try to check lock every time deref ObjecteRef
    /// to achive pausing
    pub pause: PyRwLock<()>,
    last_gc_time: PyMutex<Instant>,
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
type ObjRef<'a> = &'a dyn GcObjPtr;
type ObjPtr = NonNull<dyn GcObjPtr>;

unsafe fn drop_value(ptr: ObjPtr) {
    ptr::drop_in_place(ptr.as_ptr());
}

unsafe fn free(ptr: ObjPtr) {
    debug_assert!(ptr.as_ref().header().rc() == 0);
    debug_assert!(!ptr.as_ref().header().buffered());
    // Box::from_raw(ptr.as_ptr());
    dealloc(ptr.cast().as_ptr(), Layout::for_value(ptr.as_ref()));
}

impl CcSync {
    /// _suggest_(may or may not) collector to collect garbage. return number of cyclic garbage being collected
    ///
    /// TODO(discord9): find a better place for gc()
    #[inline]
    pub fn gc(&self) -> (usize, usize) {
        if self.should_gc() {
            self.force_gc()
        } else {
            (0, 0)
        }
    }
    #[inline]
    pub fn force_gc(&self) -> (usize, usize) {
        self.collect_cycles()
    }
    fn roots_len(&self) -> usize {
        self.roots.lock().len()
    }
    /// TODO: change to use roots'len or what to determine
    pub fn should_gc(&self) -> bool {
        let mut last_gc_time = self.last_gc_time.lock();
        // FIXME(discord9): better condition, could be important
        if last_gc_time.elapsed().as_millis() >= 10 {
            *last_gc_time = Instant::now();
            self.roots_len() > 700
        } else {
            false
        }
    }
    pub fn increment(&self, obj: ObjRef) {
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
    pub unsafe fn decrement(&self, obj: ObjRef) -> GcStatus {
        // FIXME(discord9): if a call to decrement is happening when gc() is called on annother thread, what lock should be done to ensure correctness?
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
            } else {
                self.possible_root(obj);
                GcStatus::ShouldKeep
            }
        } else {
            // FIXME(discord9): confirm if rc==0 then should drop
            GcStatus::ShouldDrop
        }
    }

    unsafe fn release(&self, obj: ObjRef) -> GcStatus {
        // because drop obj itself will drop all ObjRef store by object itself once more,
        // so balance out in here
        // by doing nothing
        // instead of minus one
        //obj.trace(&mut |ch| {

        // self.decrement(ch);
        //});
        obj.header().set_color(Color::Black);
        // before it is free in here,
        // but now change to passing message to allow it to drop outside
        if !obj.header().buffered() {
            GcStatus::ShouldDrop
        } else {
            GcStatus::BufferedDrop
        }
    }

    fn possible_root(&self, obj: ObjRef) {
        if obj.header().color() != Color::Purple {
            obj.header().set_color(Color::Purple);
            if !obj.header().buffered() {
                let _lock = obj.header().try_pausing();
                // lock here to serialize access to root&gc
                let mut roots = self.roots.lock();
                obj.header().set_buffered(true);
                roots.push(obj.as_ptr().into());
            }
        }
    }

    /// return `(acyclic garbage, cyclic garbage)`
    fn collect_cycles(&self) -> (usize, usize) {
        if IS_GC_THREAD.with(|v| v.get()) {
            return (0, 0);
            // already call collect_cycle() once
        }
        // order of acquire lock and check IS_GC_THREAD here is important
        // This prevent set multiple IS_GC_THREAD thread local variable to true
        // using write() to gain exclusive access
        let lock = self.pause.write();
        IS_GC_THREAD.with(|v| v.set(true));

        let freed = self.mark_roots();
        self.scan_roots();
        // drop lock in here (where the lock should be check in every deref() for ObjectRef)
        // to not stop the world
        // what's left for collection should already be in garbage cycle,
        // no mutator will operate on them
        (freed, self.collect_roots(lock))
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
                            // FIXME(discord9): find correct way to drop
                            // can drop directly because no one is refering it by definition
                            // (unlike in collect_white where drop_in_place first and deallocate later)
                            PyObject::drop_slow(ptr.cast::<PyObject>());
                            /*
                            if let Some(ptr) = ptr.0.as_ref().as_obj_ptr() {
                                warn!("A proper PyObject!");
                                PyObject::drop_slow(ptr)
                            }else{
                                warn!("A GcObjPtr didn't impl as_obj_ptr() therefore fall back to call its default drop impl");
                                // FIXME(discord9): wrong, without type layout info, this is wrong!
                                //drop(Box::from_raw(ptr.as_ptr()))
                            }
                             */
                            /*
                            drop_value(ptr.0);
                            free(ptr.0);
                             */
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
        IS_GC_THREAD.with(|v| v.set(false));
        drop(lock);

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
            // so to allow drop() to drop by itself
            obj.header().set_buffered(false);
            // FIXME: here drop is incorrect, for it is dropping PyObject with type information correctly.
            unsafe {
                // drop_value(*i);
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
        len_white
    }
    fn collect_white(&self, obj: ObjRef, white: &mut Vec<NonNull<dyn GcObjPtr>>) {
        if obj.header().color() == Color::White && !obj.header().buffered() {
            obj.header().set_color(Color::Black);
            obj.trace(&mut |ch| self.collect_white(ch, white));
            // because during trial deletion the reference count was already decremented.
            // and drop() dec once more, so inc it to balance out
            white.push(obj.as_ptr());
        }
    }
    fn mark_gray(&self, obj: ObjRef) {
        if obj.header().color() != Color::Gray {
            obj.header().set_color(Color::Gray);
            obj.trace(&mut |ch| {
                ch.header().dec();
                self.mark_gray(ch);
            });
        }
    }
    fn scan(&self, obj: ObjRef) {
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
    fn scan_black(&self, obj: ObjRef) {
        obj.header().set_color(Color::Black);
        obj.trace(&mut |ch| {
            ch.header().inc();
            if ch.header().color() != Color::Black {
                self.scan_black(ch)
            }
        });
    }
}
