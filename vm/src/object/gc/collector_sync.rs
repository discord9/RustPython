use std::{
    alloc::{dealloc, Layout},
    fmt,
    ops::Deref,
    ptr::{self, NonNull},
    sync::{Mutex, Arc},
};

use crate::object::gc::header::Color;
use crate::object::gc::trace::GcObjPtr;
use crate::object::gc::GcStatus;

/// only use for roots's pointer to object, mark as safe to send
#[repr(transparent)]
pub(crate) struct WrappedPtr<T: ?Sized>(NonNull<T>);
unsafe impl<T: ?Sized> Send for WrappedPtr<T> {}
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
use once_cell::sync::Lazy;
/// The global cycle collector, which collect cycle references for PyInner<T>
pub static GLOBAL_COLLECTOR: Lazy<Arc<CcSync>> = Lazy::new(||Arc::new(CcSync {
    roots: Mutex::new(Vec::new()),
    pause: Mutex::new(()),
}));

#[derive(Debug, Default)]
pub struct CcSync {
    roots: Mutex<Vec<WrappedPtr<dyn GcObjPtr>>>,
    pub pause: Mutex<()>,
}
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
    pub fn gc(&self) {
        if self.should_gc() {
            self.collect_cycles();
        }
    }
    /// TODO: change to use roots'len or what to determine
    pub fn should_gc(&self) -> bool {
        true
    }
    pub fn increment(&self, obj: ObjRef) {
        obj.header().inc();
        obj.header().set_color(Color::Black);
    }

    /// # Safety
    /// if the last ref to a object call decrement() on object,
    /// then this object should be considered freed.
    pub unsafe fn decrement(&self, obj: ObjRef) {
        // TODO: change to return status if not buffered
        if obj.header().rc() > 0 {
            // prevent RAII Drop to drop below zero
            let rc = obj.header().dec();
            if rc == 0 {
                self.release(obj);
            } else {
                self.possible_root(obj);
            }
        }
    }

    unsafe fn release(&self, obj: ObjRef) {
        // because drop obj itself will drop all ObjRef store by object itself once more,
        // so balance out in here
        // by doing nothing
        //obj.trace(&mut |ch| {

        // self.decrement(ch);
        //});
        obj.header().set_color(Color::Black);
        // TODO make it do nothing
        if !obj.header().buffered() {
            unsafe { free(obj.as_ptr()) }
        }
    }

    fn possible_root(&self, obj: ObjRef) {
        if obj.header().color() != Color::Purple {
            obj.header().set_color(Color::Purple);
            if !obj.header().buffered() {
                obj.header().set_buffered(true);
                let mut roots = self.roots.lock().unwrap();
                roots.push(obj.as_ptr().into());
            }
        }
    }

    fn collect_cycles(&self) {
        let lock = self.pause.lock().unwrap();
        self.mark_roots();
        self.scan_roots();
        // drop lock in here (where the lock should be check in every deref() for ObjectRef)
        // to not stop the world,  drop() for object can happen
        // also what's left for collection should already be in garbage cycle,
        // no mutator will operate on them
        drop(lock);
        self.collect_roots();
    }

    fn mark_roots(&self) {
        dbg!(&self.roots);
        let roots: Vec<_> = { self.roots.lock().unwrap().drain(..).collect() };
        *self.roots.lock().unwrap() = roots
            .into_iter()
            .filter(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                if obj.header().color() == Color::Purple {
                    self.mark_gray(obj);
                    true
                } else {
                    obj.header().set_buffered(false);
                    if obj.header().color() == Color::Black && obj.rc() == 0 {
                        unsafe {
                            free(ptr.0);
                            // obj is dangling after this line?
                        }
                    }
                    false
                }
            })
            .collect();
    }
    fn scan_roots(&self) {
        self.roots
            .lock()
            .unwrap()
            .iter()
            .map(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                self.scan(obj);
            })
            .count();
    }
    fn collect_roots(&self) {
        // Collecting the nodes into this Vec is difference from the original
        // Bacon-Rajan paper. We need this because we have destructors(RAII) and
        // running them during traversal will cause cycles to be broken which
        // ruins the rest of our traversal.
        let mut white = Vec::new();
        let roots: Vec<_> = { self.roots.lock().unwrap().drain(..).collect() };
        roots
            .into_iter()
            .map(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                obj.header().set_buffered(false);
                self.collect_white(obj, &mut white);
            })
            .count();
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
            unsafe {
                drop_value(*i);
            }
        }
        // drop first, deallocate later so to avoid heap corruption
        // cause by access pointer of already dropped value's mem?
        for i in &white {
            unsafe { free(*i) }
        }
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
        dbg!();
        obj.trace(&mut |ch| {
            ch.header().inc();
            if ch.header().color() != Color::Black {
                self.scan_black(ch)
            }
        });
        dbg!();
    }
}
