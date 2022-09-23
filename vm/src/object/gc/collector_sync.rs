/// based on paper:http://link.springer.com/10.1007/3-540-45337-7_12
/// and crate: https://github.com/fitzgen/bacon-rajan-cc
/// for a simple ref count cycle collector
/// TODO(discord9): make a on-the-fly version based on doi:10.1145/1255450.1255453
use std::{
    alloc::{dealloc, Layout},
    fmt,
    ops::Deref,
    ptr::{self, NonNull},
    sync::{Arc, Mutex},
};

use crate::object::gc::header::Color;
use crate::object::gc::trace::GcObjPtr;
use crate::object::gc::GcStatus;

use once_cell::sync::Lazy;
use rustpython_common::{lock::PyRwLock, rc::PyRc};
use std::cell::Cell;
thread_local! {
    /// assume any drop() impl doesn't create new thread, so gc only work in this one thread.
    pub static IS_GC_THREAD: Cell<bool> = Cell::new(false);
}
/// The global cycle collector, which collect cycle references for PyInner<T>
pub static GLOBAL_COLLECTOR: Lazy<PyRc<CcSync>> = Lazy::new(|| {
    PyRc::new(CcSync {
        roots: Mutex::new(Vec::new()),
        pause: Mutex::new(()),
        gc_threads_num: PyRwLock::new(0),
    })
});

/// only use for roots's pointer to object, mark `NonNull` as safe to send
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

#[derive(Debug, Default)]
pub struct CcSync {
    roots: Mutex<Vec<WrappedPtr<dyn GcObjPtr>>>,
    /// for stop the world, will be try to check lock every time deref ObjecteRef
    /// to achive pausing
    pub pause: Mutex<()>,
    pub gc_threads_num: PyRwLock<isize>,
}
type ObjRef<'a> = &'a dyn GcObjPtr;
type ObjPtr = NonNull<dyn GcObjPtr>;

impl Drop for CcSync {
    fn drop(&mut self) {
        // force a gc before drop
        self.collect_cycles();
    }
}

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

    pub fn vis_debug(&self) {
        for root in self.roots.lock().unwrap().iter(){
            let childs = self.get_childs(unsafe{ root.0.as_ref() });
            dbg!(root);
            dbg!(childs);
        }
    }

    fn get_childs(&self, obj: ObjRef) -> Vec<WrappedPtr<dyn GcObjPtr>>{
        let mut ret = Vec::new();
        obj.trace(&mut |ch|{
            ret.push(ch.as_ptr().into())
        });
        ret
    }

    /// _suggest_(may or may not) collector to collect garbage.
    #[inline]
    pub fn gc(&self) {
        if self.should_gc() {
            self.collect_cycles();
        }
    }
    fn roots_len(&self) -> usize {
        self.roots.lock().unwrap().len()
    }
    /// TODO: change to use roots'len or what to determine
    pub fn should_gc(&self) -> bool {
        self.roots_len() > 100
    }
    pub fn increment(&self, obj: ObjRef) {
        obj.header().inc();
        obj.header().set_color(Color::Black);
        self.gc();
    }

    /// # Safety
    /// if the last ref to a object call decrement() on object,
    /// then this object should be considered freed.
    pub unsafe fn decrement(&self, obj: ObjRef) -> GcStatus {
        // TODO(discord9): find a better place for gc()
        if obj.header().rc() > 0 {
            // prevent RAII Drop to drop below zero
            let rc = obj.header().dec();
            if rc == 0 {
                self.release(obj)
            } else {
                self.possible_root(obj);
                GcStatus::ShouldKeep
            }
        } else {
            // FIXME(discord9): confirm if rc==0 then should drop
            GcStatus::CallerDrop
        }
    }

    unsafe fn release(&self, obj: ObjRef) -> GcStatus {
        // because drop obj itself will drop all ObjRef store by object itself once more,
        // so balance out in here
        // by doing nothing
        //obj.trace(&mut |ch| {

        // self.decrement(ch);
        //});
        obj.header().set_color(Color::Black);
        // before it is free in here,
        // but now change to passing message to allow it to drop outside
        if !obj.header().buffered() {
            GcStatus::CallerDrop
            // unsafe { free(obj.as_ptr()) }
        } else {
            // self.gc();
            GcStatus::BufferedDrop
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
        if IS_GC_THREAD.with(|v| v.get()) {
            return;
            // already call collect_cycle() once
        }
        let lock = self.pause.lock().unwrap();
        IS_GC_THREAD.with(|v| v.set(true));
        {
            *self.gc_threads_num.write() += 1;
        }
        warn!(
            "Start collect_cycle() with len()={}, gc_threads_num={}",
            self.roots_len(),
            self.gc_threads_num.read()
        );
        self.mark_roots();
        self.scan_roots();
        // drop lock in here (where the lock should be check in every deref() for ObjectRef)
        // to not stop the world, so drop() in collect_roots() for object can happen
        // also what's left for collection should already be in garbage cycle,
        // no mutator will operate on them
        drop(lock);
        IS_GC_THREAD.with(|v| v.set(false));
        {
            *self.gc_threads_num.write() -= 1;
        }
        self.collect_roots();
        warn!("End collect_cycle() with len()={}", self.roots_len());
    }

    fn mark_roots(&self) {
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
                            // can drop directly because no one is refering it
                            // (unlike in collect_white where drop_in_place first and deallocate later)
                            Box::from_raw(ptr.0.as_ptr());
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

        #[cfg(debug_assertions)]
        let non_empty = true;
        #[cfg(debug_assertions)]
        if non_empty {
            warn!(
                "Start collect_roots() collect cyclic garbage in white.len()={}",
                white.len()
            );
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
            unsafe {
                drop_value(*i);
            }
        }
        // drop first, deallocate later so to avoid heap corruption
        // cause by circular ref and therefore
        // access pointer of already dropped value's mem?
        for i in &white {
            unsafe { free(*i) }
        }

        #[cfg(debug_assertions)]
        if non_empty {
            warn!("End collect_roots(). Done free cyclic garbage.");
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
                #[cfg(debug_assertions)]
                if ch.header().rc() == 0 {
                    dbg!(ch.header());
                    dbg!(obj.as_ptr());
                    dbg!(ch.as_ptr());
                    warn!("A gray object have a child that should be free, something is wrong.");
                    dbg!(self.get_childs(obj));
                }

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
