use enum_dispatch::enum_dispatch;
use rustpython_common::lock::{PyMutex, PyRwLock};

use crate::object::gc::{deadlock_handler, header::GcHeader};
use crate::object::{Erased, PyInner, PyObjectPayload};
use crate::{AsObject, Py, PyObject, PyObjectRef, PyRef};
use core::ptr::NonNull;

/// indicate what to do with the object afer calling dec()
#[derive(PartialEq, Eq)]
pub enum GcStatus {
    /// should be drop by caller
    ShouldDrop,
    /// already buffered, will be dealloc by collector, caller should call `drop_only` to run destructor only but not dealloc memory region
    BufferedDrop,
    /// should keep and not drop by caller
    ShouldKeep,
}

#[enum_dispatch]
pub trait GcObjPtr: GcTrace {
    fn inc(&self);
    fn dec(&self) -> GcStatus;
    fn rc(&self) -> usize;
    /// return object header
    fn header(&self) -> &GcHeader;
    // as a NonNull pointer to a gc managed object
    fn as_ptr(&self) -> NonNull<dyn GcObjPtr>;
}

#[enum_dispatch(GcObjPtr)]
pub(in crate::object) enum GcObj<T: PyObjectPayload> {
    PyInner(PyInner<Erased>),
    PyObject(PyObject),
    Py(Py<T>),
}

unsafe impl<T: PyObjectPayload> GcTrace for GcObj<T> {
    fn trace(&self, tracer_fn: &mut TracerFn) {
        match self {
            GcObj::PyInner(v) => v.trace(tracer_fn),
            GcObj::PyObject(v) => v.trace(tracer_fn),
            GcObj::Py(v) => v.trace(tracer_fn),
        }
    }
}
/// use `trace()` to call on all owned ObjectRef
///
/// # Safety
///
/// see `trace()`'s requirement
pub unsafe trait GcTrace {
    /// call tracer_fn for every object(childrens) owned by a Object
    /// # Safety
    ///
    /// must make sure that every owned object(Every stored `PyObjectRef` to be exactly) is called with tracer_fn **at most once**.
    /// If some field is not called, the worst results is memory leak, but if some field is called repeatly, panic and deadlock can happen.
    ///
    /// _**DO NOT**_ clone a `PyObjectRef`(which mess up the ref count system) in `trace()`, use `ref`erence or, if actually had to, use `as_ptr()`(which is a last resort and better not to use) instead and operate on NonNull
    ///
    /// ```ignore
    /// for ch in childs:
    ///     tracer_fn(ch)
    /// ```
    ///
    /// Note that Two `PyObjectRef` to the Same `PyObject` still count as two Ref, and should be called twice(once for each one) in this case.
    fn trace(&self, tracer_fn: &mut TracerFn);
}

/// A `TracerFn` is a callback function that is invoked for each `PyGcObjectRef` owned
/// by an instance of something.
pub type TracerFn<'a> = dyn FnMut(&dyn GcObjPtr) + 'a;

unsafe impl GcTrace for PyObjectRef {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn(self.as_ref())
    }
}

unsafe impl GcTrace for () {
    #[inline]
    fn trace(&self, _tracer_fn: &mut TracerFn) {}
}

unsafe impl<T: PyObjectPayload> GcTrace for PyRef<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn((*self).as_object())
    }
}

unsafe impl<T: GcTrace> GcTrace for Option<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        if let Some(v) = self {
            v.trace(tracer_fn);
        }
    }
}

unsafe impl<T> GcTrace for [T]
where
    T: GcTrace,
{
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        for elem in self {
            elem.trace(tracer_fn);
        }
    }
}

unsafe impl<T> GcTrace for Vec<T>
where
    T: GcTrace,
{
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        for elem in self {
            elem.trace(tracer_fn);
        }
    }
}

unsafe impl<T: GcTrace> GcTrace for PyMutex<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        // FIXME(discord9): check if this may cause a deadlock or not
        match self.try_lock() {
            Some(v) => v.trace(tracer_fn),
            None => {
                error!("Could be in dead lock.");
                // not kill the thread for now
                // deadlock_handler()
            }
        }
    }
}

unsafe impl<T: GcTrace> GcTrace for PyRwLock<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        // FIXME(discord9): check if this may cause a deadlock or not, maybe try `recursive`?
        match self.try_read_recursive() {
            Some(v) => v.trace(tracer_fn),
            None => deadlock_handler(),
        }
    }
}

// TODO(discord9): impl_tuples!(impossible with declarative macros)
// TODO(discord9): GcTrace as a derive proc macro
unsafe impl<A: GcTrace, B: GcTrace> GcTrace for (A, B) {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        self.0.trace(tracer_fn);
        self.1.trace(tracer_fn);
    }
}
