use rustpython_common::lock::{PyMutex, PyRwLock};

use crate::object::gc::header::GcHeader;
use crate::object::PyObjectPayload;
use crate::{AsObject, PyObjectRef, PyRef};
use core::ptr::NonNull;

/// indicate what to do with the object afer calling dec()
#[derive(PartialEq, Eq)]
pub enum GcStatus {
    /// should be drop by caller
    ShouldDrop,
    /// already buffered, will be drop by collector, no more action is required at caller
    Buffered,
    /// should keep and not drop by caller
    ShouldKeep,
}
pub trait GcObjPtr: GcTrace {
    fn inc(&self);
    fn dec(&self) -> GcStatus;
    fn rc(&self) -> usize;
    /// return object header
    fn header(&self) -> &GcHeader;
    // as a NonNull pointer to a gc managed object
    fn as_ptr(&self) -> NonNull<dyn GcObjPtr>;
}

/// use `trace()` to call on all owned ObjectRef
pub trait GcTrace {
    /// call tracer_fn for every GcOjbect owned by a dyn GcTrace Object
    /// # API Contract
    /// must make sure that every owned object(Every stored `PyObjectRef` to be exactly) is called with tracer_fn **at most once**.
    ///
    /// if some field is not called, the worse results is memory leak, but if some field is called repeatly, panic and deadlock can happen.
    ///
    /// Note that Two `PyObjectRef` to the Same `PyObject` still count as two Ref, and should be called twice(once for each one) in this case.
    ///
    /// ```ignore
    /// for ch in childs:
    ///     tracer_fn(ch)
    /// ```
    /// _**DO NOT**_ clone a `PyObjectRef`(which mess up the ref count system) in trace(), use ref or, if actually had to, use `as_ptr()`(which is a last resort and better not to use) instead and operate on NonNull
    fn trace(&self, tracer_fn: &mut TracerFn);
}

/// A `TracerFn` is a callback function that is invoked for each `PyGcObjectRef` owned
/// by an instance of something.
pub type TracerFn<'a> = dyn FnMut(&dyn GcObjPtr) + 'a;

impl GcTrace for PyObjectRef {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn(self.as_ref())
    }
}

impl GcTrace for () {
    #[inline]
    fn trace(&self, _tracer_fn: &mut TracerFn) {}
}

impl<T: PyObjectPayload> GcTrace for PyRef<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn((*self).as_object())
    }
}

impl<T: GcTrace> GcTrace for Option<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        if let Some(v) = self {
            v.trace(tracer_fn);
        }
    }
}

impl<T> GcTrace for [T]
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

impl<T: GcTrace> GcTrace for PyMutex<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        self.lock().trace(tracer_fn);
    }
}
// FIXME(discord9): does a RwLock<T:GcTrace> actually own this ObjectRef? Need to rethink

impl<T: GcTrace> GcTrace for PyRwLock<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        self.read().trace(tracer_fn);
    }
}

// TODO(discord9): impl_tuples!(impossible with declarative macros)
// TODO(discord9): GcTrace as a derive proc macro
impl<A: GcTrace, B: GcTrace> GcTrace for (A, B) {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        self.0.trace(tracer_fn);
        self.1.trace(tracer_fn);
    }
}
