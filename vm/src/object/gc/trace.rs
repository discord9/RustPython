use crate::object::gc::header::GcHeader;
use crate::object::PyObjectPayload;
use crate::{PyObjectRef, PyRef};
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
    /// must make sure that every owned object(Every stored `PyObjectRef` to be exactly) is called with tracer_fn once,
    ///  or garbage collect won't act correctly and very likely to _**panic**_ or deadlock.
    /// ```
    /// for ch in childs:
    ///     tracer_fn(ch)
    /// ```
    /// *DO NOT* clone a `PyObjectRef`, use `as_ptr()` instead and operate on NonNull
    fn trace(&self, tracer_fn: &mut TracerFn);
}

/// A `TracerFn` is a callback function that is invoked for each `PyGcObjectRef` owned
/// by an instance of something.
pub type TracerFn<'a> = dyn FnMut(&dyn GcObjPtr) + 'a;

impl<T: PyObjectPayload> GcTrace for PyRef<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn(&(**self))
    }
}

impl<T: PyObjectPayload> GcTrace for Option<PyRef<T>> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        if let Some(v) = self{
            v.trace(tracer_fn);
        }
    }
}

impl<T: PyObjectPayload> GcTrace for Vec<PyRef<T>> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        for elem in self{
            elem.trace(tracer_fn);
        }
    }
}

impl GcTrace for PyObjectRef {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn(self.as_ref())
    }
}

impl GcTrace for Option<PyObjectRef> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        if let Some(v) = self {
            v.trace(tracer_fn);
        }
    }
}

impl GcTrace for Vec<PyObjectRef> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        for elem in self{
            elem.trace(tracer_fn);
        }
    }
}
