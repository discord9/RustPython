use core::ptr::NonNull;
use crate::object::gc::header::GcHeader;

/// indicate what to do with the object afer calling dec()
#[derive(PartialEq, Eq)]
pub enum GcStatus{
    /// should be drop by caller
    ShouldDrop,
    /// already buffered, will be drop by collector, no more action is required at caller
    Buffered,
    /// should keep and not drop by caller
    ShouldKeep
}
pub trait GcObjPtr: GcTrace {
    fn inc(&self);
    fn dec(&self)->GcStatus;
    fn rc(&self) -> usize;
    /// return object header
    fn header(&self) -> &GcHeader;
    fn as_ptr(&self) -> NonNull<dyn GcObjPtr>;
}


pub trait GcTrace {
    /// call tracer_fn for every GcOjbect owned by a dyn GcTrace Object
    /// # API Contract
    /// must make sure that every owned object is called with tracer_fn, or garbage collect won't act correctly.
    fn trace(&self, tracer_fn: &mut TracerFn);
}

/// A `TracerFn` is a callback function that is invoked for each `PyGcObjectRef` owned
/// by an instance of something.
pub type TracerFn<'a> = dyn FnMut(&dyn GcObjPtr) + 'a;

use crate::builtins::PyList;

impl GcTrace for PyList{
    fn trace(&self, tracer_fn: &mut TracerFn) {
        for elem in self.borrow_vec().iter(){
            tracer_fn(elem.as_ref());
        }
    }
}