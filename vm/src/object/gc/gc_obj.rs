use std::{borrow::Borrow, ops::Deref, ptr::NonNull, sync::Arc};

use crate::{
    object::gc::{CcSync, GcHeader, GcObjPtr, GcTrace, TracerFn},
    PyObjectRef,
};

pub struct PyGcObject {
    gc: Arc<CcSync>,
    header: GcHeader,
    /// the best way is to modify PyInner's RefCount field,
    /// but take a ref for simpler proof of concept
    inner: PyObjectRef,
}

impl Deref for PyGcObject {
    type Target = PyObjectRef;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl GcTrace for PyGcObject {
    fn trace(&self, tracer_fn: &mut TracerFn) {
        // TODO: call trace for inner object
    }
}

impl GcObjPtr for PyGcObject {
    fn inc(&self) {
        self.gc.increment(self);
    }

    fn dec(&self) {
        unsafe {
            self.gc.decrement(self);
        }
    }

    fn rc(&self) -> usize {
        self.header().rc()
    }

    fn header(&self) -> &GcHeader {
        &self.header
    }

    fn as_ptr(&self) -> NonNull<dyn GcObjPtr> {
        NonNull::from(self)
    }
}

#[derive(Debug)]
pub struct PyGcObjectRef {
    ptr: NonNull<PyGcObject>,
}

impl Deref for PyGcObjectRef {
    type Target = PyGcObject;
    fn deref(&self) -> &Self::Target {
        let obj = unsafe {
            // Safe to assume this to be non-null here, as if it weren't true, we'd be breaking
            // the contract anyway.
            // This allows the null check to be elided in the destructor if we
            // manipulated the reference count in the same function.
            self.ptr.as_ref()
        };
        {
            // lock to check if gc is pausing the world
            // this is how stop-the-world happen
            let _lock = obj.gc.pause.lock().unwrap();
        }
        obj
    }
}

impl Borrow<PyGcObject> for PyGcObjectRef {
    fn borrow(&self) -> &PyGcObject {
        self
    }
}

impl PyGcObjectRef {
    fn new(gc: Arc<CcSync>, obj: PyObjectRef) -> Self {
        let obj = Box::new(PyGcObject {
            gc,
            header: GcHeader::new(),
            inner: obj,
        });
        let ptr = Box::into_raw(obj);
        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
        }
    }
    fn ptr(&self) -> NonNull<PyGcObject> {
        self.ptr
    }
}
