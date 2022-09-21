use std::{ptr::NonNull, sync::Arc};

use crate::{
    object::gc::{CcSync, GcHeader},
    PyObjectRef,
};

pub struct PyGcObject {
    gc: Arc<CcSync>,
    header: GcHeader,
    /// the best way is to modify PyInner's RefCount field,
    /// but take a ref for simpler proof of concept
    inner: PyObjectRef,
}

#[derive(Debug)]
pub struct PyGcObjectRef {
    ptr: NonNull<PyGcObject>,
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
