//! There is four step in drop a Python Object:
//! 1. run `__del__`
//! 2. clear weakref(if have any)
//! 3. run `drop_dealloc`(i.e. run destructor)
//! The third step may vary also `drop` and `dealloc` may be separated, so we have two function here

use rustpython_common::lock::PyRwLockWriteGuard;

use super::super::{core::PyInner, payload::PyObjectPayload, PyObject};
use crate::object::gc::header::GcHeaderInner;
use std::ptr::NonNull;

/// Try to run both `drop` and `dealloc` for a object
unsafe fn drop_dealloc_obj<T: PyObjectPayload>(x: *mut PyObject) {
    if (*x).header().buffered() {
        error!("Try to drop&dealloc a buffered object! Drop only for now!");
        drop_only_obj::<T>(x);
    } else {
        drop(Box::from_raw(x as *mut PyInner<T>));
    }
}

macro_rules! partially_drop {
    ($OBJ: ident. $($(#[$attr:meta])? $FIELD: ident),*) => {
        $(
            $(#[$attr])?
            NonNull::from(&$OBJ.$FIELD).as_ptr().drop_in_place();
        )*
    };
}

/// drop only(doesn't deallocate)
/// NOTE: `header` is not drop to prevent UB
unsafe fn drop_only_obj<T: PyObjectPayload>(x: *mut PyObject) {
    let obj = &*x.cast::<PyInner<T>>();
    partially_drop!(obj.typeid, typ, dict, slots, payload);
}

/// deallocate memory with type info(cast as PyInner<T>) in heap only, DOES NOT run destructor
/// # Safety
/// - should only be called after its' destructor is done(i.e. called `drop_value`(which called drop_in_place))
/// - panic on a null pointer
/// move drop `header` here to prevent UB
unsafe fn dealloc_only_obj<T: PyObjectPayload>(x: *mut PyObject) {
    {
        let obj = &*x.cast::<PyInner<T>>();
        // partially_drop!(obj.header, vtable, weak_list);
    } // don't want keep a ref to a to be deallocated object
    std::alloc::dealloc(
        x.cast(),
        std::alloc::Layout::for_value(&*x.cast::<PyInner<T>>()),
    );
}

impl PyObject {
    pub(in crate::object) fn is_traceable(&self) -> bool {
        self.0.vtable.trace.is_some()
    }
    /// only clear weakref and then run rust RAII destructor, no `__del__` neither dealloc
    pub(in crate::object) unsafe fn drop_clr_wr(ptr: NonNull<PyObject>) -> bool {
        todo!()
    }
    /// acquire a header with write lock
    pub(in crate::object) fn header(&self) -> PyRwLockWriteGuard<GcHeaderInner> {
        todo!()
    }

    /// call `drop_only` in vtable, which run drop but not dealloc
    pub(in crate::object) unsafe fn drop_only(ptr: NonNull<PyObject>) {
        todo!()
    }

    /// call `dealloc_only` in vtable, which dealloc but not drop
    pub(in crate::object) unsafe fn dealloc_only(ptr: NonNull<PyObject>) -> bool {
        todo!()
    }
}
