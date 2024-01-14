//! There is four step in drop a Python Object:
//! 1. run `__del__`
//! 2. clear weakref(if have any)
//! 3. run `drop_dealloc`(i.e. run destructor)
//! The third step may vary also `drop` and `dealloc` may be separated, so we have two function here

use crate::common::lock::PyRwLockWriteGuard;

#[cfg(feature = "gc_bacon")]
use crate::object::gc::{utils::GcStatus, GcHeaderInner};
use crate::object::{core::PyInner, payload::PyObjectPayload, PyObject};
use std::ptr::NonNull;

/// partially drop(without deallocate) a object's field using `drop_in_place`
macro_rules! partially_drop {
    ($OBJ: ident. $($(#[$attr:meta])? $FIELD: ident),*) => {
        $(
            $(#[$attr])?
            NonNull::from(&$OBJ.$FIELD).as_ptr().drop_in_place();
        )*
    };
}

/// drop only(doesn't deallocate)
/// NOTE: `ref_count` is not drop here to prevent UB
pub(super) unsafe fn drop_only_obj<T: PyObjectPayload>(x: *mut PyObject) {
    let obj = &*x.cast::<PyInner<T>>();
    partially_drop!(obj.typeid, typ, dict, slots, payload);
}

/// deallocate memory with type info(cast as PyInner<T>) in heap only, DOES NOT run destructor
/// # Safety
/// - should only be called after its' destructor is done(i.e. called `drop_value`(which called drop_in_place))
/// - panic on a null pointer
/// move drop `header` here to prevent UB
pub(super) unsafe fn dealloc_only_obj<T: PyObjectPayload>(x: *mut PyObject) {
    {
        let obj = &*x.cast::<PyInner<T>>();
        // no need to drop weak list or vtable, it's a pointer
        partially_drop!(obj.ref_count);
    } // don't want keep a ref to a to be deallocated object
    std::alloc::dealloc(
        x.cast(),
        std::alloc::Layout::for_value(&*x.cast::<PyInner<T>>()),
    );
}

impl PyObject {
    /// Decrement the reference count of the object.
    /// And try to drop accordingly
    /// TODO(discord9): cfg
    pub fn dec_try_drop(ptr: NonNull<Self>) {
        #[cfg(not(feature = "gc_bacon"))]
        {
            let zelf = unsafe { ptr.as_ref() };
            if zelf.0.ref_count.dec() {
                unsafe { PyObject::drop_slow(ptr) }
            }
            return;
        }
        #[cfg(feature = "gc_bacon")]
        {
            // wrap zelf in scope to prevent UB from invalid reference
            let stat = {
                let zelf = unsafe { ptr.as_ref() };
                let gc = { zelf.header().gc() };
                gc.decrement(zelf)
            };
            match stat {
                GcStatus::ShouldDrop => unsafe {
                    PyObject::drop_slow(ptr);
                },
                GcStatus::BufferedDrop => unsafe {
                    PyObject::drop_only(ptr);
                },
                GcStatus::ShouldKeep | GcStatus::DoNothing => (),
            }
        }
    }
}

impl PyObject {
    pub(in crate::object) fn is_traceable(&self) -> bool {
        self.0.vtable.trace.is_some()
    }
    /// acquire a header with write lock
    #[cfg(feature = "gc_bacon")]
    pub(in crate::object) fn header(&self) -> PyRwLockWriteGuard<GcHeaderInner> {
        self.0.ref_count.header()
    }

    /// call `drop_only` in vtable, which run drop but not dealloc
    /// might fail to drop due to been resurrected by __del__ which return false
    pub(in crate::object) unsafe fn drop_only(ptr: NonNull<PyObject>) -> bool {
        if let Err(()) = ptr.as_ref().drop_slow_inner() {
            // abort drop because it's resurrected by __del__, hence rc is not zero
            return false;
        }
        let drop_only = ptr.as_ref().0.vtable.drop_only;
        // call drop only when there are no references in scope - stacked borrows stuff
        drop_only(ptr.as_ptr());
        true
    }

    /// call `dealloc_only` in vtable, which dealloc but not drop
    pub(in crate::object) unsafe fn dealloc_only(ptr: NonNull<PyObject>) -> bool {
        let dealloc_only = ptr.as_ref().0.vtable.dealloc_only;
        // call drop only when there are no references in scope - stacked borrows stuff
        dealloc_only(ptr.as_ptr());
        true
    }
}
