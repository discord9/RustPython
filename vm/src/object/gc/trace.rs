use rustpython_common::lock::PyRwLock;

use crate::{object::PyObjectPayload, AsObject, PyObject, PyObjectRef, PyRef};

pub type TracerFn = dyn FnMut(&PyObject);

/// # Safety
/// impl `trace()` with caution! Following those guideline so trace doesn't cause memory error!:
/// - Make sure that every owned object(Every PyObjectRef/PyRef) is called with tracer_fn **at most once**.
/// If some field is not called, the worst results is just memory leak,
/// but if some field is called repeatly, panic and deadlock can happen.
///
/// - _**DO NOT**_ clone a `PyObjectRef` or `Pyef<T>` in `trace()`
pub unsafe trait Trace {
    /// impl `trace()` with caution! Following those guideline so trace doesn't cause memory error!:
    /// - Make sure that every owned object(Every PyObjectRef/PyRef) is called with tracer_fn **at most once**.
    /// If some field is not called, the worst results is just memory leak,
    /// but if some field is called repeatly, panic and deadlock can happen.
    ///
    /// - _**DO NOT**_ clone a `PyObjectRef` or `Pyef<T>` in `trace()`
    fn trace(&self, tracer_fn: &mut TracerFn);
}

unsafe impl Trace for PyObjectRef {
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn(self)
    }
}

unsafe impl Trace for PyObject {
    fn trace(&self, tracer_fn: &mut TracerFn) {
        // TODO(discord9): move to core.rs
    }
}

unsafe impl Trace for () {
    fn trace(&self, _tracer_fn: &mut TracerFn) {}
}

unsafe impl<T: PyObjectPayload> Trace for PyRef<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn((*self).as_object())
    }
}

unsafe impl<T: Trace> Trace for Option<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        if let Some(v) = self {
            v.trace(tracer_fn);
        }
    }
}

unsafe impl<T> Trace for [T]
where
    T: Trace,
{
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        for elem in self {
            elem.trace(tracer_fn);
        }
    }
}

unsafe impl<T> Trace for Vec<T>
where
    T: Trace,
{
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        for elem in self {
            elem.trace(tracer_fn);
        }
    }
}

// DO NOT impl Trace on PyMutex
// because gc's tracing might recursively trace to itself, which cause dead lock on Mutex

unsafe impl<T: Trace> Trace for PyRwLock<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        match self.try_read_recursive() {
            Some(inner) => inner.trace(tracer_fn),
            // this means something else is holding the lock,
            // but since gc stopped the world, during gc the lock is always held
            // so it is safe to ignore those in gc
            None => (),
        }
    }
}

macro_rules! trace_tuple {
    ($(($NAME: ident, $NUM: tt)),*) => {
        unsafe impl<$($NAME: Trace),*> Trace for ($($NAME),*) {
            #[inline]
            fn trace(&self, tracer_fn: &mut TracerFn) {
                $(
                    self.$NUM.trace(tracer_fn);
                )*
            }
        }

    };
}

trace_tuple!((A, 0), (B, 1));
trace_tuple!((A, 0), (B, 1), (C, 2));
trace_tuple!((A, 0), (B, 1), (C, 2), (D, 3));
trace_tuple!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4));
trace_tuple!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4), (F, 5));
trace_tuple!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4), (F, 5), (G, 6));
