use crate::{PyObject, PyObjectRef, PyRef, object::PyObjectPayload, AsObject};

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
    fn trace(&self, traceer_fn: &mut TracerFn);
}

unsafe impl Trace for PyObjectRef {
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn(self)
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

unsafe impl<T: Trace> Trace for PyRwLock<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        try_read_timeout(self, |inner| inner.trace(tracer_fn));
    }
}
