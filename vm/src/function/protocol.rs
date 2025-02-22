use super::IntoFuncArgs;
use crate::{
    builtins::{iter::PySequenceIterator, PyDict, PyDictRef},
    convert::ToPyObject,
    identifier,
    protocol::{PyIter, PyIterIter, PyMapping, PyMappingMethods},
    types::{AsMapping, GenericMethod},
    AsObject, PyObject, PyObjectRef, PyPayload, PyResult, TryFromObject, VirtualMachine,
};
use std::{borrow::Borrow, marker::PhantomData, ops::Deref};

#[pytrace]
#[derive(Clone)]
pub struct ArgCallable {
    obj: PyObjectRef,
    #[notrace]
    call: GenericMethod,
}

impl ArgCallable {
    #[inline(always)]
    pub fn invoke(&self, args: impl IntoFuncArgs, vm: &VirtualMachine) -> PyResult {
        (self.call)(&self.obj, args.into_args(vm), vm)
    }
}

impl std::fmt::Debug for ArgCallable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArgCallable")
            .field("obj", &self.obj)
            .field("call", &format!("{:08x}", self.call as usize))
            .finish()
    }
}

impl Borrow<PyObject> for ArgCallable {
    #[inline(always)]
    fn borrow(&self) -> &PyObject {
        &self.obj
    }
}

impl AsRef<PyObject> for ArgCallable {
    #[inline(always)]
    fn as_ref(&self) -> &PyObject {
        &self.obj
    }
}

impl From<ArgCallable> for PyObjectRef {
    #[inline(always)]
    fn from(value: ArgCallable) -> PyObjectRef {
        value.obj
    }
}

impl TryFromObject for ArgCallable {
    fn try_from_object(vm: &VirtualMachine, obj: PyObjectRef) -> PyResult<Self> {
        let Some(callable) = obj.to_callable() else {
            return Err(vm.new_type_error(format!("'{}' object is not callable", obj.class().name())));
        };
        let call = callable.call;
        Ok(ArgCallable { obj, call })
    }
}

/// An iterable Python object.
///
/// `ArgIterable` implements `FromArgs` so that a built-in function can accept
/// an object that is required to conform to the Python iterator protocol.
///
/// ArgIterable can optionally perform type checking and conversions on iterated
/// objects using a generic type parameter that implements `TryFromObject`.
pub struct ArgIterable<T = PyObjectRef> {
    iterable: PyObjectRef,
    iterfn: Option<crate::types::IterFunc>,
    _item: PhantomData<T>,
}

#[cfg(feature = "gc_bacon")]
unsafe impl<T: crate::object::gc::Trace> crate::object::gc::Trace for ArgIterable<T> {
    fn trace(&self, tracer_fn: &mut crate::object::gc::TracerFn) {
        self.iterable.trace(tracer_fn)
    }
}

impl<T> ArgIterable<T> {
    /// Returns an iterator over this sequence of objects.
    ///
    /// This operation may fail if an exception is raised while invoking the
    /// `__iter__` method of the iterable object.
    pub fn iter<'a>(&self, vm: &'a VirtualMachine) -> PyResult<PyIterIter<'a, T>> {
        let iter = PyIter::new(match self.iterfn {
            Some(f) => f(self.iterable.clone(), vm)?,
            None => PySequenceIterator::new(self.iterable.clone(), vm)?.into_pyobject(vm),
        });
        iter.into_iter(vm)
    }
}

impl<T> TryFromObject for ArgIterable<T>
where
    T: TryFromObject,
{
    fn try_from_object(vm: &VirtualMachine, obj: PyObjectRef) -> PyResult<Self> {
        let iterfn = {
            let cls = obj.class();
            let iterfn = cls.mro_find_map(|x| x.slots.iter.load());
            if iterfn.is_none() && !cls.has_attr(identifier!(vm, __getitem__)) {
                return Err(vm.new_type_error(format!("'{}' object is not iterable", cls.name())));
            }
            iterfn
        };
        Ok(Self {
            iterable: obj,
            iterfn,
            _item: PhantomData,
        })
    }
}

#[derive(Debug, Clone)]
#[pytrace]
pub struct ArgMapping {
    obj: PyObjectRef,
    #[notrace]
    methods: &'static PyMappingMethods,
}

impl ArgMapping {
    #[inline]
    pub fn with_methods(obj: PyObjectRef, methods: &'static PyMappingMethods) -> Self {
        Self { obj, methods }
    }

    #[inline(always)]
    pub fn from_dict_exact(dict: PyDictRef) -> Self {
        Self {
            obj: dict.into(),
            methods: PyDict::as_mapping(),
        }
    }

    #[inline(always)]
    pub fn mapping(&self) -> PyMapping {
        PyMapping {
            obj: &self.obj,
            methods: self.methods,
        }
    }
}

impl Borrow<PyObject> for ArgMapping {
    #[inline(always)]
    fn borrow(&self) -> &PyObject {
        &self.obj
    }
}

impl AsRef<PyObject> for ArgMapping {
    #[inline(always)]
    fn as_ref(&self) -> &PyObject {
        &self.obj
    }
}

impl Deref for ArgMapping {
    type Target = PyObject;
    #[inline(always)]
    fn deref(&self) -> &PyObject {
        &self.obj
    }
}

impl From<ArgMapping> for PyObjectRef {
    #[inline(always)]
    fn from(value: ArgMapping) -> PyObjectRef {
        value.obj
    }
}

impl ToPyObject for ArgMapping {
    #[inline(always)]
    fn to_pyobject(self, _vm: &VirtualMachine) -> PyObjectRef {
        self.obj
    }
}

impl TryFromObject for ArgMapping {
    fn try_from_object(vm: &VirtualMachine, obj: PyObjectRef) -> PyResult<Self> {
        let mapping = PyMapping::try_protocol(&obj, vm)?;
        let methods = mapping.methods;
        Ok(Self { obj, methods })
    }
}

// this is not strictly related to PySequence protocol.
#[derive(Clone)]
pub struct ArgSequence<T = PyObjectRef>(Vec<T>);

#[cfg(feature = "gc_bacon")]
unsafe impl<T: crate::object::gc::Trace> crate::object::gc::Trace for ArgSequence<T> {
    fn trace(&self, tracer_fn: &mut crate::object::gc::TracerFn) {
        self.0.trace(tracer_fn);
    }
}

impl<T> ArgSequence<T> {
    #[inline(always)]
    pub fn into_vec(self) -> Vec<T> {
        self.0
    }
    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }
}

impl<T> std::ops::Deref for ArgSequence<T> {
    type Target = [T];
    #[inline(always)]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T: TryFromObject> TryFromObject for ArgSequence<T> {
    fn try_from_object(vm: &VirtualMachine, obj: PyObjectRef) -> PyResult<Self> {
        obj.try_to_value(vm).map(Self)
    }
}
