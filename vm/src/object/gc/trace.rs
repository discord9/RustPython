use std::{any::TypeId, collections::HashSet};

use once_cell::sync::Lazy;
use rustpython_common::lock::PyRwLock;

use crate::{object::PyObjectPayload, AsObject, PyObjectRef, PyRef};

use super::GcObjRef;

pub type TracerFn<'a> = dyn FnMut(GcObjRef) + 'a;

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

pub struct TraceHelper {}

/// apply a macro to a list of traceable type. using macro instead of generic
/// because otherwise require specialization feature to enable
#[macro_export]
macro_rules! list_traceable {
    ($MACRO_NAME: tt) => {{
        use $crate::builtins::*;
        use $crate::builtins::{
            enumerate::PyReverseSequenceIterator,
            function::PyCell,
            list::{PyListIterator, PyListReverseIterator},
            memory::{PyMemoryViewIterator, PyMemoryViewNewArgs},
            super_::PySuperNewArgs,
            tuple::PyTupleIterator,
        };
        use $crate::function::{ArgCallable, ArgIterable, ArgMapping, ArgSequence};
        use $crate::protocol::{
            PyBuffer, PyIter, PyIterIter, PyIterReturn, PyMapping, PyNumber, PySequence,
        };
        $MACRO_NAME!(
            // builtin types
            // PyRange, PyStr is acyclic, therefore no trace needed for them
            PyBaseException,
            PyBoundMethod,
            PyDict,
            PyEnumerate,
            PyReverseSequenceIterator,
            PyFilter,
            PyFunction,
            PyBoundMethod,
            PyCell,
            IterStatus<PyObjectRef>,
            PositionIterInternal<PyObjectRef>,
            PySequenceIterator,
            PyCallableIterator,
            PyList,
            PyListIterator,
            PyListReverseIterator,
            PyMap,
            PyMappingProxy,
            PyMemoryViewNewArgs,
            PyMemoryViewIterator,
            PyProperty,
            PySet,
            PySlice,
            PyStaticMethod,
            PySuper,
            PySuperNewArgs,
            PyTraceback,
            PyTuple,
            PyTupleIterator,
            // FIXME(discord9): deal with static PyType properly
            PyType,
            PyUnion,
            PyWeakProxy,
            PyZip,
            PyBaseException,
            // iter in iter.rs
            PySequenceIterator,
            PyCallableIterator,
            // iter on types
            // PyList's iter
            PyListIterator,
            PyListReverseIterator,
            // PyTuple's iter
            PyTupleIterator,
            // PyEnumerate's iter
            PyReverseSequenceIterator,
            // PyMemory's iter
            PyMemoryViewIterator,
            // function/Arg protocol
            ArgCallable,
            ArgIterable,
            ArgMapping,
            ArgSequence,
            // protocol
            // struct like
            PyBuffer,
            PyIter,
            PyIterIter<PyObjectRef>,
            PyIterReturn,
            PyMapping,
            PyNumber,
            PySequence
        )
    }};
}

macro_rules! get_type_ids {
    ($($TY: ty),*$(,)?) => {
        [$(
            std::any::TypeId::of::<$TY>()
        ),*]
    };
}
pub static TRACEABLE_TYPE: Lazy<HashSet<TypeId>> =
    Lazy::new(|| HashSet::from(list_traceable!(get_type_ids)));
impl TraceHelper {
    /// return true if TypeId's corrsponding type is traceable.
    ///
    /// soundness: if extremely rare hash collision happen with TypeId(see [this](https://github.com/rust-lang/rust/issues/10389)),
    /// the worst results is just mistaken a non-traceable type as traceable, which usually doesn't interference with garbage collection
    pub fn is_traceable(tid: TypeId) -> bool {
        TRACEABLE_TYPE.contains(&tid)
    }
}
