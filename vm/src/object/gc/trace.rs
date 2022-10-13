use rustpython_common::lock::{PyMutex, PyRwLock};

use crate::object::gc::{deadlock_handler, header::GcHeader, GcObjRef, LOCK_TIMEOUT};
use crate::object::PyObjectPayload;
use crate::{AsObject, PyObject, PyObjectRef, PyRef};
use core::ptr::NonNull;
use std::any::TypeId;
use std::collections::HashSet;

// TODO: make a function that exec code on traceable data type
pub struct TraceHelper {}

macro_rules! list_traceable {
    ($MACRO_NAME: tt) => {
        {
            use crate::builtins::iter::{PyCallableIterator, PySequenceIterator};
            use crate::object::Erased;
            use crate::builtins::{
                enumerate::PyReverseSequenceIterator,
                function::PyCell,
                list::{PyListIterator, PyListReverseIterator},
                memory::PyMemoryViewIterator,
                tuple::PyTupleIterator,
            };
            use crate::builtins::{
                PyBoundMethod, PyDict, PyEnumerate, PyFilter, PyFunction, PyList, PyMappingProxy,
                PyProperty, PySet, PySlice, PyStaticMethod, PySuper, PyTraceback, PyTuple, PyType,
                PyWeakProxy, PyZip,
            };
            use crate::function::{ArgCallable, ArgIterable, ArgMapping, ArgSequence};
            use crate::protocol::{
                PyBuffer, PyIter, PyIterIter, PyIterReturn, PyMapping, PyNumber, PySequence,
            };
            $MACRO_NAME!(
                // builtin types
                // PyRange, PyStr is acyclic, therefore no trace needed for them
                PyBoundMethod,
                PyDict,
                PyEnumerate,
                PyFilter,
                PyFunction,
                PyList,
                PyMappingProxy,
                PyProperty,
                PySet,
                PySlice,
                PyStaticMethod,
                PySuper,
                PyTraceback,
                PyTuple,
                // FIXME(discord9): deal with static PyType properly
                // PyType,
                PyWeakProxy,
                PyZip,
                // misc
                // FIXME(discord9): causing dead lock on very rare occasion
                // PyCell,
                // iter in iter.rs
                // FIXME(discord9): PositionIterInternal seems to cause dead lock on trace on very rare occasion
                // which is called in PySequenceIterator(and many other iters, but they are less frequent so appeal fine?)
                // PySequenceIterator,
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
                // FIXME(discord9): confirm this is ok to do
                PyIterIter<Erased>,
                PyIterReturn,
                PyMapping,
                PyNumber,
                PySequence
            )
        }
    };
}

macro_rules! get_type_ids {
    ($($TY: ty),*$(,)?) => {
        [$(
            std::any::TypeId::of::<$TY>()
        ),*]
    };
}
pub static TRACEABLE_TYPE: once_cell::sync::Lazy<HashSet<TypeId>> =
    once_cell::sync::Lazy::new(|| HashSet::from(list_traceable!(get_type_ids)));
impl TraceHelper {
    pub fn is_traceable(tid: TypeId) -> bool {
        TRACEABLE_TYPE.contains(&tid)
    }
}

/// indicate what to do with the object afer calling dec()
#[derive(PartialEq, Eq)]
pub enum GcStatus {
    /// should be drop by caller
    ShouldDrop,
    /// already buffered, will be dealloc by collector, caller should call `drop_only` to run destructor only but not dealloc memory region
    BufferedDrop,
    /// should keep and not drop by caller
    ShouldKeep,
    /// Do Nothing, perhaps because it is RAII's deeds
    DoNothing,
}

pub trait GcObjPtr: GcTrace {
    fn inc(&self);
    fn dec(&self) -> GcStatus;
    fn rc(&self) -> usize;
    /// return object header
    fn header(&self) -> &GcHeader;
    // as a NonNull pointer to a gc managed object
    unsafe fn as_ptr(&self) -> NonNull<PyObject>;
}

/// use `trace()` to call on all owned ObjectRef
///
/// # Safety
///
/// see `trace()`'s requirement
pub unsafe trait GcTrace {
    /// call tracer_fn for every object(childrens) owned by a Object
    /// # Safety
    ///
    /// must make sure that every owned object(Every stored `PyObjectRef` to be exactly) is called with tracer_fn **at most once**.
    /// If some field is not called, the worst results is memory leak, but if some field is called repeatly, panic and deadlock can happen.
    ///
    /// _**DO NOT**_ clone a `PyObjectRef`(which mess up the ref count system) in `trace()`, use `ref`erence or, if actually had to, use `as_ptr()`(which is a last resort and better not to use) instead and operate on NonNull
    ///
    /// ```ignore
    /// for ch in childs:
    ///     tracer_fn(ch)
    /// ```
    ///
    /// Note that Two `PyObjectRef` to the Same `PyObject` still count as two Ref, and should be called twice(once for each one) in this case.
    fn trace(&self, tracer_fn: &mut TracerFn);
}

/// A `TracerFn` is a callback function that is invoked for each `PyGcObjectRef` owned
/// by an instance of something.
pub type TracerFn<'a> = dyn FnMut(GcObjRef) + 'a;

unsafe impl GcTrace for PyObjectRef {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn(self)
    }
}

unsafe impl GcTrace for () {
    #[inline]
    fn trace(&self, _tracer_fn: &mut TracerFn) {}
}

unsafe impl<T: PyObjectPayload> GcTrace for PyRef<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        tracer_fn((*self).as_object())
    }
}

unsafe impl<T: GcTrace> GcTrace for Option<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        if let Some(v) = self {
            v.trace(tracer_fn);
        }
    }
}

unsafe impl<T> GcTrace for [T]
where
    T: GcTrace,
{
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        for elem in self {
            elem.trace(tracer_fn);
        }
    }
}

unsafe impl<T> GcTrace for Vec<T>
where
    T: GcTrace,
{
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        for elem in self {
            elem.trace(tracer_fn);
        }
    }
}

unsafe impl<T: GcTrace> GcTrace for PyMutex<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        // FIXME(discord9): check if this may cause a deadlock or not
        // TODO: acquire raw pointer and iter over them?(That would be both unsafe&unsound if not in gc pausing or done by a)
        #[cfg(feature = "threading")]
        match self.try_lock() {
            Some(inner) => {
                // instead of the sound way of getting a lock and trace
                inner.trace(tracer_fn);
            }
            None => {
                // that is likely a cause of wrong `trace()` impl
                error!("Could be in dead lock.");
                // can't find better way to trace with that
                // not kill the thread for now(So to test our unsound way of tracing)
                use backtrace::Backtrace;
                let bt = Backtrace::new();
                error!(
                    "Dead lock on {}: \n--------\n{:?}",
                    std::any::type_name::<T>(),
                    bt
                );
                // deadlock_handler()
            }
        }

        #[cfg(not(feature = "threading"))]
        match self.try_lock() {
            Some(v) => v.trace(tracer_fn),
            None => {
                // that is likely a cause of wrong `trace()` impl
                error!("Could be in dead lock.");
                // not kill the thread for now
                deadlock_handler()
            }
        }
    }
}

unsafe impl<T: GcTrace> GcTrace for PyRwLock<T> {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        // FIXME(discord9): check if this may cause a deadlock or not, maybe try `recursive`?
        #[cfg(feature = "threading")]
        match self.try_read_for(LOCK_TIMEOUT) {
            Some(v) => v.trace(tracer_fn),
            None => deadlock_handler(),
        }

        #[cfg(not(feature = "threading"))]
        match self.try_read() {
            Some(v) => v.trace(tracer_fn),
            None => deadlock_handler(),
        }
    }
}

// TODO(discord9): impl_tuples!(impossible with declarative macros)
// TODO(discord9): GcTrace as a derive proc macro
unsafe impl<A: GcTrace, B: GcTrace> GcTrace for (A, B) {
    #[inline]
    fn trace(&self, tracer_fn: &mut TracerFn) {
        self.0.trace(tracer_fn);
        self.1.trace(tracer_fn);
    }
}
