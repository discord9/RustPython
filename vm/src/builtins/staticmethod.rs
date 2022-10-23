use rustpython_common::lock::PyRwLock;

use super::{PyStr, PyType, PyTypeRef};
use crate::{
    builtins::builtinfunc::PyBuiltinMethod,
    class::PyClassImpl,
    function::{FuncArgs, IntoPyNativeFunc},
    types::{Callable, Constructor, GetDescriptor, Initializer},
    Context, Py, PyObjectRef, PyPayload, PyRef, PyResult, VirtualMachine,
};

#[pyclass(module = false, name = "staticmethod")]
#[derive(Debug)]
pub struct PyStaticMethod {
    pub callable: PyRwLock<PyObjectRef>,
}

#[cfg(feature = "gc")]
unsafe impl crate::object::Trace for PyStaticMethod {
    fn trace(&self, tracer_fn: &mut crate::object::TracerFn) {
        self.callable.trace(tracer_fn)
    }
}

impl PyPayload for PyStaticMethod {
    fn class(vm: &VirtualMachine) -> &'static Py<PyType> {
        vm.ctx.types.staticmethod_type
    }
}

impl GetDescriptor for PyStaticMethod {
    fn descr_get(
        zelf: PyObjectRef,
        obj: Option<PyObjectRef>,
        _cls: Option<PyObjectRef>,
        vm: &VirtualMachine,
    ) -> PyResult {
        let (zelf, _obj) = Self::_unwrap(zelf, obj, vm)?;
        let x = Ok(zelf.callable.read().clone());
        x
    }
}

impl From<PyObjectRef> for PyStaticMethod {
    fn from(callable: PyObjectRef) -> Self {
        Self {
            callable: PyRwLock::new(callable),
        }
    }
}

impl Constructor for PyStaticMethod {
    type Args = PyObjectRef;

    fn py_new(cls: PyTypeRef, callable: Self::Args, vm: &VirtualMachine) -> PyResult {
        let doc = callable.get_attr("__doc__", vm);

        let result = PyStaticMethod {
            callable: PyRwLock::new(callable),
        }
        .into_ref_with_type(vm, cls)?;
        let obj = PyObjectRef::from(result);

        if let Ok(doc) = doc {
            obj.set_attr("__doc__", doc, vm)?;
        }

        Ok(obj)
    }
}

impl PyStaticMethod {
    pub fn new_ref(callable: PyObjectRef, ctx: &Context) -> PyRef<Self> {
        PyRef::new_ref(
            Self {
                callable: PyRwLock::new(callable),
            },
            ctx.types.staticmethod_type.to_owned(),
            None,
        )
    }
}

impl PyStaticMethod {
    pub fn new_builtin_ref<F, FKind>(
        name: impl Into<PyStr>,
        class: &'static Py<PyType>,
        f: F,
        ctx: &Context,
    ) -> PyRef<Self>
    where
        F: IntoPyNativeFunc<FKind>,
    {
        let callable = PyBuiltinMethod::new_ref(name, class, f, ctx).into();
        PyRef::new_ref(
            Self {
                callable: PyRwLock::new(callable),
            },
            ctx.types.staticmethod_type.to_owned(),
            None,
        )
    }
}

impl Initializer for PyStaticMethod {
    type Args = PyObjectRef;

    fn init(zelf: PyRef<Self>, callable: Self::Args, _vm: &VirtualMachine) -> PyResult<()> {
        *zelf.callable.write() = callable;
        Ok(())
    }
}

#[pyclass(
    with(Callable, GetDescriptor, Constructor, Initializer),
    flags(BASETYPE, HAS_DICT)
)]
impl PyStaticMethod {
    #[pygetset(magic)]
    fn func(&self) -> PyObjectRef {
        self.callable.read().clone()
    }

    #[pygetset(magic)]
    fn wrapped(&self) -> PyObjectRef {
        self.callable.read().clone()
    }

    #[pygetset(magic)]
    fn module(&self, vm: &VirtualMachine) -> PyResult {
        self.callable.read().get_attr("__module__", vm)
    }

    #[pygetset(magic)]
    fn qualname(&self, vm: &VirtualMachine) -> PyResult {
        self.callable.read().get_attr("__qualname__", vm)
    }

    #[pygetset(magic)]
    fn name(&self, vm: &VirtualMachine) -> PyResult {
        self.callable.read().get_attr("__name__", vm)
    }

    #[pygetset(magic)]
    fn annotations(&self, vm: &VirtualMachine) -> PyResult {
        self.callable.read().get_attr("__annotations__", vm)
    }

    #[pymethod(magic)]
    fn repr(&self, vm: &VirtualMachine) -> Option<String> {
        let callable = self.callable.read().repr(vm).unwrap();
        let class = Self::class(vm);

        match (
            class
                .qualname(vm)
                .downcast_ref::<PyStr>()
                .map(|n| n.as_str()),
            class.module(vm).downcast_ref::<PyStr>().map(|m| m.as_str()),
        ) {
            (None, _) => None,
            (Some(qualname), Some(module)) if module != "builtins" => {
                Some(format!("<{module}.{qualname}({callable})>"))
            }
            _ => Some(format!("<{}({})>", class.slot_name(), callable)),
        }
    }

    #[pygetset(magic)]
    fn isabstractmethod(&self, vm: &VirtualMachine) -> PyObjectRef {
        match vm.get_attribute_opt(self.callable.read().clone(), "__isabstractmethod__") {
            Ok(Some(is_abstract)) => is_abstract,
            _ => vm.ctx.new_bool(false).into(),
        }
    }

    #[pygetset(magic, setter)]
    fn set_isabstractmethod(&self, value: PyObjectRef, vm: &VirtualMachine) -> PyResult<()> {
        self.callable
            .read()
            .set_attr("__isabstractmethod__", value, vm)?;
        Ok(())
    }
}

impl Callable for PyStaticMethod {
    type Args = FuncArgs;
    #[inline]
    fn call(zelf: &crate::Py<Self>, args: FuncArgs, vm: &VirtualMachine) -> PyResult {
        let callable = zelf.callable.read().clone();
        vm.invoke(&callable, args)
    }
}

pub fn init(context: &Context) {
    PyStaticMethod::extend_class(context, context.types.staticmethod_type);
}
