use super::{type_, PyClassMethod, PyStaticMethod, PyStr, PyStrInterned, PyStrRef, PyType};
use crate::{
    builtins::PyBoundMethod,
    class::PyClassImpl,
    function::{FuncArgs, IntoPyNativeFunc, PyNativeFunc},
    types::{Callable, Constructor, GetDescriptor, Representable, Unconstructible},
    AsObject, Context, Py, PyObjectRef, PyPayload, PyRef, PyResult, VirtualMachine,
};
use std::fmt;

pub struct PyNativeFuncDef {
    pub func: PyNativeFunc,
    pub name: &'static PyStrInterned,
    pub doc: Option<PyStrRef>,
}

impl PyNativeFuncDef {
    pub fn new(func: PyNativeFunc, name: &'static PyStrInterned) -> Self {
        Self {
            func,
            name,
            doc: None,
        }
    }

    pub fn with_doc(mut self, doc: String, ctx: &Context) -> Self {
        self.doc = Some(PyStr::new_ref(doc, ctx));
        self
    }

    pub fn into_function(self) -> PyBuiltinFunction {
        self.into()
    }
    pub fn build_function(self, ctx: &Context) -> PyRef<PyBuiltinFunction> {
        self.into_function().into_ref(ctx)
    }
    pub fn build_method(self, ctx: &Context, class: &'static Py<PyType>) -> PyRef<PyBuiltinMethod> {
        PyRef::new_ref(
            PyBuiltinMethod { value: self, class },
            ctx.types.method_descriptor_type.to_owned(),
            None,
        )
    }
    pub fn build_classmethod(
        self,
        ctx: &Context,
        class: &'static Py<PyType>,
    ) -> PyRef<PyClassMethod> {
        // TODO: classmethod_descriptor
        let callable = self.build_method(ctx, class).into();
        PyClassMethod::new_ref(callable, ctx)
    }
    pub fn build_staticmethod(
        self,
        ctx: &Context,
        class: &'static Py<PyType>,
    ) -> PyRef<PyStaticMethod> {
        let callable = self.build_method(ctx, class).into();
        PyStaticMethod::new_ref(callable, ctx)
    }
}

#[pyclass(name = "builtin_function_or_method", module = false)]
pub struct PyBuiltinFunction {
    value: PyNativeFuncDef,
    module: Option<PyObjectRef>,
}

impl PyPayload for PyBuiltinFunction {
    fn class(ctx: &Context) -> &'static Py<PyType> {
        ctx.types.builtin_function_or_method_type
    }
}

impl fmt::Debug for PyBuiltinFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "builtin function {}", self.value.name.as_str())
    }
}

impl From<PyNativeFuncDef> for PyBuiltinFunction {
    fn from(value: PyNativeFuncDef) -> Self {
        Self {
            value,
            module: None,
        }
    }
}

impl PyBuiltinFunction {
    pub fn with_module(mut self, module: PyObjectRef) -> Self {
        self.module = Some(module);
        self
    }

    pub fn into_ref(self, ctx: &Context) -> PyRef<Self> {
        PyRef::new_ref(
            self,
            ctx.types.builtin_function_or_method_type.to_owned(),
            None,
        )
    }

    pub fn as_func(&self) -> &PyNativeFunc {
        &self.value.func
    }
}

impl Callable for PyBuiltinFunction {
    type Args = FuncArgs;
    #[inline]
    fn call(zelf: &Py<Self>, args: FuncArgs, vm: &VirtualMachine) -> PyResult {
        (zelf.value.func)(vm, args)
    }
}

#[pyclass(with(Callable, Constructor), flags(HAS_DICT))]
impl PyBuiltinFunction {
    #[pygetset(magic)]
    fn module(&self, vm: &VirtualMachine) -> PyObjectRef {
        vm.unwrap_or_none(self.module.clone())
    }
    #[pygetset(magic)]
    fn name(&self) -> PyStrRef {
        self.value.name.to_owned()
    }
    #[pygetset(magic)]
    fn qualname(&self) -> PyStrRef {
        self.name()
    }
    #[pygetset(magic)]
    fn doc(&self) -> Option<PyStrRef> {
        self.value.doc.clone()
    }
    #[pygetset(name = "__self__")]
    fn get_self(&self, vm: &VirtualMachine) -> PyObjectRef {
        vm.ctx.none()
    }
    #[pymethod(magic)]
    fn reduce(&self) -> PyStrRef {
        // TODO: return (getattr, (self.object, self.name)) if this is a method
        self.name()
    }
    #[pymethod(magic)]
    fn reduce_ex(&self, _ver: PyObjectRef) -> PyStrRef {
        self.name()
    }
    #[pygetset(magic)]
    fn text_signature(&self) -> Option<String> {
        let doc = self.value.doc.as_ref()?;
        let signature =
            type_::get_text_signature_from_internal_doc(self.value.name.as_str(), doc.as_str())?;
        Some(signature.to_owned())
    }
}

impl Representable for PyBuiltinFunction {
    #[inline]
    fn repr_str(zelf: &Py<Self>, _vm: &VirtualMachine) -> PyResult<String> {
        Ok(format!("<built-in function {}>", zelf.value.name))
    }
}

impl Unconstructible for PyBuiltinFunction {}

// `PyBuiltinMethod` is similar to both `PyMethodDescrObject` in
// https://github.com/python/cpython/blob/main/Include/descrobject.h
// https://github.com/python/cpython/blob/main/Objects/descrobject.c
// and `PyCMethodObject` in
// https://github.com/python/cpython/blob/main/Include/cpython/methodobject.h
// https://github.com/python/cpython/blob/main/Objects/methodobject.c
#[pyclass(module = false, name = "method_descriptor")]
pub struct PyBuiltinMethod {
    value: PyNativeFuncDef,
    class: &'static Py<PyType>,
}

impl PyPayload for PyBuiltinMethod {
    fn class(ctx: &Context) -> &'static Py<PyType> {
        ctx.types.method_descriptor_type
    }
}

impl fmt::Debug for PyBuiltinMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "method descriptor for '{}'", self.value.name)
    }
}

impl GetDescriptor for PyBuiltinMethod {
    fn descr_get(
        zelf: PyObjectRef,
        obj: Option<PyObjectRef>,
        cls: Option<PyObjectRef>,
        vm: &VirtualMachine,
    ) -> PyResult {
        let (_zelf, obj) = match Self::_check(&zelf, obj, vm) {
            Some(obj) => obj,
            None => return Ok(zelf),
        };
        let r = if vm.is_none(&obj) && !Self::_cls_is(&cls, obj.class()) {
            zelf
        } else {
            PyBoundMethod::new_ref(obj, zelf, &vm.ctx).into()
        };
        Ok(r)
    }
}

impl Callable for PyBuiltinMethod {
    type Args = FuncArgs;
    #[inline]
    fn call(zelf: &Py<Self>, args: FuncArgs, vm: &VirtualMachine) -> PyResult {
        (zelf.value.func)(vm, args)
    }
}

impl PyBuiltinMethod {
    pub fn new_ref<F, FKind>(
        name: &'static PyStrInterned,
        class: &'static Py<PyType>,
        f: F,
        ctx: &Context,
    ) -> PyRef<Self>
    where
        F: IntoPyNativeFunc<FKind>,
    {
        ctx.make_func_def(name, f).build_method(ctx, class)
    }
}

#[pyclass(
    with(GetDescriptor, Callable, Constructor, Representable),
    flags(METHOD_DESCR)
)]
impl PyBuiltinMethod {
    #[pygetset(magic)]
    fn name(&self) -> PyStrRef {
        self.value.name.to_owned()
    }
    #[pygetset(magic)]
    fn qualname(&self) -> String {
        format!("{}.{}", self.class.name(), &self.value.name)
    }
    #[pygetset(magic)]
    fn doc(&self) -> Option<PyStrRef> {
        self.value.doc.clone()
    }
    #[pygetset(magic)]
    fn text_signature(&self) -> Option<String> {
        self.value.doc.as_ref().and_then(|doc| {
            type_::get_text_signature_from_internal_doc(self.value.name.as_str(), doc.as_str())
                .map(|signature| signature.to_string())
        })
    }
    #[pymethod(magic)]
    fn reduce(
        &self,
        vm: &VirtualMachine,
    ) -> (Option<PyObjectRef>, (Option<PyObjectRef>, PyStrRef)) {
        let builtins_getattr = vm.builtins.get_attr("getattr", vm).ok();
        let classname = vm.builtins.get_attr(&self.class.__name__(vm), vm).ok();
        (builtins_getattr, (classname, self.value.name.to_owned()))
    }
}

impl Representable for PyBuiltinMethod {
    #[inline]
    fn repr_str(zelf: &Py<Self>, _vm: &VirtualMachine) -> PyResult<String> {
        Ok(format!(
            "<method '{}' of '{}' objects>",
            &zelf.value.name,
            zelf.class.name()
        ))
    }
}

impl Unconstructible for PyBuiltinMethod {}

pub fn init(context: &Context) {
    PyBuiltinFunction::extend_class(context, context.types.builtin_function_or_method_type);
    PyBuiltinMethod::extend_class(context, context.types.method_descriptor_type);
}
