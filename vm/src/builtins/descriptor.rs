use super::{PyStr, PyStrInterned, PyType, PyTypeRef};
use crate::{
    class::PyClassImpl,
    function::PySetterValue,
    types::{Constructor, GetDescriptor, Representable, Unconstructible},
    AsObject, Context, Py, PyObject, PyObjectRef, PyPayload, PyResult, VirtualMachine,
};
use rustpython_common::lock::PyRwLock;

#[derive(Debug)]
pub struct DescrObject {
    pub typ: PyTypeRef,
    pub name: &'static PyStrInterned,
    pub qualname: PyRwLock<Option<String>>,
}

#[derive(Debug)]
pub enum MemberKind {
    Bool = 14,
    ObjectEx = 16,
}

pub type MemberSetterFunc = Option<fn(&VirtualMachine, PyObjectRef, PySetterValue) -> PyResult<()>>;

pub enum MemberGetter {
    Getter(fn(&VirtualMachine, PyObjectRef) -> PyResult),
    Offset(usize),
}

pub enum MemberSetter {
    Setter(MemberSetterFunc),
    Offset(usize),
}

pub struct MemberDef {
    pub name: String,
    pub kind: MemberKind,
    pub getter: MemberGetter,
    pub setter: MemberSetter,
    pub doc: Option<String>,
}

impl MemberDef {
    fn get(&self, obj: PyObjectRef, vm: &VirtualMachine) -> PyResult {
        match self.getter {
            MemberGetter::Getter(getter) => (getter)(vm, obj),
            MemberGetter::Offset(offset) => get_slot_from_object(obj, offset, self, vm),
        }
    }

    fn set(
        &self,
        obj: PyObjectRef,
        value: PySetterValue<PyObjectRef>,
        vm: &VirtualMachine,
    ) -> PyResult<()> {
        match self.setter {
            MemberSetter::Setter(setter) => match setter {
                Some(setter) => (setter)(vm, obj, value),
                None => Err(vm.new_attribute_error("readonly attribute".to_string())),
            },
            MemberSetter::Offset(offset) => set_slot_at_object(obj, offset, self, value, vm),
        }
    }
}

impl std::fmt::Debug for MemberDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberDef")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("doc", &self.doc)
            .finish()
    }
}

#[pyclass(name = "member_descriptor", module = false)]
#[derive(Debug)]
pub struct MemberDescrObject {
    pub common: DescrObject,
    pub member: MemberDef,
}

impl PyPayload for MemberDescrObject {
    fn class(ctx: &Context) -> &'static Py<PyType> {
        ctx.types.member_descriptor_type
    }
}

fn calculate_qualname(descr: &DescrObject, vm: &VirtualMachine) -> PyResult<Option<String>> {
    if let Some(qualname) = vm.get_attribute_opt(descr.typ.to_owned().into(), "__qualname__")? {
        let str = qualname.downcast::<PyStr>().map_err(|_| {
            vm.new_type_error(
                "<descriptor>.__objclass__.__qualname__ is not a unicode object".to_owned(),
            )
        })?;
        Ok(Some(format!("{}.{}", str, descr.name)))
    } else {
        Ok(None)
    }
}

#[pyclass(with(GetDescriptor, Constructor, Representable), flags(BASETYPE))]
impl MemberDescrObject {
    #[pygetset(magic)]
    fn doc(&self) -> Option<String> {
        self.member.doc.to_owned()
    }

    #[pygetset(magic)]
    fn qualname(&self, vm: &VirtualMachine) -> PyResult<Option<String>> {
        let qualname = self.common.qualname.read();
        Ok(if qualname.is_none() {
            drop(qualname);
            let calculated = calculate_qualname(&self.common, vm)?;
            *self.common.qualname.write() = calculated.to_owned();
            calculated
        } else {
            qualname.to_owned()
        })
    }

    #[pyslot]
    fn descr_set(
        zelf: &PyObject,
        obj: PyObjectRef,
        value: PySetterValue<PyObjectRef>,
        vm: &VirtualMachine,
    ) -> PyResult<()> {
        let zelf = Self::_as_pyref(zelf, vm)?;
        zelf.member.set(obj, value, vm)
    }
}

// PyMember_GetOne
fn get_slot_from_object(
    obj: PyObjectRef,
    offset: usize,
    member: &MemberDef,
    vm: &VirtualMachine,
) -> PyResult {
    let slot = match member.kind {
        MemberKind::Bool => obj
            .get_slot(offset)
            .unwrap_or_else(|| vm.ctx.new_bool(false).into()),
        MemberKind::ObjectEx => obj.get_slot(offset).ok_or_else(|| {
            vm.new_attribute_error(format!(
                "'{}' object has no attribute '{}'",
                obj.class().name(),
                member.name
            ))
        })?,
    };
    Ok(slot)
}

// PyMember_SetOne
fn set_slot_at_object(
    obj: PyObjectRef,
    offset: usize,
    member: &MemberDef,
    value: PySetterValue,
    vm: &VirtualMachine,
) -> PyResult<()> {
    match member.kind {
        MemberKind::Bool => {
            match value {
                PySetterValue::Assign(v) => {
                    if !v.class().is(vm.ctx.types.bool_type) {
                        return Err(
                            vm.new_type_error("attribute value type must be bool".to_owned())
                        );
                    }

                    obj.set_slot(offset, Some(v))
                }
                PySetterValue::Delete => obj.set_slot(offset, None),
            };
        }
        MemberKind::ObjectEx => match value {
            PySetterValue::Assign(v) => obj.set_slot(offset, Some(v)),
            PySetterValue::Delete => obj.set_slot(offset, None),
        },
    }

    Ok(())
}

impl Unconstructible for MemberDescrObject {}

impl Representable for MemberDescrObject {
    #[inline]
    fn repr_str(zelf: &Py<Self>, _vm: &VirtualMachine) -> PyResult<String> {
        Ok(format!(
            "<member '{}' of '{}' objects>",
            zelf.common.name,
            zelf.common.typ.name(),
        ))
    }
}

impl GetDescriptor for MemberDescrObject {
    fn descr_get(
        zelf: PyObjectRef,
        obj: Option<PyObjectRef>,
        _cls: Option<PyObjectRef>,
        vm: &VirtualMachine,
    ) -> PyResult {
        match obj {
            Some(x) => {
                let zelf = Self::_as_pyref(&zelf, vm)?;
                zelf.member.get(x, vm)
            }
            None => Ok(zelf),
        }
    }
}

pub fn init(context: &Context) {
    let member_descriptor_type = &context.types.member_descriptor_type;
    MemberDescrObject::extend_class(context, member_descriptor_type);
}
