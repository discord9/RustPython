//! Buffer protocol
//! https://docs.python.org/3/c-api/buffer.html

use crate::{
    common::{
        borrow::{BorrowedValue, BorrowedValueMut},
        lock::{MapImmutable, PyMutex, PyMutexGuard},
    },
    object::{PyObjectPayload, Trace},
    sliceable::SequenceIndexOp,
    types::{Constructor, Unconstructible},
    AsObject, Py, PyObject, PyObjectRef, PyPayload, PyRef, PyResult, TryFromBorrowedObject,
    VirtualMachine,
};
use itertools::Itertools;
use std::{borrow::Cow, fmt::Debug, mem::ManuallyDrop, ops::Range};

pub struct BufferMethods {
    pub obj_bytes: fn(&PyBuffer) -> BorrowedValue<[u8]>,
    pub obj_bytes_mut: fn(&PyBuffer) -> BorrowedValueMut<[u8]>,
    pub release: fn(&PyBuffer),
    pub retain: fn(&PyBuffer),
}

impl Debug for BufferMethods {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferMethods")
            .field("obj_bytes", &(self.obj_bytes as usize))
            .field("obj_bytes_mut", &(self.obj_bytes_mut as usize))
            .field("release", &(self.release as usize))
            .field("retain", &(self.retain as usize))
            .finish()
    }
}

/// for manually increment ref count when doing memcpy
pub trait ManuallyClone {
    /// call `clone()` for every PyObjectRef/PyRef this struct owned,
    /// and **discard** them
    ///
    /// can be used when you need a memcpy/u8::copy_from_slice
    /// but still need keep the ref count right, then one can do:
    /// ```ignore
    /// fn example(src: PyBytesLikeType, dst: AnotherPyBytesLikeType){
    ///     src.manually_clone();
    ///     dst.copy_from_slice(b.as_bytes())
    /// }
    /// ```
    fn manually_clone(&self);
}

#[derive(Debug, Clone)]
pub struct PyBuffer {
    pub obj: PyObjectRef,
    pub desc: BufferDescriptor,
    methods: &'static BufferMethods,
}

impl ManuallyClone for PyBuffer {
    fn manually_clone(&self) {
        error!("Manually Clone {:?}", self.obj);
        self.obj.as_object().trace(&mut |ch| {
            let r = ch.to_owned();
            let _ = ManuallyDrop::new(r);
        });
    }
}

#[cfg(feature = "gc")]
unsafe impl crate::object::Trace for PyBuffer {
    fn trace(&self, tracer_fn: &mut crate::object::TracerFn) {
        self.obj.trace(tracer_fn)
    }
}

impl PyBuffer {
    pub fn new(obj: PyObjectRef, desc: BufferDescriptor, methods: &'static BufferMethods) -> Self {
        let zelf = Self {
            obj,
            desc: desc.validate(),
            methods,
        };
        zelf.retain();
        zelf
    }

    pub fn as_contiguous(&self) -> Option<BorrowedValue<[u8]>> {
        self.desc
            .is_contiguous()
            .then(|| unsafe { self.contiguous_unchecked() })
    }

    pub fn as_contiguous_mut(&self) -> Option<BorrowedValueMut<[u8]>> {
        (!self.desc.readonly && self.desc.is_contiguous())
            .then(|| unsafe { self.contiguous_mut_unchecked() })
    }

    pub fn from_byte_vector(bytes: Vec<u8>, vm: &VirtualMachine) -> Self {
        let bytes_len = bytes.len();
        PyBuffer::new(
            PyPayload::into_pyobject(VecBuffer::from(bytes), vm),
            BufferDescriptor::simple(bytes_len, true),
            &VEC_BUFFER_METHODS,
        )
    }

    /// # Safety
    /// assume the buffer is contiguous
    pub unsafe fn contiguous_unchecked(&self) -> BorrowedValue<[u8]> {
        self.obj_bytes()
    }

    /// # Safety
    /// assume the buffer is contiguous and writable
    pub unsafe fn contiguous_mut_unchecked(&self) -> BorrowedValueMut<[u8]> {
        self.obj_bytes_mut()
    }

    pub fn append_to(&self, buf: &mut Vec<u8>) {
        if let Some(bytes) = self.as_contiguous() {
            buf.extend_from_slice(&bytes);
        } else {
            let bytes = &*self.obj_bytes();
            self.desc.for_each_segment(true, |range| {
                buf.extend_from_slice(&bytes[range.start as usize..range.end as usize])
            });
        }
    }

    pub fn contiguous_or_collect<R, F: FnOnce(&[u8]) -> R>(&self, f: F) -> R {
        let borrowed;
        let mut collected;
        let v = if let Some(bytes) = self.as_contiguous() {
            borrowed = bytes;
            &*borrowed
        } else {
            collected = vec![];
            self.append_to(&mut collected);
            &collected
        };
        f(v)
    }

    pub fn obj_as<T: PyObjectPayload>(&self) -> &Py<T> {
        unsafe { self.obj.downcast_unchecked_ref() }
    }

    pub fn obj_bytes(&self) -> BorrowedValue<[u8]> {
        (self.methods.obj_bytes)(self)
    }

    pub fn obj_bytes_mut(&self) -> BorrowedValueMut<[u8]> {
        (self.methods.obj_bytes_mut)(self)
    }

    pub fn release(&self) {
        (self.methods.release)(self)
    }

    pub fn retain(&self) {
        (self.methods.retain)(self)
    }

    // drop PyBuffer without calling release
    // after this function, the owner should use forget()
    // or wrap PyBuffer in the ManaullyDrop to prevent drop()
    pub(crate) unsafe fn drop_without_release(&mut self) {
        std::ptr::drop_in_place(&mut self.obj);
        std::ptr::drop_in_place(&mut self.desc);
    }
}

impl TryFromBorrowedObject for PyBuffer {
    fn try_from_borrowed_object(vm: &VirtualMachine, obj: &PyObject) -> PyResult<Self> {
        let cls = obj.class();
        let as_buffer = cls.mro_find_map(|cls| cls.slots.as_buffer);
        if let Some(f) = as_buffer {
            return f(obj, vm);
        }
        Err(vm.new_type_error(format!(
            "a bytes-like object is required, not '{}'",
            cls.name()
        )))
    }
}

impl Drop for PyBuffer {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Debug, Clone)]
pub struct BufferDescriptor {
    /// product(shape) * itemsize
    /// bytes length, but not the length for obj_bytes() even is contiguous
    pub len: usize,
    pub readonly: bool,
    pub itemsize: usize,
    pub format: Cow<'static, str>,
    /// (shape, stride, suboffset) for each dimension
    pub dim_desc: Vec<(usize, isize, isize)>,
    // TODO: flags
}

impl BufferDescriptor {
    pub fn simple(bytes_len: usize, readonly: bool) -> Self {
        Self {
            len: bytes_len,
            readonly,
            itemsize: 1,
            format: Cow::Borrowed("B"),
            dim_desc: vec![(bytes_len, 1, 0)],
        }
    }

    pub fn format(
        bytes_len: usize,
        readonly: bool,
        itemsize: usize,
        format: Cow<'static, str>,
    ) -> Self {
        Self {
            len: bytes_len,
            readonly,
            itemsize,
            format,
            dim_desc: vec![(bytes_len / itemsize, itemsize as isize, 0)],
        }
    }

    #[cfg(debug_assertions)]
    pub fn validate(self) -> Self {
        assert!(self.itemsize != 0);
        assert!(self.ndim() != 0);
        let mut shape_product = 1;
        for (shape, stride, suboffset) in self.dim_desc.iter().cloned() {
            shape_product *= shape;
            assert!(suboffset >= 0);
            assert!(stride != 0);
        }
        assert!(shape_product * self.itemsize == self.len);
        self
    }

    #[cfg(not(debug_assertions))]
    pub fn validate(self) -> Self {
        self
    }

    pub fn ndim(&self) -> usize {
        self.dim_desc.len()
    }

    pub fn is_contiguous(&self) -> bool {
        if self.len == 0 {
            return true;
        }
        let mut sd = self.itemsize;
        for (shape, stride, _) in self.dim_desc.iter().cloned().rev() {
            if shape > 1 && stride != sd as isize {
                return false;
            }
            sd *= shape;
        }
        true
    }

    /// this function do not check the bound
    /// panic if indices.len() != ndim
    pub fn fast_position(&self, indices: &[usize]) -> isize {
        let mut pos = 0;
        for (i, (_, stride, suboffset)) in indices
            .iter()
            .cloned()
            .zip_eq(self.dim_desc.iter().cloned())
        {
            pos += i as isize * stride + suboffset;
        }
        pos
    }

    /// panic if indices.len() != ndim
    pub fn position(&self, indices: &[isize], vm: &VirtualMachine) -> PyResult<isize> {
        let mut pos = 0;
        for (i, (shape, stride, suboffset)) in indices
            .iter()
            .cloned()
            .zip_eq(self.dim_desc.iter().cloned())
        {
            let i = i.wrapped_at(shape).ok_or_else(|| {
                vm.new_index_error(format!("index out of bounds on dimension {}", i))
            })?;
            pos += i as isize * stride + suboffset;
        }
        Ok(pos)
    }

    pub fn for_each_segment<F>(&self, try_conti: bool, mut f: F)
    where
        F: FnMut(Range<isize>),
    {
        if self.ndim() == 0 {
            f(0..self.itemsize as isize);
            return;
        }
        if try_conti && self.is_last_dim_contiguous() {
            self._for_each_segment::<_, true>(0, 0, &mut f);
        } else {
            self._for_each_segment::<_, false>(0, 0, &mut f);
        }
    }

    fn _for_each_segment<F, const CONTI: bool>(&self, mut index: isize, dim: usize, f: &mut F)
    where
        F: FnMut(Range<isize>),
    {
        let (shape, stride, suboffset) = self.dim_desc[dim];
        if dim + 1 == self.ndim() {
            if CONTI {
                f(index..index + (shape * self.itemsize) as isize);
            } else {
                for _ in 0..shape {
                    let pos = index + suboffset;
                    f(pos..pos + self.itemsize as isize);
                    index += stride;
                }
            }
            return;
        }
        for _ in 0..shape {
            self._for_each_segment::<F, CONTI>(index + suboffset, dim + 1, f);
            index += stride;
        }
    }

    /// zip two BufferDescriptor with the same shape
    pub fn zip_eq<F>(&self, other: &Self, try_conti: bool, mut f: F)
    where
        F: FnMut(Range<isize>, Range<isize>) -> bool,
    {
        if self.ndim() == 0 {
            f(0..self.itemsize as isize, 0..other.itemsize as isize);
            return;
        }
        if try_conti && self.is_last_dim_contiguous() {
            self._zip_eq::<_, true>(other, 0, 0, 0, &mut f);
        } else {
            self._zip_eq::<_, false>(other, 0, 0, 0, &mut f);
        }
    }

    fn _zip_eq<F, const CONTI: bool>(
        &self,
        other: &Self,
        mut a_index: isize,
        mut b_index: isize,
        dim: usize,
        f: &mut F,
    ) where
        F: FnMut(Range<isize>, Range<isize>) -> bool,
    {
        let (shape, a_stride, a_suboffset) = self.dim_desc[dim];
        let (_b_shape, b_stride, b_suboffset) = other.dim_desc[dim];
        debug_assert_eq!(shape, _b_shape);
        if dim + 1 == self.ndim() {
            if CONTI {
                if f(
                    a_index..a_index + (shape * self.itemsize) as isize,
                    b_index..b_index + (shape * other.itemsize) as isize,
                ) {
                    return;
                }
            } else {
                for _ in 0..shape {
                    let a_pos = a_index + a_suboffset;
                    let b_pos = b_index + b_suboffset;
                    if f(
                        a_pos..a_pos + self.itemsize as isize,
                        b_pos..b_pos + other.itemsize as isize,
                    ) {
                        return;
                    }
                    a_index += a_stride;
                    b_index += b_stride;
                }
            }
            return;
        }

        for _ in 0..shape {
            self._zip_eq::<F, CONTI>(
                other,
                a_index + a_suboffset,
                b_index + b_suboffset,
                dim + 1,
                f,
            );
            a_index += a_stride;
            b_index += b_stride;
        }
    }

    fn is_last_dim_contiguous(&self) -> bool {
        let (_, stride, suboffset) = self.dim_desc[self.ndim() - 1];
        suboffset == 0 && stride == self.itemsize as isize
    }

    pub fn is_zero_in_shape(&self) -> bool {
        for (shape, _, _) in self.dim_desc.iter().cloned() {
            if shape == 0 {
                return true;
            }
        }
        false
    }

    // TODO: support fortain order
}

pub trait BufferResizeGuard<'a> {
    type Resizable: 'a;
    fn try_resizable_opt(&'a self) -> Option<Self::Resizable>;
    fn try_resizable(&'a self, vm: &VirtualMachine) -> PyResult<Self::Resizable> {
        self.try_resizable_opt().ok_or_else(|| {
            vm.new_buffer_error("Existing exports of data: object cannot be re-sized".to_owned())
        })
    }
}

#[pyclass(module = false, name = "vec_buffer")]
#[derive(Debug, PyPayload)]
pub struct VecBuffer {
    data: PyMutex<Vec<u8>>,
}

#[pyclass(flags(BASETYPE), with(Constructor))]
impl VecBuffer {
    pub fn take(&self) -> Vec<u8> {
        std::mem::take(&mut self.data.lock())
    }
}

impl From<Vec<u8>> for VecBuffer {
    fn from(data: Vec<u8>) -> Self {
        Self {
            data: PyMutex::new(data),
        }
    }
}

impl Unconstructible for VecBuffer {}

impl PyRef<VecBuffer> {
    pub fn into_pybuffer(self, readonly: bool) -> PyBuffer {
        let len = self.data.lock().len();
        PyBuffer::new(
            self.into(),
            BufferDescriptor::simple(len, readonly),
            &VEC_BUFFER_METHODS,
        )
    }

    pub fn into_pybuffer_with_descriptor(self, desc: BufferDescriptor) -> PyBuffer {
        PyBuffer::new(self.into(), desc, &VEC_BUFFER_METHODS)
    }
}

static VEC_BUFFER_METHODS: BufferMethods = BufferMethods {
    obj_bytes: |buffer| {
        PyMutexGuard::map_immutable(buffer.obj_as::<VecBuffer>().data.lock(), |x| x.as_slice())
            .into()
    },
    obj_bytes_mut: |buffer| {
        PyMutexGuard::map(buffer.obj_as::<VecBuffer>().data.lock(), |x| {
            x.as_mut_slice()
        })
        .into()
    },
    release: |_| {},
    retain: |_| {},
};
