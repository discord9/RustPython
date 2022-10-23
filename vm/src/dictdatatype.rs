//! Ordered dictionary implementation.
//! Inspired by: https://morepypy.blogspot.com/2015/01/faster-more-memory-efficient-and-more.html
//! And: https://www.youtube.com/watch?v=p33CVV29OG8
//! And: http://code.activestate.com/recipes/578375/

use crate::common::{
    hash,
    lock::{PyRwLock, PyRwLockReadGuard, PyRwLockWriteGuard},
};
use crate::{
    builtins::{PyInt, PyStr, PyStrInterned, PyStrRef},
    convert::ToPyObject,
    AsObject, Py, PyExact, PyObject, PyObjectRef, PyRefExact, PyResult, VirtualMachine,
};
use num_traits::ToPrimitive;
use std::{fmt, mem::size_of, ops::ControlFlow};

// HashIndex is intended to be same size with hash::PyHash
// but it doesn't mean the values are compatible with actual pyhash value

/// hash value of an object returned by __hash__
type HashValue = hash::PyHash;
/// index calculated by resolving collision
type HashIndex = hash::PyHash;
/// index into dict.indices
type IndexIndex = usize;
/// index into dict.entries
type EntryIndex = usize;

pub struct Dict<T = PyObjectRef> {
    inner: PyRwLock<DictInner<T>>,
}

#[cfg(feature = "gc")]
unsafe impl<T: crate::object::Trace> crate::object::Trace for Dict<T> {
    fn trace(&self, tracer_fn: &mut crate::object::TracerFn) {
        self.inner.trace(tracer_fn);
    }
}

impl<T> fmt::Debug for Dict<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Debug").finish()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(transparent)]
struct IndexEntry(i64);

impl IndexEntry {
    const FREE: Self = Self(-1);
    const DUMMY: Self = Self(-2);

    /// # Safety
    /// idx must not be one of FREE or DUMMY
    unsafe fn from_index_unchecked(idx: usize) -> Self {
        debug_assert!((idx as isize) >= 0);
        Self(idx as i64)
    }

    fn index(self) -> Option<usize> {
        if self.0 >= 0 {
            Some(self.0 as usize)
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct DictInner<T> {
    used: usize,
    filled: usize,
    indices: Vec<IndexEntry>,
    entries: Vec<Option<DictEntry<T>>>,
}

#[cfg(feature = "gc")]
unsafe impl<T: crate::object::Trace> crate::object::Trace for DictInner<T> {
    fn trace(&self, tracer_fn: &mut crate::object::TracerFn) {
        self.entries
            .iter()
            .map(|v| {
                if let Some(v) = v {
                    v.key.trace(tracer_fn);
                    v.value.trace(tracer_fn);
                }
            })
            .count();
    }
}

impl<T: Clone> Clone for Dict<T> {
    fn clone(&self) -> Self {
        Self {
            inner: PyRwLock::new(self.inner.read().clone()),
        }
    }
}

impl<T> Default for Dict<T> {
    fn default() -> Self {
        Self {
            inner: PyRwLock::new(DictInner {
                used: 0,
                filled: 0,
                indices: vec![IndexEntry::FREE; 8],
                entries: Vec::new(),
            }),
        }
    }
}

#[derive(Clone)]
struct DictEntry<T> {
    hash: HashValue,
    key: PyObjectRef,
    index: IndexIndex,
    value: T,
}
static_assertions::assert_eq_size!(DictEntry<PyObjectRef>, Option<DictEntry<PyObjectRef>>);

#[derive(Debug, PartialEq, Eq)]
pub struct DictSize {
    indices_size: usize,
    pub entries_size: usize,
    pub used: usize,
    filled: usize,
}

struct GenIndexes {
    idx: HashIndex,
    perturb: HashValue,
    mask: HashIndex,
}

impl GenIndexes {
    fn new(hash: HashValue, mask: HashIndex) -> Self {
        let hash = hash.abs();
        Self {
            idx: hash,
            perturb: hash,
            mask,
        }
    }
    fn next(&mut self) -> usize {
        let prev = self.idx;
        self.idx = prev
            .wrapping_mul(5)
            .wrapping_add(self.perturb)
            .wrapping_add(1);
        self.perturb >>= 5;
        (prev & self.mask) as usize
    }
}

impl<T> DictInner<T> {
    fn resize(&mut self, new_size: usize) {
        let new_size = {
            let mut i = 1;
            while i < new_size {
                i <<= 1;
            }
            i
        };
        self.indices = vec![IndexEntry::FREE; new_size];
        let mask = (new_size - 1) as i64;
        for (entry_idx, entry) in self.entries.iter_mut().enumerate() {
            if let Some(entry) = entry {
                let mut idxs = GenIndexes::new(entry.hash, mask);
                loop {
                    let index_index = idxs.next();
                    unsafe {
                        // Safety: index is always valid here
                        // index_index is generated by idxs
                        // entry_idx is saved one
                        let idx = self.indices.get_unchecked_mut(index_index);
                        if *idx == IndexEntry::FREE {
                            *idx = IndexEntry::from_index_unchecked(entry_idx);
                            entry.index = index_index;
                            break;
                        }
                    }
                }
            } else {
                //removed entry
            }
        }
        self.filled = self.used;
    }

    fn unchecked_push(
        &mut self,
        index: IndexIndex,
        hash_value: HashValue,
        key: PyObjectRef,
        value: T,
        index_entry: IndexEntry,
    ) {
        let entry = DictEntry {
            hash: hash_value,
            key,
            value,
            index,
        };
        let entry_index = self.entries.len();
        self.entries.push(Some(entry));
        self.indices[index] = unsafe {
            // SAFETY: entry_index is self.entries.len(). it never can
            // grow to `usize-2` because hash tables cannot full its index
            IndexEntry::from_index_unchecked(entry_index)
        };
        self.used += 1;
        if let IndexEntry::FREE = index_entry {
            self.filled += 1;
            if let Some(new_size) = self.should_resize() {
                self.resize(new_size)
            }
        }
    }

    fn size(&self) -> DictSize {
        DictSize {
            indices_size: self.indices.len(),
            entries_size: self.entries.len(),
            used: self.used,
            filled: self.filled,
        }
    }

    #[inline]
    fn should_resize(&self) -> Option<usize> {
        if self.filled * 3 > self.indices.len() * 2 {
            Some(self.used * 2)
        } else {
            None
        }
    }

    #[inline]
    fn get_entry_checked(&self, idx: EntryIndex, index_index: IndexIndex) -> Option<&DictEntry<T>> {
        match self.entries.get(idx) {
            Some(Some(entry)) if entry.index == index_index => Some(entry),
            _ => None,
        }
    }
}

type PopInnerResult<T> = ControlFlow<Option<DictEntry<T>>>;

impl<T: Clone> Dict<T> {
    fn read(&self) -> PyRwLockReadGuard<'_, DictInner<T>> {
        self.inner.read()
    }

    fn write(&self) -> PyRwLockWriteGuard<'_, DictInner<T>> {
        self.inner.write()
    }

    /// Store a key
    pub fn insert<K>(&self, vm: &VirtualMachine, key: &K, value: T) -> PyResult<()>
    where
        K: DictKey + ?Sized,
    {
        let hash = key.key_hash(vm)?;
        let _removed = loop {
            let (entry_index, index_index) = self.lookup(vm, key, hash, None)?;
            let mut inner = self.write();
            if let Some(index) = entry_index.index() {
                // Update existing key
                if let Some(entry) = inner.entries.get_mut(index) {
                    let entry = entry
                        .as_mut()
                        .expect("The dict was changed since we did lookup.");
                    if entry.index == index_index {
                        let removed = std::mem::replace(&mut entry.value, value);
                        // defer dec RC
                        break Some(removed);
                    } else {
                        // stuff shifted around, let's try again
                    }
                } else {
                    // The dict was changed since we did lookup. Let's try again.
                }
            } else {
                // New key:
                inner.unchecked_push(index_index, hash, key.to_pyobject(vm), value, entry_index);
                break None;
            }
        };
        Ok(())
    }

    pub fn contains<K: DictKey + ?Sized>(&self, vm: &VirtualMachine, key: &K) -> PyResult<bool> {
        let (entry, _) = self.lookup(vm, key, key.key_hash(vm)?, None)?;
        Ok(entry.index().is_some())
    }

    /// Retrieve a key
    #[cfg_attr(feature = "flame-it", flame("Dict"))]
    pub fn get<K: DictKey + ?Sized>(&self, vm: &VirtualMachine, key: &K) -> PyResult<Option<T>> {
        let hash = key.key_hash(vm)?;
        self._get_inner(vm, key, hash)
    }

    fn _get_inner<K: DictKey + ?Sized>(
        &self,
        vm: &VirtualMachine,
        key: &K,
        hash: HashValue,
    ) -> PyResult<Option<T>> {
        let ret = loop {
            let (entry, index_index) = self.lookup(vm, key, hash, None)?;
            if let Some(index) = entry.index() {
                let inner = self.read();
                if let Some(entry) = inner.get_entry_checked(index, index_index) {
                    break Some(entry.value.clone());
                } else {
                    // The dict was changed since we did lookup. Let's try again.
                    continue;
                }
            } else {
                break None;
            }
        };
        Ok(ret)
    }

    pub fn get_chain<K: DictKey + ?Sized>(
        &self,
        other: &Self,
        vm: &VirtualMachine,
        key: &K,
    ) -> PyResult<Option<T>> {
        let hash = key.key_hash(vm)?;
        if let Some(x) = self._get_inner(vm, key, hash)? {
            Ok(Some(x))
        } else {
            other._get_inner(vm, key, hash)
        }
    }

    pub fn clear(&self) {
        let _removed = {
            let mut inner = self.write();
            inner.indices.clear();
            inner.indices.resize(8, IndexEntry::FREE);
            inner.used = 0;
            inner.filled = 0;
            // defer dec rc
            std::mem::take(&mut inner.entries)
        };
    }

    /// Delete a key
    pub fn delete<K>(&self, vm: &VirtualMachine, key: &K) -> PyResult<()>
    where
        K: DictKey + ?Sized,
    {
        if self.delete_if_exists(vm, key)? {
            Ok(())
        } else {
            Err(vm.new_key_error(key.to_pyobject(vm)))
        }
    }

    pub fn delete_if_exists<K>(&self, vm: &VirtualMachine, key: &K) -> PyResult<bool>
    where
        K: DictKey + ?Sized,
    {
        self.delete_if(vm, key, |_| Ok(true))
    }

    /// pred should be VERY CAREFUL about what it does as it is called while
    /// the dict's internal mutex is held
    pub(crate) fn delete_if<K, F>(&self, vm: &VirtualMachine, key: &K, pred: F) -> PyResult<bool>
    where
        K: DictKey + ?Sized,
        F: Fn(&T) -> PyResult<bool>,
    {
        let hash = key.key_hash(vm)?;
        let deleted = loop {
            let lookup = self.lookup(vm, key, hash, None)?;
            match self.pop_inner_if(lookup, &pred)? {
                ControlFlow::Break(entry) => break entry,
                ControlFlow::Continue(()) => continue,
            }
        };
        Ok(deleted.is_some())
    }

    pub fn delete_or_insert(&self, vm: &VirtualMachine, key: &PyObject, value: T) -> PyResult<()> {
        let hash = key.key_hash(vm)?;
        let _removed = loop {
            let lookup = self.lookup(vm, key, hash, None)?;
            let (entry, index_index) = lookup;
            if entry.index().is_some() {
                match self.pop_inner(lookup) {
                    ControlFlow::Break(Some(entry)) => break Some(entry),
                    _ => continue,
                }
            } else {
                let mut inner = self.write();
                inner.unchecked_push(index_index, hash, key.to_owned(), value, entry);
                break None;
            }
        };
        Ok(())
    }

    pub fn setdefault<K, F>(&self, vm: &VirtualMachine, key: &K, default: F) -> PyResult<T>
    where
        K: DictKey + ?Sized,
        F: FnOnce() -> T,
    {
        let hash = key.key_hash(vm)?;
        let res = loop {
            let lookup = self.lookup(vm, key, hash, None)?;
            let (index_entry, index_index) = lookup;
            if let Some(index) = index_entry.index() {
                let inner = self.read();
                if let Some(entry) = inner.get_entry_checked(index, index_index) {
                    break entry.value.clone();
                } else {
                    // The dict was changed since we did lookup, let's try again.
                    continue;
                }
            } else {
                let value = default();
                let mut inner = self.write();
                inner.unchecked_push(
                    index_index,
                    hash,
                    key.to_pyobject(vm),
                    value.clone(),
                    index_entry,
                );
                break value;
            }
        };
        Ok(res)
    }

    #[allow(dead_code)]
    pub fn setdefault_entry<K, F>(
        &self,
        vm: &VirtualMachine,
        key: &K,
        default: F,
    ) -> PyResult<(PyObjectRef, T)>
    where
        K: DictKey + ?Sized,
        F: FnOnce() -> T,
    {
        let hash = key.key_hash(vm)?;
        let res = loop {
            let lookup = self.lookup(vm, key, hash, None)?;
            let (index_entry, index_index) = lookup;
            if let Some(index) = index_entry.index() {
                let inner = self.read();
                if let Some(entry) = inner.get_entry_checked(index, index_index) {
                    break (entry.key.clone(), entry.value.clone());
                } else {
                    // The dict was changed since we did lookup, let's try again.
                    continue;
                }
            } else {
                let value = default();
                let key = key.to_pyobject(vm);
                let mut inner = self.write();
                let ret = (key.clone(), value.clone());
                inner.unchecked_push(index_index, hash, key, value, index_entry);
                break ret;
            }
        };
        Ok(res)
    }

    pub fn len(&self) -> usize {
        self.read().used
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size(&self) -> DictSize {
        self.read().size()
    }

    pub fn next_entry(&self, mut position: EntryIndex) -> Option<(usize, PyObjectRef, T)> {
        let inner = self.read();
        loop {
            let entry = inner.entries.get(position)?;
            position += 1;
            if let Some(entry) = entry {
                break Some((position, entry.key.clone(), entry.value.clone()));
            }
        }
    }

    pub fn prev_entry(&self, mut position: EntryIndex) -> Option<(usize, PyObjectRef, T)> {
        let inner = self.read();
        loop {
            let entry = inner.entries.get(position)?;
            position = position.saturating_sub(1);
            if let Some(entry) = entry {
                break Some((position, entry.key.clone(), entry.value.clone()));
            }
        }
    }

    pub fn len_from_entry_index(&self, position: EntryIndex) -> usize {
        self.read().entries.len().saturating_sub(position)
    }

    pub fn has_changed_size(&self, old: &DictSize) -> bool {
        let current = self.read().size();
        current != *old
    }

    pub fn keys(&self) -> Vec<PyObjectRef> {
        self.read()
            .entries
            .iter()
            .filter_map(|v| v.as_ref().map(|v| v.key.clone()))
            .collect()
    }

    /// Lookup the index for the given key.
    #[cfg_attr(feature = "flame-it", flame("Dict"))]
    fn lookup<K: DictKey + ?Sized>(
        &self,
        vm: &VirtualMachine,
        key: &K,
        hash_value: HashValue,
        mut lock: Option<PyRwLockReadGuard<DictInner<T>>>,
    ) -> PyResult<LookupResult> {
        let mut idxs = None;
        let mut free_slot = None;
        let ret = 'outer: loop {
            let (entry_key, ret) = {
                let inner = lock.take().unwrap_or_else(|| self.read());
                let idxs = idxs.get_or_insert_with(|| {
                    GenIndexes::new(hash_value, (inner.indices.len() - 1) as i64)
                });
                loop {
                    let index_index = idxs.next();
                    let index_entry = *unsafe {
                        // Safety: index_index is generated
                        inner.indices.get_unchecked(index_index)
                    };
                    match index_entry {
                        IndexEntry::DUMMY => {
                            if free_slot.is_none() {
                                free_slot = Some(index_index);
                            }
                        }
                        IndexEntry::FREE => {
                            let idxs = match free_slot {
                                Some(free) => (IndexEntry::DUMMY, free),
                                None => (IndexEntry::FREE, index_index),
                            };
                            return Ok(idxs);
                        }
                        idx => {
                            let entry = unsafe {
                                // Safety: DUMMY and FREE are already handled above.
                                // i is always valid and entry always exists.
                                let i = idx.index().unwrap_unchecked();
                                inner.entries.get_unchecked(i).as_ref().unwrap_unchecked()
                            };
                            let ret = (idx, index_index);
                            if key.key_is(&entry.key) {
                                break 'outer ret;
                            } else if entry.hash == hash_value {
                                break (entry.key.clone(), ret);
                            } else {
                                // entry mismatch
                            }
                        }
                    }
                    // warn!("Perturb value: {}", i);
                }
            };
            // This comparison needs to be done outside the lock.
            if key.key_eq(vm, &entry_key)? {
                break 'outer ret;
            } else {
                // hash collision
            }

            // warn!("Perturb value: {}", i);
        };
        Ok(ret)
    }

    // returns Err(()) if changed since lookup
    fn pop_inner(&self, lookup: LookupResult) -> PopInnerResult<T> {
        self.pop_inner_if(lookup, |_| Ok::<_, std::convert::Infallible>(true))
            .unwrap_or_else(|x| match x {})
    }

    fn pop_inner_if<E>(
        &self,
        lookup: LookupResult,
        pred: impl Fn(&T) -> Result<bool, E>,
    ) -> Result<PopInnerResult<T>, E> {
        let (entry_index, index_index) = lookup;
        let entry_index = if let Some(entry_index) = entry_index.index() {
            entry_index
        } else {
            return Ok(ControlFlow::Break(None));
        };
        let inner = &mut *self.write();
        let slot = if let Some(slot) = inner.entries.get_mut(entry_index) {
            slot
        } else {
            // The dict was changed since we did lookup. Let's try again.
            return Ok(ControlFlow::Continue(()));
        };
        match slot {
            Some(entry) if entry.index == index_index => {
                if !pred(&entry.value)? {
                    return Ok(ControlFlow::Break(None));
                }
            }
            // The dict was changed since we did lookup. Let's try again.
            _ => return Ok(ControlFlow::Continue(())),
        }
        *unsafe {
            // index_index is result of lookup
            inner.indices.get_unchecked_mut(index_index)
        } = IndexEntry::DUMMY;
        inner.used -= 1;
        let removed = slot.take();
        Ok(ControlFlow::Break(removed))
    }

    /// Retrieve and delete a key
    pub fn pop<K: DictKey + ?Sized>(&self, vm: &VirtualMachine, key: &K) -> PyResult<Option<T>> {
        let hash_value = key.key_hash(vm)?;
        let removed = loop {
            let lookup = self.lookup(vm, key, hash_value, None)?;
            match self.pop_inner(lookup) {
                ControlFlow::Break(entry) => break entry.map(|e| e.value),
                ControlFlow::Continue(()) => continue,
            }
        };
        Ok(removed)
    }

    pub fn pop_back(&self) -> Option<(PyObjectRef, T)> {
        let mut inner = &mut *self.write();
        let entry = loop {
            let entry = inner.entries.pop()?;
            if let Some(entry) = entry {
                break entry;
            }
        };
        inner.used -= 1;
        *unsafe {
            // entry.index always refers valid index
            inner.indices.get_unchecked_mut(entry.index)
        } = IndexEntry::DUMMY;
        Some((entry.key, entry.value))
    }

    pub fn sizeof(&self) -> usize {
        let inner = self.read();
        size_of::<Self>()
            + size_of::<DictInner<T>>()
            + inner.indices.len() * size_of::<i64>()
            + inner.entries.len() * size_of::<DictEntry<T>>()
    }
}

type LookupResult = (IndexEntry, IndexIndex);

/// Types implementing this trait can be used to index
/// the dictionary. Typical usecases are:
/// - PyObjectRef -> arbitrary python type used as key
/// - str -> string reference used as key, this is often used internally
pub trait DictKey {
    type Owned: ToPyObject;
    fn _to_owned(&self, vm: &VirtualMachine) -> Self::Owned;
    fn to_pyobject(&self, vm: &VirtualMachine) -> PyObjectRef {
        self._to_owned(vm).to_pyobject(vm)
    }
    fn key_hash(&self, vm: &VirtualMachine) -> PyResult<HashValue>;
    fn key_is(&self, other: &PyObject) -> bool;
    fn key_eq(&self, vm: &VirtualMachine, other_key: &PyObject) -> PyResult<bool>;
    fn key_as_isize(&self, vm: &VirtualMachine) -> PyResult<isize>;
}

/// Implement trait for PyObjectRef such that we can use python objects
/// to index dictionaries.
impl DictKey for PyObject {
    type Owned = PyObjectRef;
    #[inline(always)]
    fn _to_owned(&self, _vm: &VirtualMachine) -> Self::Owned {
        self.to_owned()
    }
    #[inline(always)]
    fn key_hash(&self, vm: &VirtualMachine) -> PyResult<HashValue> {
        self.hash(vm)
    }
    #[inline(always)]
    fn key_is(&self, other: &PyObject) -> bool {
        self.is(other)
    }
    #[inline(always)]
    fn key_eq(&self, vm: &VirtualMachine, other_key: &PyObject) -> PyResult<bool> {
        vm.identical_or_equal(self, other_key)
    }
    #[inline]
    fn key_as_isize(&self, vm: &VirtualMachine) -> PyResult<isize> {
        self.try_index(vm)?.try_to_primitive(vm)
    }
}

impl DictKey for Py<PyStr> {
    type Owned = PyStrRef;
    #[inline(always)]
    fn _to_owned(&self, _vm: &VirtualMachine) -> Self::Owned {
        self.to_owned()
    }
    #[inline]
    fn key_hash(&self, vm: &VirtualMachine) -> PyResult<HashValue> {
        Ok(self.hash(vm))
    }
    #[inline(always)]
    fn key_is(&self, other: &PyObject) -> bool {
        self.is(other)
    }

    fn key_eq(&self, vm: &VirtualMachine, other_key: &PyObject) -> PyResult<bool> {
        if self.is(other_key) {
            Ok(true)
        } else if let Some(pystr) = str_exact(other_key, vm) {
            Ok(pystr.as_str() == self.as_str())
        } else {
            vm.bool_eq(self.as_object(), other_key)
        }
    }
    #[inline(always)]
    fn key_as_isize(&self, vm: &VirtualMachine) -> PyResult<isize> {
        self.as_object().key_as_isize(vm)
    }
}

impl DictKey for PyStrInterned {
    type Owned = PyRefExact<PyStr>;
    #[inline]
    fn _to_owned(&self, _vm: &VirtualMachine) -> Self::Owned {
        let zelf: &'static PyStrInterned = unsafe { &*(self as *const _) };
        zelf.to_exact()
    }
    #[inline]
    fn key_hash(&self, vm: &VirtualMachine) -> PyResult<HashValue> {
        (**self).key_hash(vm)
    }
    #[inline]
    fn key_is(&self, other: &PyObject) -> bool {
        (**self).key_is(other)
    }
    #[inline]
    fn key_eq(&self, vm: &VirtualMachine, other_key: &PyObject) -> PyResult<bool> {
        (**self).key_eq(vm, other_key)
    }
    #[inline]
    fn key_as_isize(&self, vm: &VirtualMachine) -> PyResult<isize> {
        (**self).key_as_isize(vm)
    }
}

impl DictKey for PyExact<PyStr> {
    type Owned = PyRefExact<PyStr>;
    #[inline]
    fn _to_owned(&self, _vm: &VirtualMachine) -> Self::Owned {
        self.to_owned()
    }
    #[inline(always)]
    fn key_hash(&self, vm: &VirtualMachine) -> PyResult<HashValue> {
        (**self).key_hash(vm)
    }
    #[inline(always)]
    fn key_is(&self, other: &PyObject) -> bool {
        (**self).key_is(other)
    }
    #[inline(always)]
    fn key_eq(&self, vm: &VirtualMachine, other_key: &PyObject) -> PyResult<bool> {
        (**self).key_eq(vm, other_key)
    }
    #[inline(always)]
    fn key_as_isize(&self, vm: &VirtualMachine) -> PyResult<isize> {
        (**self).key_as_isize(vm)
    }
}

// AsRef<str> fit this case but not possible in rust 1.46

/// Implement trait for the str type, so that we can use strings
/// to index dictionaries.
impl DictKey for str {
    type Owned = String;
    #[inline(always)]
    fn _to_owned(&self, _vm: &VirtualMachine) -> Self::Owned {
        self.to_owned()
    }
    #[inline]
    fn key_hash(&self, vm: &VirtualMachine) -> PyResult<HashValue> {
        // follow a similar route as the hashing of PyStrRef
        Ok(vm.state.hash_secret.hash_str(self))
    }
    #[inline(always)]
    fn key_is(&self, _other: &PyObject) -> bool {
        // No matter who the other pyobject is, we are never the same thing, since
        // we are a str, not a pyobject.
        false
    }

    fn key_eq(&self, vm: &VirtualMachine, other_key: &PyObject) -> PyResult<bool> {
        if let Some(pystr) = str_exact(other_key, vm) {
            Ok(pystr.as_str() == self)
        } else {
            // Fall back to PyObjectRef implementation.
            let s = vm.ctx.new_str(self);
            s.key_eq(vm, other_key)
        }
    }

    fn key_as_isize(&self, vm: &VirtualMachine) -> PyResult<isize> {
        Err(vm.new_type_error("'str' object cannot be interpreted as an integer".to_owned()))
    }
}

impl DictKey for String {
    type Owned = String;
    #[inline]
    fn _to_owned(&self, _vm: &VirtualMachine) -> Self::Owned {
        self.clone()
    }

    fn key_hash(&self, vm: &VirtualMachine) -> PyResult<HashValue> {
        self.as_str().key_hash(vm)
    }

    fn key_is(&self, other: &PyObject) -> bool {
        self.as_str().key_is(other)
    }

    fn key_eq(&self, vm: &VirtualMachine, other_key: &PyObject) -> PyResult<bool> {
        self.as_str().key_eq(vm, other_key)
    }

    fn key_as_isize(&self, vm: &VirtualMachine) -> PyResult<isize> {
        self.as_str().key_as_isize(vm)
    }
}

impl DictKey for usize {
    type Owned = usize;
    #[inline]
    fn _to_owned(&self, _vm: &VirtualMachine) -> Self::Owned {
        *self
    }

    fn key_hash(&self, vm: &VirtualMachine) -> PyResult<HashValue> {
        Ok(vm.state.hash_secret.hash_value(self))
    }

    fn key_is(&self, _other: &PyObject) -> bool {
        false
    }

    fn key_eq(&self, vm: &VirtualMachine, other_key: &PyObject) -> PyResult<bool> {
        if let Some(int) = other_key.payload_if_exact::<PyInt>(vm) {
            if let Some(i) = int.as_bigint().to_usize() {
                Ok(i == *self)
            } else {
                Ok(false)
            }
        } else {
            let int = vm.ctx.new_int(*self);
            vm.bool_eq(int.as_ref(), other_key)
        }
    }

    fn key_as_isize(&self, _vm: &VirtualMachine) -> PyResult<isize> {
        Ok(*self as isize)
    }
}

fn str_exact<'a>(obj: &'a PyObject, vm: &VirtualMachine) -> Option<&'a PyStr> {
    if obj.class().is(vm.ctx.types.str_type) {
        obj.payload::<PyStr>()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{Dict, DictKey};
    use crate::common::ascii;
    use crate::Interpreter;

    #[test]
    fn test_insert() {
        Interpreter::without_stdlib(Default::default()).enter(|vm| {
            let dict = Dict::default();
            assert_eq!(0, dict.len());

            let key1 = vm.new_pyobj(true);
            let value1 = vm.new_pyobj(ascii!("abc"));
            dict.insert(vm, &*key1, value1).unwrap();
            assert_eq!(1, dict.len());

            let key2 = vm.new_pyobj(ascii!("x"));
            let value2 = vm.new_pyobj(ascii!("def"));
            dict.insert(vm, &*key2, value2.clone()).unwrap();
            assert_eq!(2, dict.len());

            dict.insert(vm, &*key1, value2.clone()).unwrap();
            assert_eq!(2, dict.len());

            dict.delete(vm, &*key1).unwrap();
            assert_eq!(1, dict.len());

            dict.insert(vm, &*key1, value2.clone()).unwrap();
            assert_eq!(2, dict.len());

            assert_eq!(true, dict.contains(vm, &*key1).unwrap());
            assert_eq!(true, dict.contains(vm, "x").unwrap());

            let val = dict.get(vm, "x").unwrap().unwrap();
            vm.bool_eq(&val, &value2)
                .expect("retrieved value must be equal to inserted value.");
        })
    }

    macro_rules! hash_tests {
        ($($name:ident: $example_hash:expr,)*) => {
            $(
                #[test]
                fn $name() {
                    check_hash_equivalence($example_hash);
                }
            )*
        }
    }

    hash_tests! {
        test_abc: "abc",
        test_x: "x",
    }

    fn check_hash_equivalence(text: &str) {
        Interpreter::without_stdlib(Default::default()).enter(|vm| {
            let value1 = text;
            let value2 = vm.new_pyobj(value1.to_owned());

            let hash1 = value1.key_hash(vm).expect("Hash should not fail.");
            let hash2 = value2.key_hash(vm).expect("Hash should not fail.");
            assert_eq!(hash1, hash2);
        })
    }
}
