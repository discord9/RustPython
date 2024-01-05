use std::collections::HashMap;

use crate::common::lock::PyMutex;

use crate::common::lock::PyRwLockReadGuard;

/// This is safe to Send only because VirtualMachine is guaranteed to executed per-thread, and upon creation of virtual machine
/// no read lock is acquired, see [`VirtualMachine::start_thread`]
pub struct GCReadLock {
    pub guard: Option<PyRwLockReadGuard<'static, ()>>,
    pub recursive: usize,
}

impl Default for GCReadLock {
    fn default() -> Self {
        Self::new()
    }
}

impl GCReadLock {
    pub fn new() -> Self {
        Self {
            guard: None,
            recursive: 0,
        }
    }
    pub fn take(&mut self) -> Self {
        let guard = self.guard.take();
        let recursive = self.recursive;
        self.recursive = 0;
        Self { guard, recursive }
    }
}

unsafe impl Send for GCReadLock {}

#[derive(PartialEq, Eq)]
pub enum GcStatus {
    /// should be drop by caller
    ShouldDrop,
    /// because object is part of a garbage cycle, we don't want double dealloc
    /// or use after drop, so run `__del__` only. Drop(destructor)&dealloc is handle by gc
    GarbageCycle,
    /// already buffered, will be dealloc by collector, caller should call [`PyObject::drop_only`] to run destructor only but not dealloc memory region
    BufferedDrop,
    /// should keep and not drop by caller
    ShouldKeep,
    /// Do Nothing, even if ref cnt is zero, perhaps because it is RAII's deeds
    DoNothing,
}

/// simply record how many object is collected
#[derive(Debug, Default)]
pub struct GcResult {
    /// object that is not in cycle and is collected
    pub acyclic_cnt: usize,
    /// object that is in cycle and is collected
    pub cyclic_cnt: usize,
}

impl GcResult {
    fn new(tuple: (usize, usize)) -> Self {
        Self {
            acyclic_cnt: tuple.0,
            cyclic_cnt: tuple.1,
        }
    }
}

impl From<(usize, usize)> for GcResult {
    fn from(t: (usize, usize)) -> Self {
        Self::new(t)
    }
}

impl From<GcResult> for (usize, usize) {
    fn from(g: GcResult) -> Self {
        (g.acyclic_cnt, g.cyclic_cnt)
    }
}

impl From<GcResult> for usize {
    fn from(g: GcResult) -> Self {
        g.acyclic_cnt + g.cyclic_cnt
    }
}
