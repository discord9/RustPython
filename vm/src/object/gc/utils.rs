use std::ptr::NonNull;

use parking_lot::RwLockReadGuard;

use crate::{PyObject, VirtualMachine, PyResult, AsObject};

/// This is safe to Send only because VirtualMachine is guaranteed to executed per-thread, and upon creation of virtual machine
/// no read lock is acquired, see [`VirtualMachine::start_thread`]
pub struct GCReadLock {
    pub guard: Option<RwLockReadGuard<'static, ()>>,
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

impl PyObject{
    /// call `drop_only` in vtable, which run drop but not dealloc
    pub(in crate::object) unsafe fn call_vtable_drop_only(ptr: NonNull<PyObject>){
        let drop_only = ptr.as_ref().0.vtable.drop_only;
        // call drop only when there are no references in scope - stacked borrows stuff
        drop_only(ptr.as_ptr());
    }

    #[inline(always)] // the outer function is never inlined
    pub(crate) fn call_del(&self) -> Result<(), ()> {
        // __del__ is mostly not implemented
        #[inline(never)]
        #[cold]
        fn call_slot_del(
            zelf: &PyObject,
            slot_del: fn(&PyObject, &VirtualMachine) -> PyResult<()>,
        ) -> Result<(), ()> {
            let ret = crate::vm::thread::with_vm(zelf, |vm| {
                zelf.0.ref_count.inc();
                if let Err(e) = slot_del(zelf, vm) {
                    let del_method = zelf.get_class_attr(identifier!(vm, __del__)).unwrap();
                    vm.run_unraisable(e, None, del_method);
                }
                zelf.0.ref_count.dec()
            });
            match ret {
                // the decref right above set ref_count back to 0
                Some(true) => Ok(()),
                // we've been resurrected by __del__
                Some(false) => Err(()),
                None => {
                    warn!("couldn't run __del__ method for object");
                    Ok(())
                }
            }
        }

        // CPython-compatible drop implementation
        let del = self.class().mro_find_map(|cls| cls.slots.del.load());
        if let Some(slot_del) = del {
            call_slot_del(self, slot_del)?;
        }

        Ok(())
    }
}