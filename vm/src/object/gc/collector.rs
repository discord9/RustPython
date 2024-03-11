use super::header::Color;
use crate::object::gc::utils::{GcResult, GcStatus};
use crate::object::gc::GcObj;
use crate::object::gc::GcObjRef;
use crate::object::Traverse;
use crate::{PyObject, VirtualMachine};
use itertools::Itertools;
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockWriteGuard};
use std::ptr::NonNull;
use std::sync::Arc;

use once_cell::sync::Lazy;

/// The global cycle collector, which collect cycle references for PyInner<T>
pub static GLOBAL_COLLECTOR: Lazy<Arc<Collector>> = Lazy::new(|| Arc::new(Default::default()));

/// only use for roots's pointer to object, mark `NonNull` as safe to send
/// which is true only if when we are holding a gc pause lock
/// but essentially, we are using raw pointer here only because we know only one thread can be doing GC at a time
#[repr(transparent)]
struct WrappedPtr(NonNull<PyObject>);
unsafe impl Send for WrappedPtr {}
impl std::ops::Deref for WrappedPtr {
    type Target = NonNull<PyObject>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for WrappedPtr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl From<NonNull<PyObject>> for WrappedPtr {
    fn from(ptr: NonNull<PyObject>) -> Self {
        Self(ptr)
    }
}

impl From<WrappedPtr> for NonNull<PyObject> {
    fn from(w: WrappedPtr) -> Self {
        w.0
    }
}

#[derive(Debug)]
pub struct GcCond {
    pub(crate) threshold: RwLock<usize>,
    pub(crate) root_cleanup_size: RwLock<usize>,
    pub(crate) alloc_cnt: RwLock<usize>,
    pub(crate) dealloc_cnt: RwLock<usize>,
}

impl std::default::Default for GcCond {
    fn default() -> Self {
        Self {
            threshold: RwLock::new(700),
            root_cleanup_size: RwLock::new(10000),
            alloc_cnt: Default::default(),
            dealloc_cnt: Default::default(),
        }
    }
}

impl Collector {
    pub fn inc_alloc_cnt(&self) {
        *self.gc_cond.alloc_cnt.write() += 1;
    }
    pub fn inc_dealloc_cnt(&self) {
        *self.gc_cond.dealloc_cnt.write() += 1;
    }
}

/// This is the singleton collector that manage all RustPython Garbage Collect
/// within a process, which is a stop-the-world, mark-sweep, tracing collector
///
/// TODO: maybe using one per Virtual Machine/thread?
///
// Collector use thread-safe mutex because multiple non-thread vm use the same collector
pub struct Collector {
    roots: Mutex<Vec<WrappedPtr>>,
    /// for stop the world, will be try to check lock every time deref ObjecteRef
    /// to achive pausing
    /// TODO(discord9): stop-the-world with pausing between every bytecode execution
    pause: RwLock<()>,
    gc_cond: GcCond,
    is_enabled: Mutex<bool>,
    /// acquire this to prevent a new gc to happen before this gc is completed
    /// but also resume-the-world early
    cleanup_cycle: Mutex<()>,
}

impl std::fmt::Debug for Collector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CcSync")
            .field(
                "roots",
                &format!("[{} objects in buffer]", self.roots_len()),
            )
            .field("pause", &self.pause)
            .field("gc_cond", &self.gc_cond)
            .finish()
    }
}

impl Default for Collector {
    fn default() -> Self {
        Self {
            roots: Default::default(),
            pause: Default::default(),
            gc_cond: Default::default(),
            is_enabled: Mutex::new(true),
            cleanup_cycle: Mutex::new(()),
        }
    }
}

// core of gc algorithm
impl Collector {
    fn try_pause_self(&self, force: bool) -> Option<RwLockWriteGuard<()>> {
        let lock = {
            #[cfg(feature = "threading")]
            {
                // if can't access pause lock for a second, return because gc is not that emergency,
                // also normal call to `gc.collect()` can usually acquire that lock unless something is wrong
                if force {
                    // if is forced to gc, wait a while for write lock
                    match self.pause.try_write_for(std::time::Duration::from_secs(1)) {
                        Some(v) => v,
                        None => {
                            warn!("Can't acquire lock to stop the world after waiting.");
                            return None;
                        }
                    }
                } else {
                    // if not forced to gc, a non-blocking check to see if gc is possible
                    match self.pause.try_write() {
                        Some(v) => v,
                        None => {
                            warn!("Fast-Path GC fail to acquire write lock.");
                            return None;
                        }
                    }
                }
            }
            // also when no threading, there is actually no need to get a lock,(because every thread have it's own gc)
            // but for the sake of using same code(and defendsive), we acquire one anyway
            #[cfg(not(feature = "threading"))]
            {
                // when not threading, no deadlock should occur?
                let _force = force;
                self.pause.try_write().unwrap()
            }
        };
        Some(lock)
    }

    /// `force` means a explicit call to `gc.collect()`, which will try to acquire a lock for a little longer
    /// and have a higher chance to actually collect garbage
    #[allow(unused)]
    fn collect_cycles(&self, force: bool) -> GcResult {
        // acquire stop-the-world lock
        let lock = self.try_pause_self(force);
        let lock = match lock {
            Some(lock) => lock,
            None => return (0, 0).into(),
        };

        // if is empty or last gc's cleanup is not done, return early
        if self.roots_len() == 0 || self.cleanup_cycle.is_locked() {
            return (0, 0).into();
        }

        // the three main step of GC
        // 1. mark roots: which get trial DECREF object so cycles get zero rc
        // 2. scan roots: get non-cycle object back to normal ref cnt
        // 3. collect roots: collect cycles starting from roots
        let freed = Self::mark_roots(&mut *self.roots.lock());
        Self::scan_roots(&mut *self.roots.lock());
        let ret_cycle = self.collect_roots(self.roots.lock(), lock);

        (freed, ret_cycle).into()
    }

    fn mark_roots<R>(mut roots: R) -> usize
    where
        R: AsMut<Vec<WrappedPtr>>,
    {
        let mut freed = 0;
        let old_roots: Vec<_> = { roots.as_mut().drain(..).collect() };
        let mut new_roots = old_roots
            .into_iter()
            .filter(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                if obj.header().color() == Color::Purple {
                    Self::mark_gray(obj);
                    true
                } else {
                    obj.header().set_buffered(false);
                    if obj.header().color() == Color::Black && obj.header().rc() == 0 {
                        freed += 1;
                        unsafe {
                            // only dealloc here, because already drop(only) in Object's impl Drop
                            // PyObject::dealloc_only(ptr.cast::<PyObject>());
                            let ret = PyObject::dealloc_only(**ptr);
                            debug_assert!(ret);
                            // obj is dangling after this line?
                        }
                    }
                    false
                }
            })
            .collect();
        roots.as_mut().append(&mut new_roots);
        freed
    }

    fn mark_gray(obj: GcObjRef) {
        if obj.header().color() != Color::Gray {
            obj.header().set_color(Color::Gray);
            obj.traverse(&mut |ch| {
                if ch.header().is_leaked() {
                    return;
                }
                ch.header().dec();
                Self::mark_gray(ch);
            });
        }
    }

    fn scan_roots<R>(mut roots: R)
    where
        R: AsMut<Vec<WrappedPtr>>,
    {
        roots
            .as_mut()
            .iter()
            .map(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                Self::scan(obj);
            })
            .count();
    }

    fn scan(obj: GcObjRef) {
        if obj.header().color() == Color::Gray {
            if obj.header().rc() > 0 {
                Self::scan_black(obj)
            } else {
                obj.header().set_color(Color::White);
                obj.traverse(&mut |ch| {
                    if ch.header().is_leaked() {
                        return;
                    }
                    Self::scan(ch);
                });
            }
        }
    }

    fn scan_black(obj: GcObjRef) {
        obj.header().set_color(Color::Black);
        obj.traverse(&mut |ch| {
            if ch.header().is_leaked() {
                return;
            }
            ch.header().inc();
            if ch.header().color() != Color::Black {
                debug_assert!(
                    ch.header().color() == Color::Gray || ch.header().color() == Color::White
                );
                Self::scan_black(ch)
            }
        });
    }

    fn collect_roots(
        &self,
        mut roots_lock: MutexGuard<Vec<WrappedPtr>>,
        lock: RwLockWriteGuard<()>,
    ) -> usize {
        // Collecting the nodes into this Vec is difference from the original
        // Bacon-Rajan paper. We need this because we have destructors(RAII) and
        // running them during traversal will cause cycles to be broken which
        // ruins the rest of our traversal.
        let mut white = Vec::new();
        let roots: Vec<_> = { roots_lock.drain(..).collect() };
        // future inc/dec will accesss roots so drop lock in here
        drop(roots_lock);
        // release gc pause lock in here, for after this line no white garbage will be access by mutator
        roots
            .into_iter()
            .map(|ptr| {
                let obj = unsafe { ptr.as_ref() };
                obj.header().set_buffered(false);
                Self::collect_white(obj, &mut white);
            })
            .count();
        let len_white = white.len();
        if !white.is_empty() {
            info!("Cyclic garbage founded, count={}", white.len());
        }

        // mark the end of GC, but another gc can only begin after acquire cleanup_cycle lock
        // because a dead cycle can't actively change object graph anymore
        // TODO: is clean up lock necessary?
        // let _cleanup_lock = self.cleanup_cycle.lock();
        // unlock fair so high freq gc wouldn't stop the world forever
        #[cfg(feature = "threading")]
        RwLockWriteGuard::unlock_fair(lock);
        #[cfg(not(feature = "threading"))]
        drop(lock);

        self.free_cycles(white);

        len_white
    }
    fn collect_white(obj: GcObjRef, white: &mut Vec<NonNull<GcObj>>) {
        if obj.header().color() == Color::White && !obj.header().buffered() {
            obj.header().set_color(Color::Black);
            obj.header().set_in_cycle(true);
            obj.traverse(&mut |ch| {
                if ch.header().is_leaked() {
                    return;
                }
                Self::collect_white(ch, white)
            });
            white.push(NonNull::from(obj));
        }
    }

    /// free everything in white, safe to use even when those objects form cycle refs
    /// This is a very delicated process and very error-prone, see:
    /// https://devguide.python.org/internals/garbage-collector/#destroying-unreachable-objects
    /// for more details
    fn free_cycles(&self, white: Vec<NonNull<PyObject>>) -> usize {
        // TODO: impl PEP 442
        // 0. add back one ref for all thing in white
        // 1. clear weakref
        // 2. PEP 442 deal with `__del__`: https://peps.python.org/pep-0442/
        // 3. deal with resurrected object by run cycle detect on them one more times
        // NOTE: require Stop-The-World lock
        // 4. drop first, then dealloc to avoid access dealloced memory
        // TODO: fix this function
        // Run drop on each of nodes.
        #[cfg(debug_assertions)]
        {
            // check if the pointers are not same
            let mut ptrs = white.iter().map(|i| i.as_ptr()).collect_vec();
            ptrs.sort_unstable();
            ptrs.dedup();
            debug_assert_eq!(ptrs.len(), white.len());
        }
        info!("Before inc ref count: white.len()={}", white.len());
        white.iter().for_each(|i| {
            // Calling drop() will decrement the reference count on any of our live children.
            // However, during trial deletion the reference count was already decremented
            // so we'll end up decrementing twice. To avoid that, we increment the count
            // just before calling drop() so that it balances out. This is another difference
            // from the original paper caused by having destructors that we need to run.

            let obj = unsafe { i.as_ref() };
            obj.traverse(&mut |ch| {
                if ch.header().is_leaked() {
                    return;
                }
                if ch.header().rc() > 0 {
                    ch.header().inc();
                }
            });
        });

        // 2. run __del__
        let mut resurrected_not_buffered: Vec<WrappedPtr> = Vec::new();
        let white = white
            .into_iter()
            .filter(|i| unsafe {
                let zelf = i.as_ref();
                if let Err(()) = zelf.call_del() {
                    let mut header = zelf.header();
                    if !header.buffered() {
                        header.set_buffered(true);
                        resurrected_not_buffered.push(WrappedPtr::from(*i));
                    }
                    false
                } else {
                    true
                }
            })
            .collect_vec();

        info!("Before clear weakref: white.len()={}", white.len());
        // 1. Handle and clean weak references
        for i in white.iter() {
            unsafe {
                let zelf = i.as_ref();
                if let Some(wrl) = zelf.weak_ref_list() {
                    wrl.clear();
                }
            }
        }

        info!("Before rebuffer resurrected: resurrected_not_buffered.len()={}", resurrected_not_buffered.len());
        // 3. run cycle detect and gc on resurrected one more times
        // and save resurrected object for next gc
        {
            let mut roots = self.roots.lock();
            roots.extend(resurrected_not_buffered);
        }
        for i in white.iter() {
            unsafe { PyObject::call_vtable_drop_only(*i) }
        }

        for i in white.iter() {
            let ret = unsafe { PyObject::dealloc_only(*i) };
            debug_assert!(ret);
        }

        info!("Cyclic garbage collected, count={}", white.len());

        white.len()
    }
}

// utility function for `Collector`
impl Collector {
    fn roots_len(&self) -> usize {
        self.roots.lock().len()
    }

    // basic inc/dec operation
    pub fn increment(&self, obj: &PyObject) {
        if obj.header().is_leaked() {
            return;
        }
        // acquire exclusive access to obj's header

        // TODO: this might not be necessary
        // prevent starting a gc in the middle of change header state
        // let _lock_gc = obj.header().try_pausing();

        obj.header().inc_black();
    }

    /// if the last ref to a object call decrement() on object,
    /// then this object should be considered freed. However it's backing alllocating
    /// _might_ not be freed immediately.
    /// the actual detail of drop is decoupled from this function for cleaner code
    pub fn decrement(&self, obj: &PyObject) -> GcStatus {
        let mut header = obj.header();
        if header.is_leaked() {
            // a leaked object should always keep
            return GcStatus::ShouldKeep;
        }

        // acquire exclusive access to obj's header, so no decrement in the middle of increment of vice versa

        // prevent RAII Drop to drop below zero
        if header.rc() > 0 {
            let rc = header.dec();
            drop(header);
            if rc == 0 {
                self.inc_dealloc_cnt();
                self.release(obj)
            } else {
                self.possible_root(obj);
                GcStatus::ShouldKeep
            }
        } else {
            // FIXME(discord9): confirm if already rc==0 then should do nothing
            GcStatus::DoNothing
        }
    }
}

/// dealing with drop and marking roots
impl Collector {
    fn release(&self, obj: &PyObject) -> GcStatus {
        // because drop obj itself will drop all ObjRef store by object itself once more,
        // so balance out in here
        // by doing nothing
        // instead of minus one and do:
        // ```ignore
        // obj.trace(&mut |ch| {
        //   self.decrement(ch);
        // });
        //```
        let mut header = obj.header();
        header.set_color(Color::Black);

        // TODO(discord9): just drop in here, not by the caller, which is cleaner
        // before it is free in here,
        // but now change to passing message to allow it to drop outside
        match header.buffered() {
            true => GcStatus::BufferedDrop,
            false => GcStatus::ShouldDrop,
        }
    }

    fn possible_root(&self, obj: &PyObject) {
        let mut header = obj.header();
        if header.color() != Color::Purple {
            header.set_color(Color::Purple);
            // prevent add to buffer for multiple times
            let mut roots = self.roots.lock();
            if !header.buffered() {
                header.set_buffered(true);
                roots.push(NonNull::from(obj).into());
            }
        }
    }
}

/// Prevent STW lock to untimely trigger a GC in another thread
pub fn pausing(vm: &VirtualMachine) {
    let mut pause = vm.pause_lock.borrow_mut();
    if pause.guard.is_some() {
        pause.recursive += 1;
    } else {
        assert_eq!(pause.recursive, 0);
        let lock = GLOBAL_COLLECTOR.pause.read_recursive();
        pause.guard = Some(lock);
    }
}

/// Allowing STW lock to start a GC
pub fn resuming(vm: &VirtualMachine) {
    let mut pause = vm.pause_lock.borrow_mut();
    if pause.guard.is_some() {
        if pause.recursive > 0 {
            pause.recursive -= 1;
        } else {
            assert_eq!(pause.recursive, 0);
            pause.guard = None;
        }
    } else {
        assert_eq!(pause.recursive, 0);
        warn!("resuming without pausing");
    }
}

/// check if need to gc by checking if alloc_cnt > threshold + dealloc_cnt
pub fn need_gc(vm: &VirtualMachine) -> bool {
    let threshold = *GLOBAL_COLLECTOR.gc_cond.threshold.read();
    let root_len = GLOBAL_COLLECTOR.roots_len();
    let root_max_len = *GLOBAL_COLLECTOR.gc_cond.root_cleanup_size.read();
    let mut alloc_cnt = GLOBAL_COLLECTOR.gc_cond.alloc_cnt.write();
    let mut dealloc_cnt = GLOBAL_COLLECTOR.gc_cond.dealloc_cnt.write();
    let gc_cond = (*alloc_cnt > threshold + *dealloc_cnt) || root_len > root_max_len;
    // prevent stocking too many dealloced objects in roots buffer
    let is_enabled = *GLOBAL_COLLECTOR.is_enabled.lock();
    let ret = gc_cond && is_enabled;
    if ret {
        // reset counter
        *alloc_cnt = 0;
        *dealloc_cnt = 0;
    }
    ret
}

pub fn try_collect(vm: &VirtualMachine) -> usize {
    if isenabled(vm) {
        let res = stop_the_world_gc(vm, false);
        res.acyclic_cnt + res.cyclic_cnt
    } else {
        0
    }
}

pub fn collect(vm: &VirtualMachine) -> usize {
    if isenabled(vm) {
        let res = stop_the_world_gc(vm, true);
        res.acyclic_cnt + res.cyclic_cnt
    } else {
        0
    }
}

/// unlock current thread's STW read lock and acquire write lock
fn stop_the_world_gc(vm: &VirtualMachine, force: bool) -> GcResult {
    // not using `unlocked` because might need to recursively check pause_lock
    // which volatile the value of `RefCell`
    let mut read_lock_stat = vm.pause_lock.borrow_mut().take();
    read_lock_stat.guard = None;

    let res = GLOBAL_COLLECTOR.collect_cycles(force);

    read_lock_stat.guard = Some(GLOBAL_COLLECTOR.pause.read_recursive());
    *vm.pause_lock.borrow_mut() = read_lock_stat;
    res
}

pub fn isenabled(_vm: &VirtualMachine) -> bool {
    *GLOBAL_COLLECTOR.is_enabled.lock()
}

pub fn setenabled(_vm: &VirtualMachine, enabled: bool) {
    *GLOBAL_COLLECTOR.is_enabled.lock() = enabled;
}
