#[derive(PartialEq, Eq)]
pub enum GcStatus {
    /// should be drop by caller
    ShouldDrop,
    /// because object is part of a garbage cycle, we don't want double dealloc
    /// or use after drop, so run `__del__` only. Drop(destructor)&dealloc is handle by gc
    GarbageCycle,
    /// already buffered, will be dealloc by collector, caller should call [`PyObject::del_Drop`] to run destructor only but not dealloc memory region
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
    acyclic_cnt: usize,
    /// object that is in cycle and is collected
    cyclic_cnt: usize,
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
