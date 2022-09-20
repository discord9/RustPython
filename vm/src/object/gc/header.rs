use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
#[cfg(feature = "threading")]
pub struct GcHeader {
    ref_cnt: AtomicUsize,
    color: Mutex<Color>,
    buffered: Mutex<bool>,
    // log_ptr: Mutex<Option<LogPointer>>,
}

#[cfg(not(feature = "threading"))]
pub struct GcHeader {
    ref_cnt: usize,
    color: core::cell::Cell<Color>,
    buffered: core::cell::Cell<bool>,
}

impl GcHeader {
    pub fn new() -> Self {
        Self {
            ref_cnt: 1.into(),
            color: Mutex::new(Color::Black),
            buffered: Mutex::new(false),
        }
    }
    pub fn color(&self) -> Color {
        *self.color.lock().unwrap()
    }
    pub(in crate::object::gc)fn set_color(&self, new_color: Color) {
        // dbg!(new_color);
        *self.color.lock().unwrap() = new_color;
    }
    pub fn buffered(&self) -> bool {
        *self.buffered.lock().unwrap()
    }
    pub(in crate::object::gc) fn set_buffered(&self, buffered: bool) {
        dbg!(buffered);
        *self.buffered.lock().unwrap() = buffered;
    }
    /// simple RC += 1
    pub(in crate::object::gc)fn inc(&self) -> usize {
        self.ref_cnt.fetch_add(1, Ordering::Relaxed) + 1
    }
    /// simple RC -= 1
    pub(in crate::object::gc)fn dec(&self) -> usize {
        self.ref_cnt.fetch_sub(1, Ordering::Relaxed) - 1
    }
    pub(in crate::object::gc) fn rc(&self) -> usize {
        self.ref_cnt.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Color {
    /// In use or free
    Black,
    /// Possible member of cycle
    Gray,
    /// Member of garbage cycle
    White,
    /// Possible root of cycle
    Purple,
    Green,
    Red,
    Orange,
}
