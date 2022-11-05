use std::time::{Duration, Instant};

use rustpython_common::lock::PyMutex;
use sysinfo::{ProcessExt, ProcessRefreshKind, RefreshKind, System, SystemExt};

pub static SYS: once_cell::sync::Lazy<PyMutex<sysinfo::System>> =
    once_cell::sync::Lazy::new(|| {
        PyMutex::new(sysinfo::System::new_with_specifics(
            RefreshKind::new()
                .with_memory()
                .with_processes(ProcessRefreshKind::everything()),
        ))
    });

/// if is in unsupported system or some thing goes wrong, return None
fn get_mem_usage() -> Option<u64> {
    if System::IS_SUPPORTED {
        let mut sys = SYS.lock();
        let pid = sysinfo::get_current_pid().ok()?;
        sys.refresh_process(pid);
        let proc = sys.process(pid)?;
        Some(proc.memory())
    } else {
        None
    }
}

#[derive(Debug)]
pub struct MemBalancer {
    gced_bytes: u64,
    gc_time: Duration,
    live: u64,
    mem_delta: u64,
    beat_period: Duration,
    // support vars
    /// Last time a heartbeat happen
    mem_before_gc: u64,
    gc_start_time: Instant,
    last_mem: u64,
    last_beat: Instant,
    heap_limit: u64,
}

impl MemBalancer {
    const TUNING_PARAM: u64 = 2;
    const NURSERY: u64 = 10 * 1024 * 1024;
    const GC_FACTOR: f32 = 0.5;
    const ALLOC_FACTOR: f32 = 0.95;
    pub fn new() -> Self {
        Self {
            gced_bytes: 0,
            gc_time: Default::default(),
            live: 0,
            mem_delta: 0,
            beat_period: Default::default(),
            mem_before_gc: 0,
            gc_start_time: Instant::now(),
            last_mem: 0,
            last_beat: Instant::now(),
            heap_limit: 0,
        }
    }

    pub const fn is_support(&self) -> bool {
        System::IS_SUPPORTED
    }
    pub fn excess_heap_limit(&self) -> bool {
        if let Some(mem) = get_mem_usage() {
            mem > self.heap_limit
        } else {
            false
        }
    }

    pub fn mark_start_gc(&mut self) {
        self.mem_before_gc = get_mem_usage().unwrap_or(0);
        self.gc_start_time = Instant::now();
        error!("Mem when start gc={}MB", self.mem_before_gc / 1024 / 1024);
    }

    pub fn mark_end_gc(&mut self, gced_bytes: u64) {
        let cur = get_mem_usage().unwrap_or(0);
        error!("Gc end, mem = {}MB", cur / 1024 / 1024);
        // to prevent extremely large E
        let gced_bytes = gced_bytes.max(1024);
        self.on_gc(gced_bytes, self.gc_start_time.elapsed(), cur);
    }

    /// update mem alloc speed
    pub fn update_mem_delta(&mut self) {
        let new = get_mem_usage();
        if let Some(new) = new {
            let delta = if new > self.last_mem {
                new - self.last_mem
            } else {
                0
            };
            self.last_mem = new;
            self.on_heartbeat(delta, self.last_beat.elapsed());
        }
    }

    fn on_gc(&mut self, gced_bytes: u64, gc_time: Duration, live: u64) {
        self.gced_bytes =
            (self.gced_bytes as f32 * Self::GC_FACTOR + gced_bytes as f32 * Self::GC_FACTOR) as u64;
        self.gc_time =
            self.gc_time.mul_f32(Self::GC_FACTOR) + gc_time.mul_f32(1.0 - Self::GC_FACTOR);
        self.live = live;
    }
    fn on_heartbeat(&mut self, mem_delta: u64, hb_period: Duration) {
        self.mem_delta = (Self::ALLOC_FACTOR * self.mem_delta as f32
            + (1.0 - Self::ALLOC_FACTOR) * mem_delta as f32) as u64;
        self.beat_period = self.beat_period.mul_f32(Self::ALLOC_FACTOR)
            + hb_period.mul_f32(1.0 - Self::ALLOC_FACTOR);
    }

    pub fn compute_heap_limit(&mut self) {
        let e_min = 2 * 1024 * 1024;
        let l_c = (self.live / Self::TUNING_PARAM) as f32;
        let g = self.mem_delta as f32 / self.beat_period.as_secs_f32();
        let s = self.gced_bytes as f32 / self.gc_time.as_secs_f32();
        let e = (l_c * g / s).sqrt() as u64;
        dbg!(self.live / 1024 / 1024);
        dbg!(e / 1024 / 1024);
        self.heap_limit = self.live + e.max(e_min) + Self::NURSERY;
        error!(
            "New heap limit = {}MB, cur mem = {:?}MB, E = {e}",
            self.heap_limit / 1024 / 1024,
            get_mem_usage().unwrap() / 1024 / 1024
        );
    }
}
