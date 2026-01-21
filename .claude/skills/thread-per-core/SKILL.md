---
name: thread-per-core
description: Thread-per-core architecture patterns for LaminarDB including CPU pinning, SPSC queues, lock-free structures, and io_uring. Use when implementing the reactor, optimizing for low latency, or working on multi-core scaling.
---

# Thread-Per-Core Skill

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         Thread-Per-Core                         │
├─────────────────────────────────────────────────────────────────┤
│  Core 0              Core 1              Core 2              ...│
│  ┌────────────┐      ┌────────────┐      ┌────────────┐         │
│  │  Reactor   │      │  Reactor   │      │  Reactor   │         │
│  │  ┌──────┐  │      │  ┌──────┐  │      │  ┌──────┐  │         │
│  │  │State │  │      │  │State │  │      │  │State │  │         │
│  │  └──────┘  │      │  └──────┘  │      │  └──────┘  │         │
│  │  ┌──────┐  │      │  ┌──────┐  │      │  ┌──────┐  │         │
│  │  │ WAL  │  │      │  │ WAL  │  │      │  │ WAL  │  │         │
│  │  └──────┘  │      │  └──────┘  │      │  └──────┘  │         │
│  └────────────┘      └────────────┘      └────────────┘         │
│        │                   │                   │                 │
│        └───────── SPSC Queues ─────────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
```

## CPU Pinning

```rust
use core_affinity::CoreId;

pub struct PinnedThread {
    core_id: CoreId,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl PinnedThread {
    pub fn spawn<F>(core_id: CoreId, f: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        let handle = std::thread::spawn(move || {
            // Pin to specific core
            core_affinity::set_for_current(core_id);
            
            // Optionally set real-time priority
            #[cfg(target_os = "linux")]
            {
                let pid = unsafe { libc::gettid() };
                let param = libc::sched_param {
                    sched_priority: 99,
                };
                unsafe {
                    libc::sched_setscheduler(pid, libc::SCHED_FIFO, &param);
                }
            }
            
            f()
        });
        
        Self {
            core_id,
            handle: Some(handle),
        }
    }
}
```

## SPSC Queues

```rust
use crossbeam::channel::{bounded, Receiver, Sender};

// For low-latency, use SPSC (single-producer single-consumer)
pub struct SpscQueue<T> {
    tx: Sender<T>,
    rx: Receiver<T>,
}

impl<T> SpscQueue<T> {
    pub fn new(capacity: usize) -> (SpscSender<T>, SpscReceiver<T>) {
        let (tx, rx) = bounded(capacity);
        (SpscSender { tx }, SpscReceiver { rx })
    }
}

// Even better: use specialized SPSC implementation
use rtrb::RingBuffer;

pub fn create_spsc<T>(capacity: usize) -> (rtrb::Producer<T>, rtrb::Consumer<T>) {
    RingBuffer::new(capacity)
}

// Usage in reactor
impl Reactor {
    pub fn send_to_core(&self, core_id: usize, event: Event) {
        // Direct SPSC send - no locks!
        self.queues[core_id].push(event).expect("queue full");
    }
    
    pub fn poll_events(&mut self) -> impl Iterator<Item = Event> + '_ {
        std::iter::from_fn(|| self.inbox.pop().ok())
    }
}
```

## Lock-Free State Store

```rust
use dashmap::DashMap;
use crossbeam::epoch::{self, Atomic, Owned};

// Option 1: DashMap for simple cases
pub struct ShardedState {
    shards: DashMap<Vec<u8>, Vec<u8>>,
}

// Option 2: Custom lock-free hash map for hot path
pub struct LockFreeMap<K, V> {
    buckets: Box<[Atomic<Node<K, V>>]>,
    size: AtomicUsize,
}

impl<K: Hash + Eq, V> LockFreeMap<K, V> {
    pub fn get(&self, key: &K) -> Option<&V> {
        let guard = epoch::pin();
        let bucket = self.bucket_for(key);
        
        let mut node = self.buckets[bucket].load(Ordering::Acquire, &guard);
        while let Some(n) = unsafe { node.as_ref() } {
            if n.key == *key {
                return Some(&n.value);
            }
            node = n.next.load(Ordering::Acquire, &guard);
        }
        None
    }
}
```

## io_uring Integration

```rust
use io_uring::{opcode, types, IoUring};

pub struct UringReactor {
    ring: IoUring,
    pending: HashMap<u64, PendingOp>,
    next_id: u64,
}

impl UringReactor {
    pub fn new(entries: u32) -> std::io::Result<Self> {
        let ring = IoUring::new(entries)?;
        Ok(Self {
            ring,
            pending: HashMap::new(),
            next_id: 0,
        })
    }
    
    pub fn submit_read(&mut self, fd: RawFd, buf: &mut [u8], offset: u64) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        
        let entry = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
            .offset(offset)
            .build()
            .user_data(id);
        
        unsafe {
            self.ring.submission().push(&entry).expect("submission queue full");
        }
        
        self.pending.insert(id, PendingOp::Read { buf_ptr: buf.as_mut_ptr() });
        id
    }
    
    pub fn poll(&mut self) -> Vec<CompletedOp> {
        self.ring.submit_and_wait(1).unwrap();
        
        let mut completed = Vec::new();
        for cqe in self.ring.completion() {
            let id = cqe.user_data();
            let result = cqe.result();
            
            if let Some(op) = self.pending.remove(&id) {
                completed.push(CompletedOp { id, result, op });
            }
        }
        completed
    }
}
```

## Memory Ordering

```rust
use std::sync::atomic::{AtomicU64, Ordering};

// Correct memory ordering for different scenarios:

// 1. Simple counter (relaxed is fine)
static COUNTER: AtomicU64 = AtomicU64::new(0);
fn increment() {
    COUNTER.fetch_add(1, Ordering::Relaxed);
}

// 2. Flag for signaling (release/acquire)
static READY: AtomicBool = AtomicBool::new(false);
static mut DATA: u64 = 0;

fn producer() {
    unsafe { DATA = 42; }  // Write data
    READY.store(true, Ordering::Release);  // Release ensures DATA is visible
}

fn consumer() {
    while !READY.load(Ordering::Acquire) {}  // Acquire syncs with release
    let data = unsafe { DATA };  // Safe to read now
}

// 3. Compare-and-swap (SeqCst for simplicity, or careful Acquire/Release)
fn cas_update(atomic: &AtomicU64, expected: u64, new: u64) -> bool {
    atomic.compare_exchange(
        expected,
        new,
        Ordering::AcqRel,  // Success: both acquire and release
        Ordering::Acquire, // Failure: just acquire to read current value
    ).is_ok()
}
```

## Event Loop Pattern

```rust
pub struct EventLoop {
    // Per-core state - no sharing
    state: StateStore,
    operators: Vec<Box<dyn Operator>>,
    
    // Communication channels
    inbox: SpscReceiver<Event>,
    outbox: SpscSender<Output>,
    
    // I/O
    io_ring: UringReactor,
}

impl EventLoop {
    pub fn run(&mut self) {
        loop {
            // 1. Process incoming events (non-blocking)
            while let Some(event) = self.inbox.try_recv() {
                self.process_event(event);
            }
            
            // 2. Check timers
            self.fire_expired_timers();
            
            // 3. Poll I/O completions
            for completed in self.io_ring.poll() {
                self.handle_io_completion(completed);
            }
            
            // 4. Yield if no work (optional busy-wait for lowest latency)
            if self.inbox.is_empty() && !self.io_ring.has_pending() {
                std::thread::yield_now();
            }
        }
    }
}
```

## Partitioning Strategy

```rust
// Partition events by key to ensure ordering
pub fn partition_key(key: &[u8], num_cores: usize) -> usize {
    let hash = fxhash::hash64(key);
    (hash as usize) % num_cores
}

// Route events to correct core
impl Router {
    pub fn route(&self, event: Event) {
        let core = partition_key(&event.key, self.num_cores);
        self.queues[core].push(event);
    }
}
```
