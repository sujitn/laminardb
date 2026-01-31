# LaminarDB DAG Pipeline with Shared Intermediate Stages

## Feature Specification v1.0

**Target Phase:** Phase 3 (Post-Phase 2 Production Hardening)
**Priority:** High
**Estimated Complexity:** Large
**Prerequisites:** Phase 2 completion (thread-per-core, io_uring, NUMA optimization)

---

## Executive Summary

This specification defines how LaminarDB will support Directed Acyclic Graph (DAG) pipeline topologies with shared intermediate stages, enabling complex streaming workflows while maintaining sub-500ns hot path latency. The design draws from 2025-2026 industry best practices including DBSP's incremental computation model, Flink 2.0's disaggregated state management, and modern barrier-based checkpointing algorithms.

---

## 1. Problem Statement

### Current Limitation
LaminarDB's Phase 1/2 implementation supports linear streaming pipelines:
```
Source → Operator₁ → Operator₂ → ... → Sink
```

### Required Capability
Real-world financial applications require DAG topologies with:
- **Fan-out:** One operator output feeding multiple downstream consumers
- **Fan-in:** Multiple upstream operators merging into one
- **Shared intermediate stages:** Computed results reused without recomputation

### Example Use Case
```
                         ┌─→ VWAP Calculator ──────→ Analytics Sink
                         │
Raw Trades ─→ Dedup ─→ Normalize ─┼─→ Anomaly Detection ──→ Alert Sink
                         │
                         └─→ Position Tracker ────→ Risk Sink
```

Here, the Normalize stage is shared—its output feeds three downstream paths without recomputation.

---

## 2. Design Principles

### 2.1 Core Principles (Aligned with LaminarDB Philosophy)

| Principle | Application |
|-----------|-------------|
| **Zero-allocation hot path** | DAG routing in Ring 0 uses pre-allocated slot buffers |
| **No custom types unless benchmarks demand** | Use standard channel primitives, extend only if latency targets missed |
| **Automatic complexity derivation** | Topology automatically infers optimal channel types (SPSC→MPSC) |
| **Three-ring separation** | DAG execution in Ring 0, topology changes in Ring 2 |

### 2.2 Industry-Informed Patterns (2025-2026 Research)

| Pattern | Source | LaminarDB Adaptation |
|---------|--------|---------------------|
| **Incremental View Maintenance** | DBSP/Feldera | Operators process deltas, not full state recomputation |
| **Disaggregated State** | Flink 2.0 | State stored in memory-mapped regions, checkpointed to remote storage |
| **Asynchronous Barrier Snapshots** | Chandy-Lamport variant | Barriers flow through DAG for consistent checkpoints |
| **Actor-based execution** | RisingWave | Each operator is an isolated actor with message passing |

---

## 3. Architecture

### 3.1 DAG Topology Model

```rust
/// A node in the DAG represents an operator or stage
pub struct DagNode {
    id: NodeId,
    operator: Box<dyn StreamOperator>,
    /// Upstream connections (fan-in)
    inputs: SmallVec<[EdgeId; 4]>,  
    /// Downstream connections (fan-out)
    outputs: SmallVec<[EdgeId; 4]>, 
    /// State partition assignment
    state_partition: StatePartitionId,
}

/// An edge represents a data flow connection
pub struct DagEdge {
    id: EdgeId,
    source: NodeId,
    target: NodeId,
    /// Channel type derived from topology analysis
    channel_type: ChannelType,
    /// Partitioning strategy for parallel execution
    partitioning: PartitioningStrategy,
}

/// The complete DAG topology
pub struct StreamingDag {
    nodes: Vec<DagNode>,
    edges: Vec<DagEdge>,
    /// Topologically sorted execution order
    execution_order: Vec<NodeId>,
    /// Shared intermediate stage registry
    shared_stages: HashMap<NodeId, SharedStageMetadata>,
}
```

### 3.2 Channel Type Derivation

Building on LaminarDB's existing automatic SPSC→MPSC upgrade pattern:

```rust
pub enum ChannelType {
    /// Single producer, single consumer (optimal for linear segments)
    Spsc,
    /// Single producer, multiple consumers (fan-out)
    Spmc,  
    /// Multiple producers, single consumer (fan-in)
    Mpsc,
    /// Multiple producers, multiple consumers (shared stage with fan-in + fan-out)
    Mpmc,
}

impl DagEdge {
    fn derive_channel_type(topology: &StreamingDag) -> ChannelType {
        let fan_in = topology.incoming_edge_count(self.target);
        let fan_out = topology.outgoing_edge_count(self.source);
        
        match (fan_in > 1, fan_out > 1) {
            (false, false) => ChannelType::Spsc,
            (false, true)  => ChannelType::Spmc,
            (true, false)  => ChannelType::Mpsc,
            (true, true)   => ChannelType::Mpmc,
        }
    }
}
```

### 3.3 Shared Intermediate Stages

A shared stage is an operator whose output is consumed by multiple downstream operators. Key requirements:

1. **Zero-copy multicast:** Output written once, read by multiple consumers
2. **Reference counting:** Track when all consumers have processed each batch
3. **Backpressure propagation:** Slowest consumer determines throughput

```rust
pub struct SharedStageMetadata {
    /// Number of downstream consumers
    consumer_count: usize,
    /// Per-batch reference counts for memory management
    batch_refcounts: RingBuffer<AtomicU32>,
    /// Watermark per consumer for backpressure
    consumer_watermarks: Vec<AtomicU64>,
}

/// Zero-copy multicast buffer for shared stages
pub struct MulticastBuffer<T> {
    /// Pre-allocated slots (Ring 0 requirement)
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Write position (single writer)
    write_pos: AtomicU64,
    /// Read positions per consumer
    read_positions: Vec<AtomicU64>,
    /// Reference counts for slot reuse
    refcounts: Box<[AtomicU32]>,
}
```

### 3.4 Three-Ring Integration

| Ring | DAG Responsibilities |
|------|---------------------|
| **Ring 0 (Hot Path)** | Execute operators, route messages through DAG, multicast to shared consumers |
| **Ring 1 (Background)** | Checkpoint state snapshots, garbage collect completed batches |
| **Ring 2 (Control Plane)** | Topology modifications, add/remove operators, rebalancing |

---

## 4. Checkpointing for DAG Topologies

### 4.1 Asynchronous Barrier Snapshot Algorithm

Based on 2025 research showing this approach reduces checkpoint overhead by 40-94% compared to synchronous methods (WJARR 2025, Flink 2.0 VLDB paper).

```rust
/// Checkpoint barrier that flows through the DAG
#[repr(C)]
pub struct CheckpointBarrier {
    /// Unique checkpoint identifier
    checkpoint_id: u64,
    /// Timestamp when checkpoint was initiated
    timestamp: Timestamp,
    /// Barrier type
    barrier_type: BarrierType,
}

pub enum BarrierType {
    /// Standard aligned barrier (wait for all inputs)
    Aligned,
    /// Unaligned barrier (snapshot in-flight data)
    Unaligned,
}

impl StreamOperator for DagNode {
    fn process_barrier(&mut self, barrier: CheckpointBarrier) -> BarrierAction {
        match self.inputs.len() {
            // Source or single input: forward immediately
            0 | 1 => {
                self.snapshot_state(barrier.checkpoint_id);
                BarrierAction::Forward(barrier)
            }
            // Multiple inputs: align barriers
            n => {
                self.barrier_aligner.receive(barrier);
                if self.barrier_aligner.all_aligned() {
                    self.snapshot_state(barrier.checkpoint_id);
                    BarrierAction::Forward(barrier)
                } else {
                    BarrierAction::Buffer
                }
            }
        }
    }
}
```

### 4.2 Incremental State Snapshots

Drawing from Flink's RocksDB incremental checkpointing and DBSP's delta-based computation:

```rust
pub struct IncrementalSnapshot {
    /// Base checkpoint this delta applies to
    base_checkpoint_id: u64,
    /// Delta changes since last checkpoint
    deltas: Vec<StateDelta>,
    /// Operators included in this snapshot
    operator_states: HashMap<NodeId, OperatorSnapshot>,
}

pub enum StateDelta {
    /// Key-value insert or update
    Upsert { key: Bytes, value: Bytes },
    /// Key deletion
    Delete { key: Bytes },
    /// Range deletion (for window expiry)
    DeleteRange { start: Bytes, end: Bytes },
}

/// Checkpoint storage strategy
pub struct CheckpointConfig {
    /// Checkpoint interval (adaptive based on workload)
    interval: Duration,
    /// Use incremental checkpoints
    incremental: bool,
    /// Maximum checkpoint size before forcing full snapshot
    max_incremental_size: usize,
    /// Async write to storage (Ring 1)
    async_persistence: bool,
}
```

### 4.3 DAG-Aware Checkpoint Coordination

For DAG topologies, checkpointing must handle:
- **Fan-out alignment:** Barrier reaches shared stage, then all downstream paths
- **Fan-in alignment:** All upstream barriers must arrive before operator snapshots

```rust
pub struct DagCheckpointCoordinator {
    /// Active checkpoints in progress
    active_checkpoints: HashMap<u64, CheckpointProgress>,
    /// Barrier tracking per node
    barrier_tracker: HashMap<NodeId, BarrierTracker>,
}

pub struct CheckpointProgress {
    checkpoint_id: u64,
    /// Nodes that have completed snapshot
    completed_nodes: HashSet<NodeId>,
    /// Nodes pending barrier alignment
    pending_nodes: HashSet<NodeId>,
    /// Timestamp tracking for timeout
    started_at: Instant,
}

impl DagCheckpointCoordinator {
    /// Initiate checkpoint from sources
    pub fn trigger_checkpoint(&mut self, dag: &StreamingDag) -> CheckpointId {
        let checkpoint_id = self.next_checkpoint_id();
        
        // Inject barriers at all source nodes
        for source in dag.sources() {
            source.inject_barrier(CheckpointBarrier {
                checkpoint_id,
                timestamp: Timestamp::now(),
                barrier_type: BarrierType::Aligned,
            });
        }
        
        checkpoint_id
    }
    
    /// Handle checkpoint completion notification from operator
    pub fn on_operator_checkpoint_complete(
        &mut self,
        checkpoint_id: u64,
        node_id: NodeId,
        snapshot: OperatorSnapshot,
    ) {
        if let Some(progress) = self.active_checkpoints.get_mut(&checkpoint_id) {
            progress.completed_nodes.insert(node_id);
            
            // Check if all nodes completed
            if progress.completed_nodes.len() == self.total_nodes {
                self.finalize_checkpoint(checkpoint_id);
            }
        }
    }
}
```

### 4.4 Recovery Strategy

```rust
pub struct RecoveryManager {
    /// Checkpoint storage backend
    storage: Arc<dyn CheckpointStorage>,
    /// Recovery configuration
    config: RecoveryConfig,
}

pub struct RecoveryConfig {
    /// Recovery mode
    mode: RecoveryMode,
    /// Parallel recovery threads
    parallelism: usize,
    /// Target recovery time (influences checkpoint frequency)
    target_recovery_time: Duration,
}

pub enum RecoveryMode {
    /// Restore latest complete checkpoint
    Latest,
    /// Restore specific checkpoint by ID
    Specific(u64),
    /// Restore with state migration (schema evolution)
    Migrate { from: u64, migration: StateMigration },
}

impl RecoveryManager {
    pub async fn recover(&self, dag: &mut StreamingDag) -> Result<RecoveryMetrics> {
        let checkpoint = self.storage.load_latest_checkpoint().await?;
        
        // Parallel state restoration (using Phase 2's thread-per-core)
        let restore_tasks: Vec<_> = dag.nodes.iter()
            .map(|node| {
                let state = checkpoint.operator_states.get(&node.id);
                async move { node.restore_state(state).await }
            })
            .collect();
        
        futures::future::try_join_all(restore_tasks).await?;
        
        // Reset source offsets to checkpoint positions
        for source in dag.sources() {
            let offset = checkpoint.source_offsets.get(&source.id);
            source.seek_to_offset(offset)?;
        }
        
        Ok(RecoveryMetrics {
            checkpoint_id: checkpoint.id,
            recovery_time: start.elapsed(),
            state_size_restored: checkpoint.total_size(),
        })
    }
}
```

---

## 5. Sub-500ns Latency Considerations

### 5.1 Hot Path Optimizations

| Component | Latency Budget | Optimization Strategy |
|-----------|---------------|----------------------|
| DAG routing decision | <50ns | Pre-computed routing table, branch prediction friendly |
| Multicast to N consumers | <100ns | Zero-copy with reference counting |
| Barrier processing | <50ns | Lock-free barrier alignment |
| State access | <200ns | Memory-mapped state store (existing) |
| **Total hot path** | **<500ns** | |

### 5.2 What Stays OFF the Hot Path

Per LaminarDB's existing architecture, these must remain in Ring 1/2:

- Checkpoint snapshot writes
- State compaction
- Topology modifications
- Log persistence
- Metrics collection

### 5.3 Lock-Free DAG Routing

```rust
/// Pre-computed routing table for zero-allocation routing
pub struct RoutingTable {
    /// For each (source_node, output_port) -> target slots
    routes: Box<[RoutingEntry]>,
}

#[repr(C, align(64))]  // Cache line aligned
pub struct RoutingEntry {
    /// Target node indices
    targets: [u16; 8],  // Max 8 fan-out
    /// Number of valid targets
    target_count: u8,
    /// Partitioning function pointer
    partition_fn: fn(&[u8]) -> usize,
    _padding: [u8; 47],  // Pad to cache line
}

impl RoutingTable {
    /// O(1) routing lookup
    #[inline(always)]
    pub fn route(&self, source: NodeId, port: u8) -> &RoutingEntry {
        let idx = (source.0 as usize) * 8 + (port as usize);
        unsafe { self.routes.get_unchecked(idx) }
    }
}
```

---

## 6. SQL Integration

### 6.1 DAG Definition via SQL

Leverage DataFusion integration to define DAG topologies declaratively:

```sql
-- Define a shared intermediate stage
CREATE MATERIALIZED VIEW normalized_trades AS
SELECT 
    symbol,
    price,
    volume,
    event_time,
    CASE WHEN volume > avg_volume THEN 'HIGH' ELSE 'NORMAL' END as volume_class
FROM raw_trades
JOIN symbol_stats USING (symbol);

-- Multiple consumers of the shared stage
CREATE MATERIALIZED VIEW vwap AS
SELECT 
    symbol,
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
    SUM(price * volume) / SUM(volume) as vwap
FROM normalized_trades  -- References shared stage
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1' MINUTE);

CREATE MATERIALIZED VIEW anomalies AS
SELECT *
FROM normalized_trades  -- Same shared stage
WHERE volume_class = 'HIGH' 
  AND price > (SELECT avg_price * 1.1 FROM symbol_stats WHERE symbol = normalized_trades.symbol);

-- LaminarDB automatically detects shared dependencies and creates DAG:
-- raw_trades ─→ normalized_trades ─┬─→ vwap
--                                  └─→ anomalies
```

### 6.2 DAG Visualization Query

```sql
-- Inspect the generated DAG topology
SELECT * FROM laminar_system.dag_topology;

-- Result:
-- | node_id | operator_type | inputs        | outputs          | is_shared |
-- |---------|---------------|---------------|------------------|-----------|
-- | 1       | Source        | []            | [2]              | false     |
-- | 2       | Project       | [1]           | [3, 4]           | true      |
-- | 3       | Aggregate     | [2]           | [5]              | false     |
-- | 4       | Filter        | [2]           | [6]              | false     |
-- | 5       | Sink          | [3]           | []               | false     |
-- | 6       | Sink          | [4]           | []               | false     |
```

---

## 7. API Design

### 7.1 Rust API

```rust
use laminardb::{Pipeline, Operator, Sink};

// Build DAG programmatically
let pipeline = Pipeline::builder()
    .source("trades", KafkaSource::new(config))
    .operator("dedup", Dedup::by_key(|t| t.trade_id))
    .operator("normalize", Normalize::new(schema))
    // Fan-out: shared stage with multiple consumers
    .fan_out("normalize", |builder| {
        builder
            .branch("vwap", VwapCalculator::new(Duration::minutes(1)))
            .branch("anomaly", AnomalyDetector::new(threshold))
            .branch("position", PositionTracker::new())
    })
    // Each branch has its own sink
    .sink("vwap", PostgresSink::new(analytics_db))
    .sink("anomaly", AlertSink::new(webhook_url))
    .sink("position", RedisSink::new(cache_url))
    .build()?;

// Configure checkpointing
pipeline.checkpoint_config(CheckpointConfig {
    interval: Duration::seconds(30),
    incremental: true,
    storage: S3Storage::new(bucket),
})?;

// Start execution
pipeline.run().await?;
```

### 7.2 Python API (Phase 3 Bindings)

```python
from laminardb import Pipeline, operators as ops

pipeline = (
    Pipeline()
    .source("trades", ops.KafkaSource(topic="raw-trades"))
    .op("dedup", ops.Dedup(key="trade_id"))
    .op("normalize", ops.Normalize(schema=trade_schema))
    # Fan-out with shared intermediate
    .fan_out("normalize", {
        "vwap": ops.TumblingWindow(
            duration="1m",
            agg=ops.Vwap("price", "volume")
        ),
        "anomaly": ops.Filter(lambda t: t.volume > threshold),
        "position": ops.Accumulate(
            key="symbol",
            fn=lambda acc, t: acc + t.volume
        ),
    })
    .sink("vwap", ops.PostgresSink(dsn=analytics_dsn))
    .sink("anomaly", ops.WebhookSink(url=alert_url))
    .sink("position", ops.RedisSink(url=cache_url))
)

# Enable checkpointing
pipeline.with_checkpointing(
    interval="30s",
    storage=ops.S3Storage(bucket="checkpoints")
)

pipeline.run()
```

---

## 8. Checkpointing Storage Options

### 8.1 Storage Backend Interface

```rust
#[async_trait]
pub trait CheckpointStorage: Send + Sync {
    /// Write checkpoint to storage
    async fn write_checkpoint(&self, checkpoint: &Checkpoint) -> Result<()>;
    
    /// Load checkpoint by ID
    async fn load_checkpoint(&self, id: u64) -> Result<Checkpoint>;
    
    /// List available checkpoints
    async fn list_checkpoints(&self) -> Result<Vec<CheckpointMetadata>>;
    
    /// Delete old checkpoints (retention policy)
    async fn cleanup(&self, retain_count: usize) -> Result<()>;
}
```

### 8.2 Built-in Storage Backends

| Backend | Use Case | Latency | Durability |
|---------|----------|---------|------------|
| **LocalFS** | Development, single-node | <1ms | Node-local |
| **RocksDB** | Embedded, fast recovery | <5ms | Node-local |
| **S3/GCS/Azure** | Production, distributed | 50-200ms | Highly durable |
| **Custom** | User-provided implementation | Varies | Varies |

---

## 9. Implementation Roadmap

### Phase 3.1: Core DAG Infrastructure (4 weeks)
- [ ] DAG topology data structures
- [ ] Channel type derivation
- [ ] Basic fan-out/fan-in routing
- [ ] Unit tests for topology validation

### Phase 3.2: Shared Intermediate Stages (3 weeks)
- [ ] Zero-copy multicast buffer
- [ ] Reference counting for batch memory
- [ ] Backpressure propagation
- [ ] Integration tests

### Phase 3.3: Checkpointing (4 weeks)
- [ ] Barrier injection and alignment
- [ ] Incremental state snapshots
- [ ] Checkpoint coordinator
- [ ] Recovery manager
- [ ] S3/local storage backends

### Phase 3.4: SQL Integration (3 weeks)
- [ ] DataFusion plan to DAG conversion
- [ ] Automatic shared stage detection
- [ ] System tables for DAG introspection

### Phase 3.5: Performance Validation (2 weeks)
- [ ] Latency benchmarks (must meet <500ns hot path)
- [ ] Throughput benchmarks
- [ ] Recovery time benchmarks
- [ ] Stress testing with complex DAGs

---

## 10. Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Hot path latency | <500ns p99 | Micro-benchmarks with DAG routing |
| Checkpoint overhead | <5% throughput impact | Throughput comparison with/without |
| Recovery time | <5s for 1GB state | Recovery benchmark |
| Fan-out latency | <100ns additional per consumer | Multicast micro-benchmark |
| Memory overhead | <10% for DAG metadata | Memory profiling |

---

## 11. References

1. **DBSP/Feldera** - Incremental view maintenance theory
   - Budiu et al., "DBSP: Automatic Incremental View Maintenance", VLDB 2023/2025
   
2. **Flink 2.0** - Disaggregated state management
   - Mei et al., "Disaggregated State Management in Apache Flink 2.0", VLDB 2025
   
3. **Checkpointing Research**
   - "Enhancing checkpointing and state recovery for large-scale stream processing", WJARR 2025
   - Ca-Stream adaptive checkpointing, Wiley 2025
   
4. **Latency Optimization**
   - StreamNative, "Latency Numbers Every Data Streaming Engineer Should Know", 2025
   - io_uring zero-copy networking, Linux 6.15

5. **Industry Architectures**
   - kdb+ tick architecture
   - RisingWave materialized view engine
   - Materialize incremental view maintenance

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **DAG** | Directed Acyclic Graph - topology where data flows in one direction without cycles |
| **Fan-out** | One operator output feeding multiple downstream operators |
| **Fan-in** | Multiple upstream operators feeding into one operator |
| **Shared stage** | An operator whose output is consumed by multiple downstream paths |
| **Barrier** | A marker injected into the stream to coordinate checkpoints |
| **Incremental checkpoint** | Saving only state changes since last checkpoint |
| **Aligned checkpoint** | Waiting for barriers from all inputs before snapshotting |
| **Unaligned checkpoint** | Snapshotting immediately, including in-flight data |

---

## Appendix B: Comparison with Alternatives

| Feature | LaminarDB (Proposed) | Flink | RisingWave | kdb+ |
|---------|---------------------|-------|------------|------|
| Embedded deployment | ✅ | ❌ | ❌ | ✅ |
| Sub-microsecond latency | ✅ | ❌ | ❌ | ✅ |
| DAG with shared stages | ✅ | ✅ | ✅ | Limited |
| Standard SQL | ✅ | ✅ | ✅ | ❌ (q lang) |
| Incremental checkpointing | ✅ | ✅ | ✅ | Limited |
| Thread-per-core | ✅ | ❌ | ❌ | Partial |
| Zero-copy multicast | ✅ | ❌ | ❌ | ✅ |
