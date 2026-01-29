# F-DAG-001: DAG Pipeline with Shared Intermediate Stages

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DAG-001 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | XL (4-6 weeks) |
| **Dependencies** | F-STREAM-001 to F-STREAM-007, F060, F063, F022, F034 |
| **Blocks** | F-DAG-002 (Dynamic Topology), F-DAG-003 (DAG Visualization) |
| **Owner** | TBD |
| **Research** | [DAG Pipeline Spec (Draft)](../../research/laminardb-dag-pipeline-spec.md), [Research Compendium Section 6](../../research/laminardb-dag-pipeline-spec.md) |
| **Crate** | `laminar-core` (primary), `laminar-sql` (SQL integration) |

---

## Summary

Enable Directed Acyclic Graph (DAG) pipeline topologies with shared intermediate stages in LaminarDB. This feature allows complex streaming workflows where operator outputs fan out to multiple downstream consumers, multiple upstream operators merge into one (fan-in), and computed intermediate results are reused without recomputation. The DAG executes in Ring 0 with sub-500ns hot path routing, while topology management resides in Ring 2 and checkpoint coordination in Ring 1.

---

## Motivation

### Current Limitation

LaminarDB's Phase 2 implementation supports two topological patterns:

1. **Linear pipelines** via the streaming API (`Source<T> -> operators -> Sink<T>`):
   ```
   Source -> Operator_1 -> Operator_2 -> ... -> Sink
   ```

2. **Cascading materialized views** via `MvRegistry` and `MvPipelineExecutor` (F060), which process events through a dependency DAG of MVs in topological order, but without explicit DAG-level infrastructure for:
   - Shared intermediate stages with zero-copy multicast
   - Pre-computed routing tables for O(1) fan-out
   - Barrier-based checkpoint coordination across DAG edges
   - Programmatic DAG construction beyond SQL `CREATE MATERIALIZED VIEW`

### Required Capability

Real-world financial applications require DAG topologies with:

- **Fan-out**: One operator output feeding multiple downstream consumers
- **Fan-in**: Multiple upstream operators merging into one
- **Shared intermediate stages**: Computed results reused without recomputation
- **Deterministic checkpointing**: Barriers propagated through the full DAG

### Example Use Case

```
                         +-> VWAP Calculator --------> Analytics Sink
                         |
Raw Trades -> Dedup -> Normalize -+-> Anomaly Detection -> Alert Sink
                         |
                         +-> Position Tracker ------> Risk Sink
```

Here, the Normalize stage is **shared** -- its output feeds three downstream paths without recomputation. Each downstream branch has independent windowing, state, and sink semantics.

### Industry Context (2025-2026 Research)

| System | DAG Support | Shared Stages | Checkpoint Model |
|--------|-------------|---------------|------------------|
| **Apache Flink** | Full DAG | Implicit (plan optimization) | Chandy-Lamport barriers |
| **RisingWave** | MV DAG | Explicit (MV sharing) | Barrier-based |
| **DBSP/Feldera** | Incremental DAG | Implicit (Z-set operators) | Epoch-based |
| **Materialize** | Dataflow DAG | Arrangement sharing | Frontier-based |
| **LaminarDB (current)** | Linear + MV cascade | MvRegistry only | Epoch + WAL |
| **LaminarDB (target)** | Full DAG | Explicit zero-copy multicast | Barrier + epoch hybrid |

---

## Goals

1. **DAG topology model** with nodes (operators) and edges (channels), stored as an adjacency list with topological ordering
2. **Automatic channel type derivation** -- SPSC for linear segments, SPMC for fan-out, MPSC for fan-in, consistent with the existing auto-upgrade pattern from `streaming::channel`
3. **Zero-copy multicast** for shared intermediate stages using reference-counted slot buffers
4. **Pre-computed routing table** in Ring 0 for O(1) fan-out decisions with no branching on the hot path
5. **Barrier-based checkpointing** that flows through DAG edges, coordinated with the existing `IncrementalCheckpointManager` (F022)
6. **Changelog/retraction propagation** through DAG edges using `ChangelogRecord<T>` and `ChangelogRef` from F063
7. **SQL integration** where `CREATE MATERIALIZED VIEW` statements that reference other MVs automatically form DAGs via the existing `MvRegistry`
8. **Programmatic Rust API** (Pipeline builder) for constructing DAGs without SQL
9. **Connector SDK integration** (F034) for DAG source/sink nodes bridging external systems

## Non-Goals

- **Dynamic topology changes** while the DAG is running (topology is immutable once started; changes require Ring 2 restart) -- deferred to F-DAG-002
- **Distributed DAG execution** across multiple nodes -- single-process only
- **Cyclic graphs** -- strictly acyclic; cycles are rejected at registration time (existing `MvRegistry::would_create_cycle`)
- **Automatic operator fusion/optimization** (e.g., merging adjacent map operators) -- Phase 4
- **Python/Java bindings** for the Pipeline API -- Phase 4

---

## Technical Design

### 1. DAG Topology Model

The DAG is represented as an adjacency list of `DagNode` and `DagEdge` structures with a pre-computed topological execution order. The design extends the existing `MvRegistry` dependency graph (`crates/laminar-core/src/mv/registry.rs`) to support general operators, not just materialized views.

```rust
use std::sync::Arc;

use arrow_schema::SchemaRef;
use fxhash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

/// Unique identifier for a node in the DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

/// Unique identifier for an edge in the DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeId(pub u32);

/// Unique identifier for a state partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatePartitionId(pub u32);

/// A node in the DAG represents an operator or stage.
///
/// Nodes are created during topology construction in Ring 2
/// and are immutable once the DAG is started.
pub struct DagNode {
    /// Unique node identifier.
    pub id: NodeId,
    /// Human-readable name (e.g., "normalize", "vwap_calculator").
    pub name: String,
    /// The operator that processes events at this node.
    pub operator: Box<dyn Operator>,
    /// Upstream connections (fan-in). SmallVec avoids heap alloc for <= 4 inputs.
    pub inputs: SmallVec<[EdgeId; 4]>,
    /// Downstream connections (fan-out). SmallVec avoids heap alloc for <= 4 outputs.
    pub outputs: SmallVec<[EdgeId; 4]>,
    /// Output schema for downstream type checking.
    pub output_schema: SchemaRef,
    /// State partition assignment (for thread-per-core routing).
    pub state_partition: StatePartitionId,
    /// Node classification for ring assignment.
    pub node_type: DagNodeType,
}

/// Classification of a DAG node for ring assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DagNodeType {
    /// External data source (Connector SDK source, Ring 1).
    Source,
    /// Stateless operator (map, filter, project -- Ring 0).
    StatelessOperator,
    /// Stateful operator (window, join, aggregate -- Ring 0).
    StatefulOperator,
    /// Materialized view (wraps MvPipelineExecutor node -- Ring 0).
    MaterializedView,
    /// External data sink (Connector SDK sink, Ring 1).
    Sink,
}

/// An edge represents a data flow connection between two nodes.
///
/// The channel type is automatically derived from the topology,
/// consistent with the auto-upgrade pattern in
/// `laminar-core/src/streaming/channel.rs`.
pub struct DagEdge {
    /// Unique edge identifier.
    pub id: EdgeId,
    /// Source node.
    pub source: NodeId,
    /// Target node.
    pub target: NodeId,
    /// Channel type (derived from topology analysis, never user-specified).
    pub channel_type: DagChannelType,
    /// Partitioning strategy for parallel execution.
    pub partitioning: PartitioningStrategy,
    /// Output port on the source node (for multi-output operators).
    pub source_port: u8,
    /// Input port on the target node (for multi-input operators).
    pub target_port: u8,
}

/// Channel type derived from DAG topology analysis.
///
/// Extends the existing `ChannelMode` from `streaming::channel`
/// (which only supports SPSC and MPSC) with SPMC for fan-out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DagChannelType {
    /// Single producer, single consumer (optimal for linear segments).
    /// Uses existing `streaming::channel` in SPSC mode.
    Spsc,
    /// Single producer, multiple consumers (fan-out / shared stage).
    /// Uses the new `MulticastBuffer` for zero-copy fan-out.
    Spmc,
    /// Multiple producers, single consumer (fan-in).
    /// Uses existing `streaming::channel` with MPSC auto-upgrade.
    Mpsc,
}

/// Partitioning strategy for parallel operator instances.
#[derive(Debug, Clone)]
pub enum PartitioningStrategy {
    /// All events go to a single partition (no parallelism).
    Single,
    /// Round-robin distribution across partitions.
    RoundRobin,
    /// Hash-based partitioning by key expression.
    HashBy(String),
    /// Custom partitioning function.
    Custom(Arc<dyn Fn(&[u8]) -> usize + Send + Sync>),
}
```

### 2. Channel Type Derivation

Building on LaminarDB's existing automatic SPSC-to-MPSC upgrade pattern from `streaming::channel`, the DAG derives channel types from the topology structure. This is computed once at topology construction time (Ring 2) and encoded into the pre-computed routing table.

```rust
impl DagEdge {
    /// Derives the channel type from the DAG topology.
    ///
    /// The channel type is NEVER user-specified. It is computed from
    /// the fan-in and fan-out of the connected nodes.
    ///
    /// - SPSC: Single input, single output (linear segment)
    /// - SPMC: Single input, multiple outputs (fan-out / shared stage)
    /// - MPSC: Multiple inputs, single output (fan-in / merge)
    fn derive_channel_type(dag: &StreamingDag, edge: &DagEdge) -> DagChannelType {
        let source_fan_out = dag.outgoing_edge_count(edge.source);
        let target_fan_in = dag.incoming_edge_count(edge.target);

        match (target_fan_in > 1, source_fan_out > 1) {
            (false, false) => DagChannelType::Spsc,
            (false, true)  => DagChannelType::Spmc,
            (true, false)  => DagChannelType::Mpsc,
            // For MPSC + fan-out: the source side uses SPMC multicast,
            // the target side merges via MPSC. Each edge is one direction.
            (true, true)   => DagChannelType::Mpsc,
        }
    }
}
```

### 3. The Complete DAG Topology

```rust
/// The complete DAG topology.
///
/// Constructed in Ring 2 via `DagBuilder` or from SQL `CREATE MATERIALIZED VIEW`
/// chains. Once built, the topology is immutable and can be executed in Ring 0.
pub struct StreamingDag {
    /// All nodes in the DAG.
    nodes: FxHashMap<NodeId, DagNode>,
    /// All edges in the DAG.
    edges: FxHashMap<EdgeId, DagEdge>,
    /// Topologically sorted execution order (dependencies first).
    /// Computed via Kahn's algorithm, same as `MvRegistry::update_topo_order()`.
    execution_order: Vec<NodeId>,
    /// Shared intermediate stage metadata (nodes with fan-out > 1).
    shared_stages: FxHashMap<NodeId, SharedStageMetadata>,
    /// Pre-computed routing table for Ring 0 execution.
    routing_table: RoutingTable,
    /// Source nodes (entry points).
    source_nodes: Vec<NodeId>,
    /// Sink nodes (exit points).
    sink_nodes: Vec<NodeId>,
    /// Next node/edge ID counters.
    next_node_id: u32,
    next_edge_id: u32,
}

impl StreamingDag {
    /// Returns the number of outgoing edges from a node.
    #[inline]
    #[must_use]
    pub fn outgoing_edge_count(&self, node: NodeId) -> usize {
        self.nodes.get(&node).map_or(0, |n| n.outputs.len())
    }

    /// Returns the number of incoming edges to a node.
    #[inline]
    #[must_use]
    pub fn incoming_edge_count(&self, node: NodeId) -> usize {
        self.nodes.get(&node).map_or(0, |n| n.inputs.len())
    }

    /// Returns all source nodes (nodes with no inputs).
    #[must_use]
    pub fn sources(&self) -> &[NodeId] {
        &self.source_nodes
    }

    /// Returns all sink nodes (nodes with no outputs).
    #[must_use]
    pub fn sinks(&self) -> &[NodeId] {
        &self.sink_nodes
    }

    /// Returns nodes in topological execution order (dependencies first).
    #[must_use]
    pub fn execution_order(&self) -> &[NodeId] {
        &self.execution_order
    }

    /// Returns metadata for shared stages (nodes with fan-out > 1).
    #[must_use]
    pub fn shared_stages(&self) -> &FxHashMap<NodeId, SharedStageMetadata> {
        &self.shared_stages
    }

    /// Validates the DAG topology.
    ///
    /// # Errors
    ///
    /// Returns `DagError::CycleDetected` if the graph contains cycles.
    /// Returns `DagError::DisconnectedNode` if any node has no inputs and no outputs.
    /// Returns `DagError::SchemaMismatch` if connected edges have incompatible schemas.
    pub fn validate(&self) -> Result<(), DagError> {
        self.check_acyclic()?;
        self.check_connected()?;
        self.check_schemas()?;
        Ok(())
    }
}
```

### 4. Shared Intermediate Stages (Zero-Copy Multicast)

A shared stage is an operator whose output is consumed by multiple downstream operators. The key design is zero-copy multicast: output is written once and read by multiple consumers via reference counting.

```rust
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Metadata for a shared intermediate stage.
///
/// Created during topology construction for any node with fan-out > 1.
pub struct SharedStageMetadata {
    /// Node that produces the shared output.
    pub producer_node: NodeId,
    /// Number of downstream consumers.
    pub consumer_count: usize,
    /// Consumer node IDs (for backpressure tracking).
    pub consumer_nodes: Vec<NodeId>,
    /// Per-consumer processing watermarks (for backpressure).
    pub consumer_watermarks: Vec<AtomicU64>,
}

/// Zero-copy multicast buffer for shared stages.
///
/// Uses pre-allocated slot buffers (Ring 0 requirement) with
/// per-slot reference counts. When the producer writes a slot,
/// the refcount is set to `consumer_count`. Each consumer
/// decrements the refcount after reading. When refcount reaches 0,
/// the slot is available for reuse.
///
/// # Safety
///
/// This structure uses `UnsafeCell` for zero-copy access.
/// The safety invariant is maintained by the write_pos/read_positions
/// protocol: a slot is only written when refcount == 0, and only
/// read when refcount > 0.
pub struct MulticastBuffer<T> {
    /// Pre-allocated slots (Ring 0 requirement: no allocation on push).
    // SAFETY: Accessed via atomic coordination between write_pos and
    // read_positions. A slot is only written when its refcount == 0
    // (all consumers have read it or it is fresh). A slot is only
    // read when its refcount > 0 (producer has written it).
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Capacity (power of 2 for bitmask indexing, same as RingBuffer).
    capacity: usize,
    /// Bitmask for index wrapping (capacity - 1).
    mask: usize,
    /// Write position (single writer, monotonically increasing).
    write_pos: AtomicU64,
    /// Read positions per consumer (one per downstream node).
    read_positions: Vec<AtomicU64>,
    /// Reference counts per slot (initialized to consumer_count on write).
    refcounts: Box<[AtomicU32]>,
    /// Number of consumers.
    consumer_count: u32,
}

// SAFETY: MulticastBuffer uses atomic operations for all shared state.
// The UnsafeCell slots are protected by the refcount protocol.
unsafe impl<T: Send> Send for MulticastBuffer<T> {}
unsafe impl<T: Send> Sync for MulticastBuffer<T> {}

impl<T: Clone> MulticastBuffer<T> {
    /// Creates a new multicast buffer.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Buffer size (rounded up to power of 2).
    /// * `consumer_count` - Number of downstream consumers.
    pub fn new(capacity: usize, consumer_count: usize) -> Self {
        let capacity = capacity.next_power_of_two();
        let mask = capacity - 1;

        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(UnsafeCell::new(MaybeUninit::uninit()));
        }

        let mut refcounts = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            refcounts.push(AtomicU32::new(0));
        }

        let read_positions = (0..consumer_count)
            .map(|_| AtomicU64::new(0))
            .collect();

        Self {
            slots: slots.into_boxed_slice(),
            capacity,
            mask,
            write_pos: AtomicU64::new(0),
            read_positions,
            refcounts: refcounts.into_boxed_slice(),
            consumer_count: consumer_count as u32,
        }
    }

    /// Publishes a value to all consumers. Ring 0, O(1).
    ///
    /// Returns `Err` if the buffer is full (slowest consumer has not
    /// consumed the slot yet -- backpressure signal).
    #[inline]
    pub fn publish(&self, value: T) -> Result<(), DagError> {
        let pos = self.write_pos.load(Ordering::Relaxed);
        let idx = (pos as usize) & self.mask;

        // Check that the slot is free (all consumers have read it)
        if self.refcounts[idx].load(Ordering::Acquire) != 0 {
            return Err(DagError::BackpressureFull);
        }

        // SAFETY: Slot is free (refcount == 0), and we are the sole writer.
        unsafe {
            (*self.slots[idx].get()).write(value);
        }

        // Set refcount to consumer_count so all consumers can read
        self.refcounts[idx].store(self.consumer_count, Ordering::Release);
        self.write_pos.store(pos + 1, Ordering::Release);

        Ok(())
    }

    /// Reads a value for a specific consumer. Ring 0, O(1).
    ///
    /// Returns `None` if no new data is available for this consumer.
    #[inline]
    pub fn consume(&self, consumer_idx: usize) -> Option<&T> {
        let read_pos = self.read_positions[consumer_idx].load(Ordering::Relaxed);
        let write_pos = self.write_pos.load(Ordering::Acquire);

        if read_pos >= write_pos {
            return None; // No new data
        }

        let idx = (read_pos as usize) & self.mask;

        // SAFETY: write_pos > read_pos guarantees the slot has been written.
        // The refcount > 0 guarantees the data is still valid.
        let value = unsafe { (*self.slots[idx].get()).assume_init_ref() };

        // Advance read position
        self.read_positions[consumer_idx].store(read_pos + 1, Ordering::Relaxed);

        // Decrement refcount; if we are the last consumer, the slot is freed
        self.refcounts[idx].fetch_sub(1, Ordering::Release);

        Some(value)
    }
}
```

### 5. Pre-Computed Routing Table (Ring 0 Hot Path)

The routing table is computed once at DAG construction time (Ring 2) and used at runtime in Ring 0 for O(1) routing decisions with no dynamic dispatch or branching.

```rust
use crate::tpc::CachePadded;

/// Pre-computed routing table for zero-allocation routing in Ring 0.
///
/// Each entry is cache-line aligned (64 bytes) to prevent false sharing
/// when accessed by the reactor loop.
pub struct RoutingTable {
    /// For each (source_node, output_port) -> routing entry.
    /// Indexed as: routes[node_id * MAX_PORTS + port].
    routes: Box<[RoutingEntry]>,
    /// Maximum ports per node (compile-time constant).
    max_ports: usize,
}

/// A single routing entry, cache-line aligned.
///
/// Contains the target node indices and the fan-out count.
/// Max 8 fan-out targets per output port.
#[repr(C, align(64))]
pub struct RoutingEntry {
    /// Target node indices (NodeId values).
    pub targets: [u16; 8],
    /// Number of valid targets (0 = no routing, terminal node).
    pub target_count: u8,
    /// Whether this is a shared stage (multicast).
    pub is_multicast: bool,
    /// Padding to fill cache line (64 bytes total).
    _padding: [u8; 45],
}

impl RoutingTable {
    /// Builds the routing table from a DAG topology.
    ///
    /// Called once during DAG construction in Ring 2.
    pub fn build(dag: &StreamingDag) -> Self {
        let max_ports = 8usize;
        let max_nodes = dag.nodes.len().next_power_of_two().max(16);
        let total_entries = max_nodes * max_ports;

        let mut routes = vec![RoutingEntry::empty(); total_entries];

        for (_, edge) in &dag.edges {
            let idx = (edge.source.0 as usize) * max_ports + (edge.source_port as usize);
            if idx < total_entries {
                let entry = &mut routes[idx];
                if (entry.target_count as usize) < 8 {
                    entry.targets[entry.target_count as usize] = edge.target.0 as u16;
                    entry.target_count += 1;
                }
            }
        }

        // Mark multicast entries (fan-out > 1)
        for entry in routes.iter_mut() {
            entry.is_multicast = entry.target_count > 1;
        }

        Self {
            routes: routes.into_boxed_slice(),
            max_ports,
        }
    }

    /// O(1) routing lookup. Ring 0 hot path.
    ///
    /// Returns the routing entry for the given source node and output port.
    #[inline(always)]
    pub fn route(&self, source: NodeId, port: u8) -> &RoutingEntry {
        let idx = (source.0 as usize) * self.max_ports + (port as usize);
        // SAFETY: Index is bounded by construction (max_nodes * max_ports).
        // The routing table is immutable after construction.
        debug_assert!(idx < self.routes.len());
        unsafe { self.routes.get_unchecked(idx) }
    }
}

impl RoutingEntry {
    fn empty() -> Self {
        Self {
            targets: [0; 8],
            target_count: 0,
            is_multicast: false,
            _padding: [0; 45],
        }
    }
}
```

### 6. DAG Executor (Ring 0)

The executor runs the DAG in topological order within the reactor loop. It uses the pre-computed routing table for O(1) dispatch and the multicast buffer for shared stages.

```rust
use crate::operator::{Event, Operator, OperatorContext, Output};
use crate::operator::changelog::{ChangelogBuffer, ChangelogRef};
use crate::alloc::HotPathGuard;

/// Executes a streaming DAG in Ring 0.
///
/// The executor processes events through the DAG in topological order,
/// using the pre-computed routing table for O(1) dispatch.
///
/// # Ring Architecture
///
/// - **Ring 0**: `DagExecutor::process_event()` -- routing, operator calls
/// - **Ring 1**: Checkpoint barriers, WAL drain, sink I/O
/// - **Ring 2**: Topology construction (`DagBuilder`), lifecycle management
pub struct DagExecutor {
    /// The immutable DAG topology.
    dag: Arc<StreamingDag>,
    /// Operators indexed by NodeId (dense array for cache locality).
    operators: Vec<Option<Box<dyn Operator>>>,
    /// Pre-computed routing table.
    routing_table: Arc<RoutingTable>,
    /// Multicast buffers for shared stages.
    multicast_buffers: FxHashMap<NodeId, MulticastBuffer<Event>>,
    /// Per-node input queues for fan-in merging.
    /// Uses pre-allocated VecDeques to avoid allocation on the hot path.
    input_queues: Vec<VecDeque<Event>>,
    /// Changelog buffer for delta propagation (F063 integration).
    changelog: ChangelogBuffer,
    /// Checkpoint barrier aligner (for multi-input nodes).
    barrier_aligner: BarrierAligner,
    /// Execution metrics.
    metrics: DagExecutorMetrics,
}

/// Metrics for DAG execution.
#[derive(Debug, Clone, Default)]
pub struct DagExecutorMetrics {
    /// Total events processed across all nodes.
    pub events_processed: u64,
    /// Events processed per node.
    pub events_per_node: FxHashMap<NodeId, u64>,
    /// Barriers processed.
    pub barriers_processed: u64,
    /// Multicast publishes.
    pub multicast_publishes: u64,
    /// Backpressure stalls.
    pub backpressure_stalls: u64,
}

impl DagExecutor {
    /// Creates a new DAG executor.
    ///
    /// # Arguments
    ///
    /// * `dag` - The immutable DAG topology (constructed in Ring 2).
    /// * `changelog_capacity` - Capacity for the changelog buffer (F063).
    pub fn new(dag: Arc<StreamingDag>, changelog_capacity: usize) -> Self {
        let routing_table = Arc::new(RoutingTable::build(&dag));
        let max_nodes = dag.nodes.len();

        // Pre-allocate input queues for each node
        let input_queues = (0..max_nodes)
            .map(|_| VecDeque::with_capacity(64))
            .collect();

        // Create multicast buffers for shared stages
        let multicast_buffers = dag
            .shared_stages()
            .iter()
            .map(|(&node_id, meta)| {
                let buffer = MulticastBuffer::new(4096, meta.consumer_count);
                (node_id, buffer)
            })
            .collect();

        Self {
            dag,
            operators: Vec::new(), // Populated via register_operator()
            routing_table,
            multicast_buffers,
            input_queues,
            changelog: ChangelogBuffer::with_capacity(changelog_capacity),
            barrier_aligner: BarrierAligner::new(),
            metrics: DagExecutorMetrics::default(),
        }
    }

    /// Processes a source event through the DAG.
    ///
    /// Events are routed through the DAG in topological order using
    /// the pre-computed routing table. Shared stages use zero-copy
    /// multicast. Changelog deltas are tracked for F063 retraction.
    ///
    /// # Performance
    ///
    /// - Routing decision: <50ns (array index lookup)
    /// - Multicast per consumer: <100ns (refcount increment)
    /// - Total hot path budget: <500ns (Ring 0 constraint)
    ///
    /// # Errors
    ///
    /// Returns `DagError::BackpressureFull` if a shared stage buffer is full.
    /// Returns `DagError::OperatorError` if an operator fails during processing.
    pub fn process_event(
        &mut self,
        source_node: NodeId,
        event: Event,
        ctx: &mut OperatorContext,
    ) -> Result<(), DagError> {
        // Zero-allocation guard (F071 integration)
        #[cfg(debug_assertions)]
        let _guard = HotPathGuard::new();

        // Enqueue event at source node
        self.input_queues[source_node.0 as usize].push_back(event);

        // Process nodes in topological order
        for &node_id in self.dag.execution_order() {
            self.process_node(node_id, ctx)?;
        }

        Ok(())
    }

    /// Processes all pending events at a single node.
    #[inline]
    fn process_node(
        &mut self,
        node_id: NodeId,
        ctx: &mut OperatorContext,
    ) -> Result<(), DagError> {
        let queue = &mut self.input_queues[node_id.0 as usize];
        if queue.is_empty() {
            return Ok(());
        }

        // Process each input event through the operator
        while let Some(event) = queue.pop_front() {
            if let Some(Some(operator)) = self.operators.get_mut(node_id.0 as usize) {
                let outputs = operator.process(&event, ctx);

                // Route outputs to downstream nodes
                for output in outputs {
                    if let Output::Event(out_event) = output {
                        self.route_output(node_id, out_event)?;
                    }
                }

                self.metrics.events_processed += 1;
            }
        }

        Ok(())
    }

    /// Routes an output event from a node to its downstream targets.
    #[inline]
    fn route_output(
        &mut self,
        source: NodeId,
        event: Event,
    ) -> Result<(), DagError> {
        let entry = self.routing_table.route(source, 0);

        if entry.target_count == 0 {
            // Terminal node (sink) -- no further routing
            return Ok(());
        }

        if entry.is_multicast {
            // Shared stage: use zero-copy multicast buffer
            if let Some(buffer) = self.multicast_buffers.get(&source) {
                buffer.publish(event)?;
                self.metrics.multicast_publishes += 1;

                // Consumers read from multicast buffer and enqueue
                for i in 0..entry.target_count as usize {
                    if let Some(consumed) = buffer.consume(i) {
                        let target = NodeId(entry.targets[i] as u32);
                        self.input_queues[target.0 as usize]
                            .push_back(consumed.clone());
                    }
                }
            }
        } else {
            // Single target: direct enqueue (no multicast overhead)
            let target = NodeId(entry.targets[0] as u32);
            self.input_queues[target.0 as usize].push_back(event);
        }

        Ok(())
    }
}
```

### 7. Three-Ring Integration

| Ring | DAG Responsibilities | Implementation Module |
|------|---------------------|----------------------|
| **Ring 0 (Hot Path)** | `DagExecutor::process_event()`: route events through DAG, call operators, multicast to shared consumers. Budget: <500ns total. | `laminar-core/src/dag/executor.rs` |
| **Ring 1 (Background)** | Checkpoint barrier coordination (`DagCheckpointCoordinator`), changelog drain to WAL, `SinkRunner` I/O. | `laminar-core/src/dag/checkpoint.rs` |
| **Ring 2 (Control Plane)** | `DagBuilder` topology construction, `MvRegistry` integration, SQL DDL processing, lifecycle management (start/stop/restart). | `laminar-core/src/dag/builder.rs` |

```
+-------------------------------------------------------------------+
|                        RING 0: HOT PATH                            |
|  DagExecutor.process_event() -- <500ns                             |
|  +-----------+   +-----------+   +-----------+   +----------+      |
|  | Routing   |-->| Operator  |-->| Multicast |-->| Changelog|      |
|  | Table O(1)|   | .process()|   | Buffer    |   | Ref      |      |
|  +-----------+   +-----------+   +-----------+   +----------+      |
|                                                       |            |
|                          SPSC Queue (lock-free)       |            |
|                                                       v            |
+-------------------------------------------------------------------+
|                     RING 1: BACKGROUND                             |
|  +--------------+   +----------+   +------------------+            |
|  | Checkpoint   |   | WAL      |   | SinkRunner       |            |
|  | Coordinator  |   | Drain    |   | (F034 Connector) |            |
|  | (barriers)   |   | (F022)   |   |                  |            |
|  +--------------+   +----------+   +------------------+            |
|                                                                    |
+-------------------------------------------------------------------+
|                     RING 2: CONTROL PLANE                          |
|  +-----------+   +-----------+   +---------------+                 |
|  | DagBuilder|   | MvRegistry|   | ConnectorRuntime|               |
|  | (topology)|   | (F060)    |   | (F034)         |               |
|  +-----------+   +-----------+   +---------------+                 |
+-------------------------------------------------------------------+
```

### 8. Latency Budget (Ring 0)

| Component | Budget | Optimization Strategy |
|-----------|--------|----------------------|
| Routing table lookup | <50ns | Pre-computed array index, cache-line aligned entries |
| Operator dispatch | <200ns | Virtual call to `Operator::process()` (existing) |
| Multicast to N consumers | <100ns | Zero-copy refcount, no data copying |
| Changelog ref push | <10ns | Pre-allocated `ChangelogBuffer` (F063) |
| State access | <200ns | FxHashMap, mmap state store (existing) |
| **Total hot path** | **<500ns** | Consistent with Ring 0 SLA |

### 9. DAG-Aware Checkpoint Coordination

Checkpointing for DAG topologies uses the Chandy-Lamport asynchronous barrier snapshot algorithm, adapted for LaminarDB's three-ring architecture. Barriers are injected at source nodes and flow through DAG edges. At fan-in nodes (multiple inputs), barriers are aligned before snapshotting.

This integrates with the existing `IncrementalCheckpointManager` (F022) for state persistence and `ChangelogBuffer`/`ChangelogRef` (F063) for delta tracking.

```rust
/// Checkpoint barrier that flows through the DAG edges.
///
/// Barriers are injected at source nodes by the checkpoint coordinator
/// (Ring 1) and propagate through the DAG alongside data events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct CheckpointBarrier {
    /// Unique checkpoint identifier (monotonically increasing).
    pub checkpoint_id: u64,
    /// Timestamp when the checkpoint was initiated.
    pub timestamp: i64,
    /// Barrier type.
    pub barrier_type: BarrierType,
}

/// Barrier alignment mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierType {
    /// Aligned barrier (Chandy-Lamport): wait for all inputs before snapshot.
    /// Lower overhead but may cause brief pauses at fan-in nodes.
    Aligned,
    /// Unaligned barrier (Flink 1.11+): snapshot immediately including
    /// in-flight data. No pauses but larger checkpoint size.
    Unaligned,
}

/// Aligns barriers at fan-in nodes (multi-input operators).
///
/// For aligned barriers, the aligner buffers data from channels that
/// have already delivered a barrier, waiting for all inputs to align.
pub struct BarrierAligner {
    /// Per-node, per-checkpoint: which inputs have delivered barriers.
    pending: FxHashMap<(NodeId, u64), FxHashSet<EdgeId>>,
    /// Per-node: total expected input count.
    input_counts: FxHashMap<NodeId, usize>,
    /// Buffered events from aligned channels (waiting for barrier alignment).
    buffered_events: FxHashMap<(NodeId, u64), Vec<Event>>,
}

impl BarrierAligner {
    /// Creates a new barrier aligner.
    pub fn new() -> Self {
        Self {
            pending: FxHashMap::default(),
            input_counts: FxHashMap::default(),
            buffered_events: FxHashMap::default(),
        }
    }

    /// Records that a barrier arrived on a specific input edge.
    ///
    /// Returns `true` if all inputs have now delivered barriers
    /// (alignment complete -- safe to snapshot).
    pub fn receive_barrier(
        &mut self,
        node_id: NodeId,
        edge_id: EdgeId,
        checkpoint_id: u64,
    ) -> bool {
        let key = (node_id, checkpoint_id);
        let received = self.pending.entry(key).or_default();
        received.insert(edge_id);

        let expected = self.input_counts.get(&node_id).copied().unwrap_or(1);
        received.len() >= expected
    }

    /// Cleans up state for a completed checkpoint.
    pub fn complete_checkpoint(&mut self, node_id: NodeId, checkpoint_id: u64) {
        let key = (node_id, checkpoint_id);
        self.pending.remove(&key);
        self.buffered_events.remove(&key);
    }
}

/// Coordinates checkpointing across the DAG topology.
///
/// Runs in Ring 1 (background). Initiates checkpoints by injecting
/// barriers at source nodes, tracks progress, and finalizes when
/// all nodes have completed their snapshots.
///
/// Integrates with:
/// - F022 `IncrementalCheckpointManager` for state persistence
/// - F063 `ChangelogBuffer` for delta tracking
/// - F062 Per-Core WAL for per-node WAL segments
pub struct DagCheckpointCoordinator {
    /// Active checkpoints in progress.
    active_checkpoints: FxHashMap<u64, CheckpointProgress>,
    /// DAG topology reference.
    dag: Arc<StreamingDag>,
    /// Next checkpoint ID.
    next_checkpoint_id: u64,
    /// Checkpoint configuration.
    config: DagCheckpointConfig,
}

/// Progress tracking for an active checkpoint.
pub struct CheckpointProgress {
    /// Checkpoint identifier.
    pub checkpoint_id: u64,
    /// Nodes that have completed their snapshot.
    pub completed_nodes: FxHashSet<NodeId>,
    /// Nodes still pending barrier alignment or snapshot.
    pub pending_nodes: FxHashSet<NodeId>,
    /// Timestamp when the checkpoint was initiated.
    pub started_at: std::time::Instant,
    /// Per-node operator state snapshots.
    pub node_snapshots: FxHashMap<NodeId, Vec<u8>>,
    /// Source offsets at checkpoint time (for exactly-once replay).
    pub source_offsets: FxHashMap<String, FxHashMap<u32, u64>>,
}

/// Configuration for DAG-aware checkpointing.
#[derive(Debug, Clone)]
pub struct DagCheckpointConfig {
    /// Checkpoint interval. `None` = manual trigger only.
    pub interval: Option<std::time::Duration>,
    /// Barrier type (aligned or unaligned).
    pub barrier_type: BarrierType,
    /// Maximum time to wait for barrier alignment before timeout.
    pub alignment_timeout: std::time::Duration,
    /// Whether to use incremental checkpoints (F022).
    pub incremental: bool,
    /// Maximum concurrent checkpoints.
    pub max_concurrent: usize,
}

impl Default for DagCheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Some(std::time::Duration::from_secs(30)),
            barrier_type: BarrierType::Aligned,
            alignment_timeout: std::time::Duration::from_secs(60),
            incremental: true,
            max_concurrent: 2,
        }
    }
}

impl DagCheckpointCoordinator {
    /// Creates a new checkpoint coordinator for the given DAG.
    pub fn new(dag: Arc<StreamingDag>, config: DagCheckpointConfig) -> Self {
        Self {
            active_checkpoints: FxHashMap::default(),
            dag,
            next_checkpoint_id: 1,
            config,
        }
    }

    /// Initiates a checkpoint by injecting barriers at all source nodes.
    ///
    /// Returns the checkpoint ID for tracking.
    pub fn trigger_checkpoint(&mut self) -> u64 {
        let checkpoint_id = self.next_checkpoint_id;
        self.next_checkpoint_id += 1;

        let barrier = CheckpointBarrier {
            checkpoint_id,
            timestamp: chrono::Utc::now().timestamp_millis(),
            barrier_type: self.config.barrier_type,
        };

        // Initialize progress tracking
        let all_nodes: FxHashSet<_> = self.dag.nodes.keys().copied().collect();
        self.active_checkpoints.insert(
            checkpoint_id,
            CheckpointProgress {
                checkpoint_id,
                completed_nodes: FxHashSet::default(),
                pending_nodes: all_nodes,
                started_at: std::time::Instant::now(),
                node_snapshots: FxHashMap::default(),
                source_offsets: FxHashMap::default(),
            },
        );

        // Inject barriers at all source nodes
        // (actual injection is performed by the executor when it
        //  reads the barrier command from the Ring 1 -> Ring 0 SPSC queue)
        checkpoint_id
    }

    /// Records that a node has completed its snapshot.
    ///
    /// Returns `true` if all nodes have completed (checkpoint finalized).
    pub fn on_node_snapshot_complete(
        &mut self,
        checkpoint_id: u64,
        node_id: NodeId,
        snapshot: Vec<u8>,
    ) -> bool {
        if let Some(progress) = self.active_checkpoints.get_mut(&checkpoint_id) {
            progress.completed_nodes.insert(node_id);
            progress.pending_nodes.remove(&node_id);
            progress.node_snapshots.insert(node_id, snapshot);

            if progress.pending_nodes.is_empty() {
                // All nodes completed -- finalize checkpoint
                return true;
            }
        }
        false
    }

    /// Finalizes a completed checkpoint.
    ///
    /// Persists the checkpoint via `IncrementalCheckpointManager` (F022)
    /// and returns checkpoint metadata.
    pub fn finalize_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Option<DagCheckpointMetadata> {
        let progress = self.active_checkpoints.remove(&checkpoint_id)?;
        Some(DagCheckpointMetadata {
            checkpoint_id,
            duration: progress.started_at.elapsed(),
            nodes_snapshotted: progress.completed_nodes.len(),
            source_offsets: progress.source_offsets,
        })
    }
}

/// Metadata for a completed DAG checkpoint.
#[derive(Debug)]
pub struct DagCheckpointMetadata {
    /// Checkpoint identifier.
    pub checkpoint_id: u64,
    /// Total time to complete the checkpoint.
    pub duration: std::time::Duration,
    /// Number of nodes snapshotted.
    pub nodes_snapshotted: usize,
    /// Source offsets for exactly-once replay.
    pub source_offsets: FxHashMap<String, FxHashMap<u32, u64>>,
}
```

### 10. Recovery Strategy

Recovery restores the DAG from the latest complete checkpoint and replays WAL entries after the checkpoint epoch.

```rust
/// Recovers a DAG from a checkpoint.
///
/// # Recovery Sequence
///
/// 1. Load latest valid checkpoint (metadata.json)
/// 2. Restore operator state for each node
/// 3. Replay WAL entries after checkpoint epoch (F022)
/// 4. Reset source offsets for exactly-once replay (F023)
/// 5. Resume DAG execution from recovered state
///
/// # Integration
///
/// - F022: Uses `IncrementalCheckpointManager` for RocksDB checkpoint loading
/// - F062: Uses per-core WAL segments for WAL replay
/// - F034: Resets `SourceConnector::commit_offsets()` for exactly-once
pub struct DagRecoveryManager {
    /// Checkpoint storage backend.
    checkpoint_dir: std::path::PathBuf,
    /// Recovery configuration.
    config: RecoveryConfig,
}

/// Recovery configuration.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Recovery mode.
    pub mode: RecoveryMode,
    /// Parallel recovery threads (one per core, matching F013).
    pub parallelism: usize,
    /// Target recovery time (influences checkpoint frequency).
    pub target_recovery_time: std::time::Duration,
}

/// Recovery mode selection.
#[derive(Debug, Clone)]
pub enum RecoveryMode {
    /// Restore from the latest complete checkpoint.
    Latest,
    /// Restore from a specific checkpoint by ID.
    Specific(u64),
}

impl DagRecoveryManager {
    /// Recovers the DAG from the latest checkpoint.
    ///
    /// # Errors
    ///
    /// Returns `DagError::NoCheckpointFound` if no valid checkpoint exists.
    /// Returns `DagError::RecoveryFailed` if state restoration fails.
    pub async fn recover(
        &self,
        dag: &mut StreamingDag,
        executor: &mut DagExecutor,
    ) -> Result<DagRecoveryMetrics, DagError> {
        // 1. Find latest valid checkpoint
        let checkpoint_meta = self.find_latest_checkpoint()?;

        // 2. Restore operator state for each node (parallel, one per core)
        for &node_id in dag.execution_order() {
            if let Some(snapshot) = checkpoint_meta
                .node_snapshots
                .get(&node_id)
            {
                executor.restore_operator_state(node_id, snapshot)?;
            }
        }

        // 3. Return source offsets for exactly-once replay
        Ok(DagRecoveryMetrics {
            checkpoint_id: checkpoint_meta.checkpoint_id,
            source_offsets: checkpoint_meta.source_offsets.clone(),
            nodes_restored: dag.nodes.len(),
        })
    }

    fn find_latest_checkpoint(&self) -> Result<DagCheckpointMetadata, DagError> {
        // Search checkpoint_dir for latest metadata.json
        // (follows same pattern as F022 IncrementalCheckpointManager)
        todo!("Scan checkpoint directory for latest valid checkpoint")
    }
}

/// Recovery metrics.
#[derive(Debug)]
pub struct DagRecoveryMetrics {
    /// Checkpoint ID that was restored.
    pub checkpoint_id: u64,
    /// Source offsets for exactly-once replay.
    pub source_offsets: FxHashMap<String, FxHashMap<u32, u64>>,
    /// Number of nodes restored.
    pub nodes_restored: usize,
}
```

### 11. Changelog/Retraction Propagation (F063 Integration)

Delta changes flow through DAG edges using the existing `ChangelogRecord<T>` and `ChangelogRef` types from `laminar-core/src/operator/changelog.rs`. This enables incremental computation inspired by DBSP/Feldera's Z-set model.

```rust
use crate::operator::changelog::{ChangelogRecord, ChangelogRef, ChangelogBuffer};
use crate::operator::window::CdcOperation;

/// Delta propagation through the DAG.
///
/// When an operator at a shared stage produces output, the output
/// includes changelog metadata (insert/delete/update weights).
/// Downstream operators use these weights for incremental processing:
///
/// - Aggregators: Call `accumulate()` for weight > 0, `retract()` for weight < 0
/// - Filters: Pass through with weight preserved
/// - Joins: Propagate retractions to both sides
///
/// This follows the DBSP/Feldera incremental computation model
/// (VLDB 2023/2025 Best Paper).
pub struct DagChangelogPropagator {
    /// Per-node changelog buffers for tracking deltas.
    /// Uses pre-allocated ChangelogBuffer from F063 (zero-alloc Ring 0).
    node_changelogs: FxHashMap<NodeId, ChangelogBuffer>,
    /// Whether changelog tracking is enabled.
    enabled: bool,
}

impl DagChangelogPropagator {
    /// Creates a new propagator for the DAG.
    pub fn new(dag: &StreamingDag, buffer_capacity: usize) -> Self {
        let node_changelogs = dag
            .nodes
            .keys()
            .map(|&id| (id, ChangelogBuffer::with_capacity(buffer_capacity)))
            .collect();

        Self {
            node_changelogs,
            enabled: true,
        }
    }

    /// Records a changelog entry for a node's output.
    ///
    /// Ring 0, <10ns (pre-allocated ChangelogBuffer).
    #[inline]
    pub fn record_output(
        &mut self,
        node_id: NodeId,
        batch_offset: u32,
        row_index: u32,
        operation: CdcOperation,
    ) {
        if !self.enabled {
            return;
        }
        if let Some(buffer) = self.node_changelogs.get_mut(&node_id) {
            let changelog_ref = ChangelogRef::new(
                batch_offset,
                row_index,
                operation.weight() as i16,
                operation,
            );
            buffer.push(changelog_ref);
        }
    }

    /// Drains changelog entries for Ring 1 processing.
    ///
    /// Called by the Ring 1 background task to flush deltas to WAL
    /// and propagate to downstream sinks.
    pub fn drain_node(&mut self, node_id: NodeId) -> Vec<ChangelogRef> {
        self.node_changelogs
            .get_mut(&node_id)
            .map(|buf| buf.drain().collect())
            .unwrap_or_default()
    }
}
```

### 12. MvRegistry Integration (F060)

The DAG subsystem integrates with the existing `MvRegistry` (`laminar-core/src/mv/registry.rs`) and `MvPipelineExecutor` (`laminar-core/src/mv/executor.rs`). When SQL `CREATE MATERIALIZED VIEW` statements reference other MVs, the `MvRegistry` already tracks the dependency DAG with cycle detection and topological ordering. The `StreamingDag` can be constructed from an `MvRegistry` snapshot.

```rust
use crate::mv::registry::{MaterializedView, MvRegistry};
use crate::mv::executor::MvPipelineExecutor;
use crate::mv::watermark::CascadingWatermarkTracker;

impl StreamingDag {
    /// Constructs a DAG from an MvRegistry.
    ///
    /// Each MV becomes a `DagNode` with `DagNodeType::MaterializedView`.
    /// Dependencies between MVs become `DagEdge` connections.
    /// Base tables become `DagNodeType::Source` nodes.
    ///
    /// The topological order from `MvRegistry::topo_order()` is preserved.
    pub fn from_mv_registry(registry: &MvRegistry) -> Result<Self, DagError> {
        let mut dag = Self::new();
        let mut name_to_node: FxHashMap<String, NodeId> = FxHashMap::default();

        // Create source nodes for base tables
        for base_table in registry.base_tables() {
            let node_id = dag.add_source_node(base_table)?;
            name_to_node.insert(base_table.to_string(), node_id);
        }

        // Create MV nodes in topological order
        for mv_name in registry.topo_order() {
            if let Some(mv) = registry.get_view(mv_name) {
                let node_id = dag.add_mv_node(mv)?;
                name_to_node.insert(mv_name.to_string(), node_id);

                // Create edges from sources to this MV
                for source_name in &mv.sources {
                    if let Some(&source_node) = name_to_node.get(source_name) {
                        dag.add_edge(source_node, node_id)?;
                    }
                }
            }
        }

        // Compute execution order and routing table
        dag.finalize()?;

        Ok(dag)
    }
}
```

### 13. Watermark Propagation Through DAG

Watermarks propagate through the DAG following min semantics, consistent with the existing `CascadingWatermarkTracker` from `laminar-core/src/mv/watermark.rs`. For DAG topologies with fan-in, a node's watermark is the minimum of all its input watermarks. For fan-out, the watermark is forwarded to all downstream nodes.

This integrates with the per-partition watermarks (F064), keyed watermarks (F065), and alignment groups (F066).

```rust
use crate::mv::watermark::CascadingWatermarkTracker;
use crate::time::watermark::{WatermarkTracker, PartitionedWatermarkTracker};
use crate::time::alignment_group::WatermarkAlignmentGroup;

/// DAG-aware watermark tracker.
///
/// Extends `CascadingWatermarkTracker` with DAG-specific semantics:
/// - Fan-out: watermark forwarded to all downstream nodes
/// - Fan-in: watermark = min(all input watermarks)
/// - Alignment groups (F066): bounded drift between DAG branches
pub struct DagWatermarkTracker {
    /// Per-node watermarks (reuses CascadingWatermarkTracker logic).
    inner: CascadingWatermarkTracker,
    /// Per-node per-partition watermarks (F064 integration).
    partitioned: FxHashMap<NodeId, PartitionedWatermarkTracker>,
    /// Alignment groups for controlling drift between DAG branches (F066).
    alignment_groups: Vec<WatermarkAlignmentGroup>,
}

impl DagWatermarkTracker {
    /// Advances the watermark at a source node and propagates through the DAG.
    ///
    /// Returns the list of (node_name, new_watermark) pairs that were updated.
    pub fn advance_source_watermark(
        &mut self,
        source_name: &str,
        watermark: i64,
    ) -> Vec<(String, i64)> {
        self.inner.update_watermark(source_name, watermark)
    }

    /// Returns the current watermark for a specific node.
    #[must_use]
    pub fn get_watermark(&self, node_name: &str) -> Option<i64> {
        self.inner.get_watermark(node_name)
    }
}
```

### 14. SQL Integration

SQL statements automatically create DAG topologies through the existing `StreamingPlanner` (`laminar-sql/src/planner/mod.rs`) and `MvRegistry`. When `CREATE MATERIALIZED VIEW` references another MV, the dependency is registered, forming a DAG.

#### 14.1 DAG Definition via SQL

```sql
-- Step 1: Define a source
CREATE SOURCE TABLE raw_trades (
    trade_id BIGINT,
    symbol VARCHAR,
    price DOUBLE,
    volume BIGINT,
    event_time TIMESTAMP,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'raw-trades',
    format = 'json'
);

-- Step 2: Define a shared intermediate MV (auto-becomes DAG node)
CREATE MATERIALIZED VIEW normalized_trades AS
SELECT
    symbol,
    price,
    volume,
    event_time,
    CASE WHEN volume > 10000 THEN 'HIGH' ELSE 'NORMAL' END AS volume_class
FROM raw_trades;

-- Step 3: Multiple MVs consuming the shared stage (DAG fan-out)
CREATE MATERIALIZED VIEW vwap AS
SELECT
    symbol,
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    SUM(price * volume) / SUM(volume) AS vwap
FROM normalized_trades  -- References shared intermediate
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1' MINUTE)
EMIT AFTER WATERMARK;

CREATE MATERIALIZED VIEW anomalies AS
SELECT *
FROM normalized_trades  -- Same shared intermediate
WHERE volume_class = 'HIGH';

CREATE MATERIALIZED VIEW position_tracker AS
SELECT
    symbol,
    SUM(volume) AS total_volume,
    COUNT(*) AS trade_count
FROM normalized_trades  -- Same shared intermediate
GROUP BY symbol
EMIT ON UPDATE;

-- LaminarDB automatically detects shared dependencies and creates DAG:
-- raw_trades -> normalized_trades -+-> vwap
--                                  +-> anomalies
--                                  +-> position_tracker
```

#### 14.2 DAG Introspection

```sql
-- Inspect the generated DAG topology
SELECT * FROM laminar_system.dag_topology;

-- Result:
-- | node_id | name               | node_type          | inputs               | outputs                                  | is_shared |
-- |---------|--------------------|--------------------|--------------------- |------------------------------------------|-----------|
-- | 0       | raw_trades         | Source             | []                   | [normalized_trades]                      | false     |
-- | 1       | normalized_trades  | MaterializedView   | [raw_trades]         | [vwap, anomalies, position_tracker]      | true      |
-- | 2       | vwap               | MaterializedView   | [normalized_trades]  | []                                       | false     |
-- | 3       | anomalies          | MaterializedView   | [normalized_trades]  | []                                       | false     |
-- | 4       | position_tracker   | MaterializedView   | [normalized_trades]  | []                                       | false     |

-- Show dependencies for a specific node
SHOW DEPENDENCIES FOR vwap;
-- vwap -> normalized_trades -> raw_trades

-- Explain the DAG execution plan
EXPLAIN DAG;
-- Execution order: raw_trades -> normalized_trades -> [vwap, anomalies, position_tracker]
-- Shared stages: normalized_trades (3 consumers)
-- Channel types: raw_trades->normalized_trades: SPSC, normalized_trades->*: SPMC (multicast)
```

### 15. Programmatic Rust API (Pipeline Builder)

For users who prefer a programmatic API over SQL, the `DagBuilder` provides a fluent builder pattern for constructing DAG topologies.

```rust
use crate::dag::{StreamingDag, DagExecutor, DagCheckpointConfig};
use crate::operator::Operator;
use crate::streaming::{Source, Sink};

/// Builder for constructing DAG topologies programmatically.
///
/// Constructed in Ring 2. Once `build()` is called, the topology is
/// immutable and can be executed in Ring 0.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::dag::DagBuilder;
///
/// let dag = DagBuilder::new()
///     .source("trades", kafka_source)
///     .operator("dedup", dedup_operator)
///     .operator("normalize", normalize_operator)
///     .connect("trades", "dedup")
///     .connect("dedup", "normalize")
///     // Fan-out: shared stage with multiple consumers
///     .fan_out("normalize", |builder| {
///         builder
///             .branch("vwap", vwap_operator)
///             .branch("anomaly", anomaly_operator)
///             .branch("position", position_operator)
///     })
///     .sink("vwap", analytics_sink)
///     .sink("anomaly", alert_sink)
///     .sink("position", risk_sink)
///     .build()?;
/// ```
pub struct DagBuilder {
    /// Nodes being constructed.
    nodes: Vec<(String, DagNodeType, Option<Box<dyn Operator>>)>,
    /// Edges being constructed (source_name, target_name).
    edges: Vec<(String, String)>,
    /// Name -> index mapping.
    name_index: FxHashMap<String, usize>,
    /// Checkpoint configuration.
    checkpoint_config: DagCheckpointConfig,
}

impl DagBuilder {
    /// Creates a new DAG builder.
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            name_index: FxHashMap::default(),
            checkpoint_config: DagCheckpointConfig::default(),
        }
    }

    /// Adds a source node to the DAG.
    ///
    /// Source nodes are entry points that receive data from external
    /// systems via the Connector SDK (F034).
    pub fn source(
        mut self,
        name: &str,
        operator: Box<dyn Operator>,
    ) -> Self {
        let idx = self.nodes.len();
        self.nodes.push((name.to_string(), DagNodeType::Source, Some(operator)));
        self.name_index.insert(name.to_string(), idx);
        self
    }

    /// Adds an operator node to the DAG.
    pub fn operator(
        mut self,
        name: &str,
        operator: Box<dyn Operator>,
    ) -> Self {
        let idx = self.nodes.len();
        self.nodes.push((
            name.to_string(),
            DagNodeType::StatefulOperator,
            Some(operator),
        ));
        self.name_index.insert(name.to_string(), idx);
        self
    }

    /// Connects two nodes with an edge.
    pub fn connect(mut self, from: &str, to: &str) -> Self {
        self.edges.push((from.to_string(), to.to_string()));
        self
    }

    /// Creates a fan-out from a shared stage to multiple branches.
    ///
    /// The `branches` closure receives a `FanOutBuilder` that allows
    /// adding multiple downstream branches from the shared node.
    pub fn fan_out<F>(mut self, shared_node: &str, branches: F) -> Self
    where
        F: FnOnce(FanOutBuilder) -> FanOutBuilder,
    {
        let fan_out = branches(FanOutBuilder::new(shared_node.to_string()));
        for (branch_name, branch_type, branch_op) in fan_out.branches {
            let idx = self.nodes.len();
            self.nodes.push((branch_name.clone(), branch_type, Some(branch_op)));
            self.name_index.insert(branch_name.clone(), idx);
            self.edges.push((shared_node.to_string(), branch_name));
        }
        self
    }

    /// Adds a sink node to the DAG.
    ///
    /// Sink nodes send data to external systems via the Connector SDK (F034).
    pub fn sink(
        mut self,
        name: &str,
        operator: Box<dyn Operator>,
    ) -> Self {
        let idx = self.nodes.len();
        self.nodes.push((name.to_string(), DagNodeType::Sink, Some(operator)));
        self.name_index.insert(name.to_string(), idx);
        // Auto-connect: find the node with matching name prefix
        // (e.g., sink "vwap" connects to operator "vwap")
        if let Some(&source_idx) = self.name_index.get(name) {
            // Already connected via fan_out
        }
        self
    }

    /// Configures checkpointing for the DAG.
    pub fn checkpoint_config(mut self, config: DagCheckpointConfig) -> Self {
        self.checkpoint_config = config;
        self
    }

    /// Builds the immutable DAG topology.
    ///
    /// Validates the topology (acyclic, connected, schema-compatible),
    /// computes topological order, derives channel types, and builds
    /// the pre-computed routing table.
    ///
    /// # Errors
    ///
    /// Returns `DagError::CycleDetected` if the graph contains cycles.
    /// Returns `DagError::DisconnectedNode` if a node has no connections.
    /// Returns `DagError::NodeNotFound` if an edge references a missing node.
    pub fn build(self) -> Result<StreamingDag, DagError> {
        let mut dag = StreamingDag::new();

        // Add nodes
        for (name, node_type, operator) in self.nodes {
            dag.add_node(name, node_type, operator)?;
        }

        // Add edges
        for (from, to) in self.edges {
            let from_id = dag.node_id_by_name(&from)
                .ok_or_else(|| DagError::NodeNotFound(from.clone()))?;
            let to_id = dag.node_id_by_name(&to)
                .ok_or_else(|| DagError::NodeNotFound(to.clone()))?;
            dag.add_edge(from_id, to_id)?;
        }

        // Validate and finalize
        dag.finalize()?;

        Ok(dag)
    }
}

/// Builder for fan-out branches from a shared stage.
pub struct FanOutBuilder {
    shared_node: String,
    branches: Vec<(String, DagNodeType, Box<dyn Operator>)>,
}

impl FanOutBuilder {
    fn new(shared_node: String) -> Self {
        Self {
            shared_node,
            branches: Vec::new(),
        }
    }

    /// Adds a branch from the shared stage.
    pub fn branch(
        mut self,
        name: &str,
        operator: Box<dyn Operator>,
    ) -> Self {
        self.branches.push((
            name.to_string(),
            DagNodeType::StatefulOperator,
            operator,
        ));
        self
    }
}
```

### 16. Connector SDK Integration (F034)

DAG source and sink nodes connect to external systems via the Connector SDK (`laminar-connectors`). The `ConnectorRuntime` (F034) manages the lifecycle of connectors in Ring 1, while the DAG processes data in Ring 0.

```rust
use crate::streaming::Source;

/// Integration point between DAG source nodes and the Connector SDK.
///
/// When a DAG source node is backed by a `SourceConnector` (e.g., Kafka),
/// the `ConnectorRuntime` runs the connector in Ring 1 and pushes data
/// into the DAG via a `Source<ArrowRecord>` channel.
///
/// ```text
/// Ring 1: SourceConnector.poll_batch() -> Source.push_arrow()
///                                              |
///                                              v (SPSC channel)
/// Ring 0: DagExecutor reads from source node's input queue
/// ```
pub struct ConnectorDagBridge {
    /// Mapping from DAG source node to connector source handle.
    source_handles: FxHashMap<NodeId, SourceHandle>,
    /// Mapping from DAG sink node to connector sink handle.
    sink_handles: FxHashMap<NodeId, SinkHandle>,
}

impl ConnectorDagBridge {
    /// Attaches a source connector to a DAG source node.
    ///
    /// The connector is started via `ConnectorRuntime::start_source()`
    /// and its output is bridged to the DAG node's input queue.
    pub async fn attach_source(
        &mut self,
        runtime: &mut ConnectorRuntime,
        node_id: NodeId,
        connector_config: ConnectorConfig,
    ) -> Result<(), DagError> {
        let handle = runtime.start_source(
            &format!("dag_source_{}", node_id.0),
            /* connector and deserializer from ConnectorRegistry */
            connector_config,
        ).await?;
        self.source_handles.insert(node_id, handle);
        Ok(())
    }

    /// Attaches a sink connector to a DAG sink node.
    ///
    /// The sink subscribes to the DAG node's output and writes
    /// to the external system via `ConnectorRuntime::start_sink()`.
    pub async fn attach_sink(
        &mut self,
        runtime: &mut ConnectorRuntime,
        node_id: NodeId,
        connector_config: ConnectorConfig,
    ) -> Result<(), DagError> {
        let handle = runtime.start_sink(
            &format!("dag_sink_{}", node_id.0),
            /* connector and serializer from ConnectorRegistry */
            connector_config,
        ).await?;
        self.sink_handles.insert(node_id, handle);
        Ok(())
    }
}
```

### 17. Streaming API Integration (F-STREAM-001 to F-STREAM-007)

Internal DAG edges use the existing streaming channel primitives from `laminar-core/src/streaming/`. For linear segments (SPSC edges), the existing `streaming::channel` is used directly. For fan-out edges (SPMC), the `MulticastBuffer` is used. For fan-in edges (MPSC), the existing auto-upgrade mechanism from `streaming::channel` is leveraged.

```rust
use crate::streaming::channel::{channel_with_config, ChannelConfig, Producer, Consumer};
use crate::streaming::config::BackpressureStrategy;

/// Creates internal channels for DAG edges.
///
/// Channel type is auto-derived from the DAG topology (consistent
/// with the streaming API's auto-upgrade pattern).
fn create_edge_channel(
    edge: &DagEdge,
    config: &ChannelConfig,
) -> (Box<dyn EdgeProducer>, Box<dyn EdgeConsumer>) {
    match edge.channel_type {
        DagChannelType::Spsc => {
            // Use existing streaming::channel in SPSC mode
            let (producer, consumer) = channel_with_config::<Event>(config.clone());
            (Box::new(SpscEdgeProducer(producer)),
             Box::new(SpscEdgeConsumer(consumer)))
        }
        DagChannelType::Spmc => {
            // Use MulticastBuffer for zero-copy fan-out
            // (MulticastBuffer is created at the shared stage level)
            todo!("Multicast buffer wiring")
        }
        DagChannelType::Mpsc => {
            // Use existing streaming::channel with MPSC auto-upgrade
            // (clone the producer for each upstream edge)
            let (producer, consumer) = channel_with_config::<Event>(config.clone());
            (Box::new(MpscEdgeProducer(producer)),
             Box::new(MpscEdgeConsumer(consumer)))
        }
    }
}
```

---

## Error Types

```rust
use thiserror::Error;

/// Errors that can occur in the DAG subsystem.
#[derive(Debug, Error)]
pub enum DagError {
    /// The DAG contains a cycle.
    #[error("cycle detected involving node: {0}")]
    CycleDetected(String),

    /// A node referenced by name was not found.
    #[error("node not found: {0}")]
    NodeNotFound(String),

    /// A disconnected node was detected (no inputs and no outputs).
    #[error("disconnected node: {0}")]
    DisconnectedNode(String),

    /// Schema mismatch between connected nodes.
    #[error("schema mismatch on edge {from} -> {to}: {details}")]
    SchemaMismatch {
        from: String,
        to: String,
        details: String,
    },

    /// Backpressure: a shared stage's multicast buffer is full.
    #[error("backpressure: shared stage buffer full")]
    BackpressureFull,

    /// An operator returned an error during processing.
    #[error("operator error at node {node}: {source}")]
    OperatorError {
        node: String,
        #[source]
        source: crate::operator::OperatorError,
    },

    /// Checkpoint timeout.
    #[error("checkpoint {checkpoint_id} timed out after {elapsed:?}")]
    CheckpointTimeout {
        checkpoint_id: u64,
        elapsed: std::time::Duration,
    },

    /// No valid checkpoint found for recovery.
    #[error("no valid checkpoint found in: {0}")]
    NoCheckpointFound(String),

    /// Recovery failed.
    #[error("recovery failed: {0}")]
    RecoveryFailed(String),

    /// Maximum fan-out exceeded (>8 targets per output port).
    #[error("fan-out limit exceeded for node {node}: {count} targets (max 8)")]
    FanOutLimitExceeded {
        node: String,
        count: usize,
    },

    /// MV registry error (delegates to MvError).
    #[error("MV registry error: {0}")]
    MvError(#[from] crate::mv::MvError),

    /// Connector error (delegates to ConnectorError).
    #[error("connector error: {0}")]
    ConnectorError(String),
}
```

---

## Module Structure

```
crates/laminar-core/src/dag/
+-- mod.rs                   # Public API re-exports
+-- topology.rs              # StreamingDag, DagNode, DagEdge, DagChannelType
+-- builder.rs               # DagBuilder, FanOutBuilder
+-- executor.rs              # DagExecutor, DagExecutorMetrics
+-- routing.rs               # RoutingTable, RoutingEntry
+-- multicast.rs             # MulticastBuffer, SharedStageMetadata
+-- checkpoint.rs            # DagCheckpointCoordinator, BarrierAligner, CheckpointBarrier
+-- recovery.rs              # DagRecoveryManager, RecoveryConfig
+-- watermark.rs             # DagWatermarkTracker
+-- changelog.rs             # DagChangelogPropagator (F063 integration)
+-- connector_bridge.rs      # ConnectorDagBridge (F034 integration)
+-- error.rs                 # DagError
```

SQL integration additions:

```
crates/laminar-sql/src/
+-- planner/
|   +-- dag_planner.rs       # StreamingDag construction from SQL statements
+-- translator/
|   +-- dag_translator.rs    # SQL -> DagBuilder conversion
```

---

## Implementation Roadmap

### Phase 3.1: Core DAG Infrastructure (2 weeks)

- [ ] `DagNode`, `DagEdge`, `StreamingDag` topology data structures
- [ ] `DagBuilder` with `source()`, `operator()`, `connect()`, `fan_out()`, `sink()`
- [ ] Channel type derivation (SPSC/SPMC/MPSC auto-derive)
- [ ] Topological sort (Kahn's algorithm, reusing `MvRegistry` pattern)
- [ ] Cycle detection (DFS, reusing `MvRegistry::would_create_cycle` logic)
- [ ] `DagError` error types
- [ ] Unit tests: topology construction, validation, cycle detection (15+ tests)

### Phase 3.2: Zero-Copy Multicast and Routing (1.5 weeks)

- [ ] `MulticastBuffer<T>` with reference-counted slots
- [ ] `SharedStageMetadata` for fan-out nodes
- [ ] `RoutingTable` with cache-line aligned entries
- [ ] Backpressure propagation (slowest consumer)
- [ ] Unit tests: multicast publish/consume, backpressure, routing (10+ tests)
- [ ] Micro-benchmarks: multicast latency, routing lookup latency

### Phase 3.3: DAG Executor (1.5 weeks)

- [ ] `DagExecutor::process_event()` with topological processing
- [ ] `DagExecutor::route_output()` with routing table dispatch
- [ ] Integration with existing `Operator` trait
- [ ] Pre-allocated input queues (zero-alloc hot path)
- [ ] `HotPathGuard` integration (F071)
- [ ] Integration tests: linear DAG, fan-out DAG, fan-in DAG, diamond DAG (15+ tests)
- [ ] Benchmark: verify <500ns hot path with DAG routing

### Phase 3.4: Checkpoint Coordination (2 weeks)

- [ ] `CheckpointBarrier` type
- [ ] `BarrierAligner` for fan-in nodes
- [ ] `DagCheckpointCoordinator` (Ring 1)
- [ ] Integration with `IncrementalCheckpointManager` (F022)
- [ ] Integration with `ChangelogBuffer` (F063)
- [ ] `DagRecoveryManager` with parallel state restoration
- [ ] Tests: barrier alignment, checkpoint complete, recovery (10+ tests)

### Phase 3.5: SQL and MvRegistry Integration (1.5 weeks)

- [ ] `StreamingDag::from_mv_registry()` constructor
- [ ] `DagWatermarkTracker` with min-propagation semantics
- [ ] Integration with `CascadingWatermarkTracker` (F060)
- [ ] `DagChangelogPropagator` for delta propagation (F063)
- [ ] SQL `EXPLAIN DAG` command
- [ ] `laminar_system.dag_topology` system table
- [ ] Tests: SQL-to-DAG, watermark propagation, changelog propagation (10+ tests)

### Phase 3.6: Connector SDK Bridge (1 week)

- [ ] `ConnectorDagBridge` with `attach_source()`, `attach_sink()`
- [ ] Integration with `ConnectorRuntime` (F034)
- [ ] End-to-end test: Kafka source -> DAG -> Kafka sink (mock connector)
- [ ] Tests: connector lifecycle, checkpoint coordination (5+ tests)

### Phase 3.7: Performance Validation (1 week)

- [ ] Criterion benchmarks for DAG routing (<50ns)
- [ ] Criterion benchmarks for multicast (<100ns per consumer)
- [ ] End-to-end latency benchmark with complex DAG (<500ns p99)
- [ ] Throughput benchmark (>500K events/sec/core with DAG)
- [ ] Recovery time benchmark (<5s for 1GB state)
- [ ] Stress testing with 20-node DAG topology

---

## Test Cases

```rust
#[test]
fn test_linear_dag() {
    // Source -> A -> B -> Sink
    let dag = DagBuilder::new()
        .source("source", Box::new(PassthroughOp))
        .operator("a", Box::new(PassthroughOp))
        .operator("b", Box::new(PassthroughOp))
        .sink("sink", Box::new(PassthroughOp))
        .connect("source", "a")
        .connect("a", "b")
        .connect("b", "sink")
        .build()
        .unwrap();

    assert_eq!(dag.nodes.len(), 4);
    assert_eq!(dag.edges.len(), 3);
    assert_eq!(dag.execution_order().len(), 4);
    // All edges should be SPSC (linear pipeline)
    for edge in dag.edges.values() {
        assert_eq!(edge.channel_type, DagChannelType::Spsc);
    }
}

#[test]
fn test_fan_out_dag() {
    // Source -> Shared -> {A, B, C}
    let dag = DagBuilder::new()
        .source("source", Box::new(PassthroughOp))
        .operator("shared", Box::new(PassthroughOp))
        .connect("source", "shared")
        .fan_out("shared", |b| {
            b.branch("a", Box::new(PassthroughOp))
             .branch("b", Box::new(PassthroughOp))
             .branch("c", Box::new(PassthroughOp))
        })
        .build()
        .unwrap();

    assert_eq!(dag.shared_stages().len(), 1);
    let shared_meta = dag.shared_stages().values().next().unwrap();
    assert_eq!(shared_meta.consumer_count, 3);
}

#[test]
fn test_fan_in_dag() {
    // {A, B} -> Merge -> Sink
    let dag = DagBuilder::new()
        .source("a", Box::new(PassthroughOp))
        .source("b", Box::new(PassthroughOp))
        .operator("merge", Box::new(PassthroughOp))
        .sink("sink", Box::new(PassthroughOp))
        .connect("a", "merge")
        .connect("b", "merge")
        .connect("merge", "sink")
        .build()
        .unwrap();

    // merge node should have MPSC edge type
    let merge_id = dag.node_id_by_name("merge").unwrap();
    assert_eq!(dag.incoming_edge_count(merge_id), 2);
}

#[test]
fn test_diamond_dag() {
    // Source -> {A, B} -> Merge -> Sink
    let dag = DagBuilder::new()
        .source("source", Box::new(PassthroughOp))
        .operator("a", Box::new(PassthroughOp))
        .operator("b", Box::new(PassthroughOp))
        .operator("merge", Box::new(PassthroughOp))
        .sink("sink", Box::new(PassthroughOp))
        .connect("source", "a")
        .connect("source", "b")
        .connect("a", "merge")
        .connect("b", "merge")
        .connect("merge", "sink")
        .build()
        .unwrap();

    // source has fan-out (SPMC), merge has fan-in (MPSC)
    let source_id = dag.node_id_by_name("source").unwrap();
    let merge_id = dag.node_id_by_name("merge").unwrap();
    assert_eq!(dag.outgoing_edge_count(source_id), 2);
    assert_eq!(dag.incoming_edge_count(merge_id), 2);
}

#[test]
fn test_cycle_detection() {
    let result = DagBuilder::new()
        .operator("a", Box::new(PassthroughOp))
        .operator("b", Box::new(PassthroughOp))
        .connect("a", "b")
        .connect("b", "a")  // Cycle!
        .build();

    assert!(matches!(result, Err(DagError::CycleDetected(_))));
}

#[test]
fn test_multicast_buffer_single_consumer() {
    let buffer = MulticastBuffer::<u64>::new(16, 1);

    buffer.publish(42).unwrap();
    assert_eq!(buffer.consume(0), Some(&42));
    assert_eq!(buffer.consume(0), None); // Already consumed
}

#[test]
fn test_multicast_buffer_multiple_consumers() {
    let buffer = MulticastBuffer::<u64>::new(16, 3);

    buffer.publish(42).unwrap();
    assert_eq!(buffer.consume(0), Some(&42));
    assert_eq!(buffer.consume(1), Some(&42));
    assert_eq!(buffer.consume(2), Some(&42));
    assert_eq!(buffer.consume(0), None); // All consumed
}

#[test]
fn test_multicast_buffer_backpressure() {
    let buffer = MulticastBuffer::<u64>::new(2, 2); // Capacity 2

    buffer.publish(1).unwrap();
    buffer.publish(2).unwrap();

    // Buffer full (consumer 0 hasn't read yet)
    assert!(matches!(buffer.publish(3), Err(DagError::BackpressureFull)));

    // Consumer 0 reads slot 0
    buffer.consume(0);
    // Consumer 1 hasn't read slot 0 yet, so still full
    assert!(matches!(buffer.publish(3), Err(DagError::BackpressureFull)));

    // Consumer 1 reads slot 0 (refcount -> 0, slot freed)
    buffer.consume(1);
    // Now slot 0 is free
    buffer.publish(3).unwrap();
}

#[test]
fn test_routing_table_lookup() {
    let dag = DagBuilder::new()
        .source("s", Box::new(PassthroughOp))
        .operator("a", Box::new(PassthroughOp))
        .operator("b", Box::new(PassthroughOp))
        .connect("s", "a")
        .connect("s", "b")
        .build()
        .unwrap();

    let table = RoutingTable::build(&dag);
    let source_id = dag.node_id_by_name("s").unwrap();
    let entry = table.route(source_id, 0);

    assert_eq!(entry.target_count, 2);
    assert!(entry.is_multicast);
}

#[test]
fn test_barrier_alignment() {
    let mut aligner = BarrierAligner::new();
    aligner.input_counts.insert(NodeId(2), 2); // Node 2 has 2 inputs

    let edge_a = EdgeId(0);
    let edge_b = EdgeId(1);
    let checkpoint_id = 1;

    // First barrier arrives -- not yet aligned
    assert!(!aligner.receive_barrier(NodeId(2), edge_a, checkpoint_id));

    // Second barrier arrives -- aligned
    assert!(aligner.receive_barrier(NodeId(2), edge_b, checkpoint_id));
}

#[test]
fn test_dag_from_mv_registry() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");

    let schema = test_schema();
    registry.register(MaterializedView::new(
        "ohlc_1s",
        "SELECT ...",
        vec!["trades".into()],
        schema.clone(),
    )).unwrap();
    registry.register(MaterializedView::new(
        "ohlc_1m",
        "SELECT ...",
        vec!["ohlc_1s".into()],
        schema.clone(),
    )).unwrap();

    let dag = StreamingDag::from_mv_registry(&registry).unwrap();
    assert_eq!(dag.nodes.len(), 3); // trades, ohlc_1s, ohlc_1m
    assert_eq!(dag.execution_order().len(), 3);
}

#[test]
fn test_dag_executor_process_event() {
    let dag = build_linear_dag(); // Source -> Double -> Sink
    let mut executor = DagExecutor::new(Arc::new(dag), 1024);
    executor.register_operator(NodeId(1), Box::new(DoubleOp));

    let event = Event::new(42);
    let mut ctx = OperatorContext::new();

    executor.process_event(NodeId(0), event, &mut ctx).unwrap();

    assert_eq!(executor.metrics.events_processed, 1);
}

#[test]
fn test_dag_watermark_propagation() {
    // trades -> ohlc_1s -> {ohlc_1m, anomalies}
    let (dag, registry) = build_fan_out_dag();
    let mut tracker = DagWatermarkTracker::new(registry);

    let updated = tracker.advance_source_watermark("trades", 60_000);

    // All downstream nodes should receive the watermark
    assert!(updated.iter().any(|(n, _)| n == "ohlc_1s"));
    assert!(updated.iter().any(|(n, _)| n == "ohlc_1m"));
    assert!(updated.iter().any(|(n, _)| n == "anomalies"));
}
```

---

## Acceptance Criteria

- [ ] `StreamingDag` topology model with nodes, edges, topological ordering
- [ ] `DagBuilder` fluent API for programmatic DAG construction
- [ ] Automatic channel type derivation (SPSC/SPMC/MPSC)
- [ ] Cycle detection on topology construction
- [ ] `MulticastBuffer<T>` for zero-copy fan-out with reference counting
- [ ] `RoutingTable` with O(1) cache-line aligned lookup
- [ ] `DagExecutor` processing events in topological order in Ring 0
- [ ] Ring 0 hot path latency <500ns with DAG routing
- [ ] `BarrierAligner` for multi-input checkpoint alignment
- [ ] `DagCheckpointCoordinator` with barrier injection and progress tracking
- [ ] `DagRecoveryManager` with parallel state restoration
- [ ] `StreamingDag::from_mv_registry()` for SQL-to-DAG integration
- [ ] `DagWatermarkTracker` with min-semantics propagation
- [ ] `DagChangelogPropagator` for delta propagation (F063)
- [ ] `ConnectorDagBridge` for F034 integration
- [ ] SQL `EXPLAIN DAG` command
- [ ] 60+ unit tests passing
- [ ] Criterion benchmarks meeting latency targets

---

## Performance Targets

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| DAG routing lookup | <50ns | `cargo bench --bench dag_routing` |
| Multicast per consumer | <100ns | `cargo bench --bench multicast` |
| Hot path with DAG (end-to-end) | <500ns p99 | `cargo bench --bench dag_latency` |
| Checkpoint overhead | <5% throughput impact | Throughput with/without checkpoints |
| Recovery time | <5s for 1GB state | Recovery benchmark |
| Throughput/core with DAG | >500K events/sec | `cargo bench --bench dag_throughput` |
| Memory overhead for DAG metadata | <10% vs linear | Memory profiling |
| Barrier alignment latency | <1us per fan-in node | Micro-benchmark |

---

## Future Enhancements

1. **F-DAG-002: Dynamic Topology** -- Add/remove operators without full restart (Ring 2 coordination)
2. **F-DAG-003: DAG Visualization** -- Web UI for viewing DAG topology and metrics (Phase 5)
3. **Operator Fusion** -- Automatically merge adjacent stateless operators (map+filter) into a single fused operator
4. **Multi-way Join Optimization** -- Adaptive join ordering based on cardinality estimates
5. **Distributed DAG** -- Partition DAG across multiple nodes with inter-node channels
6. **Backfill Integration** -- F061 historical backfill through DAG topology

---

## References

1. **DBSP/Feldera** -- Incremental view maintenance theory
   - Budiu et al., "DBSP: Automatic Incremental View Maintenance", VLDB 2023/2025 Best Paper
   - [Feldera Documentation](https://docs.feldera.com/)

2. **Flink 2.0** -- Disaggregated state management
   - Mei et al., "Disaggregated State Management in Apache Flink 2.0", VLDB 2025

3. **Checkpointing Research** -- Barrier-based snapshots
   - Chandy, Lamport, "Distributed Snapshots: Determining Global States of Distributed Systems", 1985
   - "Enhancing checkpointing and state recovery for large-scale stream processing", WJARR 2025
   - Ca-Stream adaptive checkpointing, Wiley 2025

4. **LaminarDB Existing Implementation**
   - `MvRegistry` and `MvPipelineExecutor`: `crates/laminar-core/src/mv/`
   - `ChangelogBuffer` and `ChangelogRef`: `crates/laminar-core/src/operator/changelog.rs`
   - Streaming channels: `crates/laminar-core/src/streaming/channel.rs`
   - `StreamingPlanner`: `crates/laminar-sql/src/planner/mod.rs`
   - `IncrementalCheckpointManager`: F022
   - Connector SDK: F034 (`crates/laminar-connectors/`)

5. **Research Compendium** -- Section 6: DAG & Pipeline Topologies
   - [DAG Pipeline Spec (Draft)](../../research/laminardb-dag-pipeline-spec.md)

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **DAG** | Directed Acyclic Graph -- topology where data flows in one direction without cycles |
| **Fan-out** | One operator output feeding multiple downstream operators |
| **Fan-in** | Multiple upstream operators feeding into one operator |
| **Shared stage** | An operator whose output is consumed by multiple downstream paths |
| **Barrier** | A marker injected into the stream to coordinate checkpoints |
| **Multicast** | Zero-copy delivery of one message to multiple consumers |
| **Aligned barrier** | Barrier that waits for all inputs before snapshotting (Chandy-Lamport) |
| **Unaligned barrier** | Barrier that snapshots immediately including in-flight data |
| **Routing table** | Pre-computed array mapping (node, port) to target nodes |
| **Topological order** | Processing sequence where all dependencies come before dependents |

---

## Appendix B: Comparison with Alternatives

| Feature | LaminarDB (F-DAG-001) | Apache Flink | RisingWave | Materialize | DBSP/Feldera |
|---------|----------------------|--------------|------------|-------------|--------------|
| Embedded deployment | Yes | No | No | No | Yes |
| Sub-microsecond routing | Yes (<50ns) | No (~ms) | No (~ms) | No (~ms) | Yes |
| Zero-copy multicast | Yes | No | No | Arrangement sharing | Implicit |
| Pre-computed routing | Yes (O(1) array) | No (dynamic) | No (dynamic) | No (frontier) | N/A |
| Barrier checkpointing | Yes (aligned + unaligned) | Yes | Yes | Frontier-based | Epoch-based |
| SQL-to-DAG | Yes (auto from MVs) | Yes (auto from SQL) | Yes (auto from MVs) | Yes (auto from MVs) | Yes |
| Programmatic API | Yes (DagBuilder) | Yes (DataStream) | No (SQL only) | No (SQL only) | Yes (Rust API) |
| Thread-per-core | Yes (F013) | No | No | No | No |
| Changelog propagation | Yes (Z-sets, F063) | Yes (retraction) | Yes (changelog) | Yes (differential) | Yes (Z-sets) |
