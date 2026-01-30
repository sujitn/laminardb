//! # DAG Pipeline Topology
//!
//! Directed Acyclic Graph (DAG) topology data structures for complex streaming
//! workflows with fan-out, fan-in, and shared intermediate stages.
//!
//! ## Overview
//!
//! This module provides the topology layer for DAG pipelines:
//!
//! - **`StreamingDag`**: The complete DAG topology with topological ordering
//! - **`DagBuilder`**: Fluent builder API for programmatic DAG construction
//! - **`DagNode`** / **`DagEdge`**: Adjacency list representation
//! - **`DagChannelType`**: Auto-derived channel types (SPSC/SPMC/MPSC)
//!
//! ## Key Design Principles
//!
//! 1. **Channel type is auto-derived** - SPSC/SPMC/MPSC inferred from topology
//! 2. **Cycle detection** - Rejected at construction time
//! 3. **Schema validation** - Connected edges must have compatible schemas
//! 4. **Immutable once finalized** - Topology is frozen after `build()`
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     RING 2: CONTROL PLANE                       │
//! │  DagBuilder constructs StreamingDag topology                    │
//! │  ┌──────────┐   ┌──────────────┐   ┌───────────────────┐       │
//! │  │DagBuilder│──▶│ StreamingDag │──▶│ DagExecutor (F003)│       │
//! │  │ (Ring 2) │   │  (immutable) │   │    (Ring 0)       │       │
//! │  └──────────┘   └──────────────┘   └───────────────────┘       │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use laminar_core::dag::DagBuilder;
//!
//! let dag = DagBuilder::new()
//!     .source("trades", schema.clone())
//!     .operator("normalize", schema.clone())
//!     .connect("trades", "normalize")
//!     .fan_out("normalize", |b| {
//!         b.branch("vwap", schema.clone())
//!          .branch("anomaly", schema.clone())
//!     })
//!     .sink_for("vwap", "analytics", schema.clone())
//!     .sink_for("anomaly", "alerts", schema.clone())
//!     .build()?;
//!
//! assert_eq!(dag.node_count(), 5);
//! assert_eq!(dag.sources().len(), 1);
//! assert_eq!(dag.sinks().len(), 2);
//! ```

pub mod builder;
pub mod error;
pub mod topology;

#[cfg(test)]
mod tests;

// Re-export key types
pub use builder::{DagBuilder, FanOutBuilder};
pub use error::DagError;
pub use topology::{
    DagChannelType, DagEdge, DagNode, DagNodeType, EdgeId, NodeId, PartitioningStrategy,
    SharedStageMetadata, StatePartitionId, StreamingDag, MAX_FAN_OUT,
};
