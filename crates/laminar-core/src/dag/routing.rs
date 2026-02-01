//! Pre-computed routing table for O(1) hot path dispatch.
//!
//! The [`RoutingTable`] is built once during DAG finalization (Ring 2) and
//! provides O(1) lookups during event processing (Ring 0). Each
//! [`RoutingEntry`] is cache-line aligned (64 bytes) to avoid false sharing.
//!
//! # Index Formula
//!
//! ```text
//! index = node_id * MAX_PORTS + port
//! ```
//!
//! For most nodes, `port = 0` (single output). Multi-output operators
//! use separate ports for distinct output streams.

use super::topology::{NodeId, StreamingDag, MAX_FAN_OUT};

/// Maximum output ports per node.
///
/// Each port can route to a different set of targets.
/// In practice, most nodes use only port 0.
pub const MAX_PORTS: usize = MAX_FAN_OUT;

/// A cache-line aligned routing entry for a `(node, port)` pair.
///
/// Contains the target node IDs and metadata for dispatch.
/// Sized at exactly 64 bytes to align with CPU cache lines,
/// preventing false sharing between entries accessed by different cores.
///
/// # Layout (64 bytes)
///
/// ```text
/// [  targets: [u32; 8]  ][count][mcast][   padding: [u8; 30]   ]
/// |       32 bytes       | 1B   | 1B   |       30 bytes         |
/// ```
#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct RoutingEntry {
    /// Target node IDs (raw `u32` values from [`NodeId`]).
    pub targets: [u32; MAX_FAN_OUT],
    /// Number of active targets in the `targets` array.
    pub target_count: u8,
    /// Whether this entry routes to multiple consumers (multicast).
    pub is_multicast: bool,
    /// Padding to fill the 64-byte cache line.
    _padding: [u8; 30],
}

impl RoutingEntry {
    /// Creates an empty routing entry (no targets).
    #[must_use]
    const fn empty() -> Self {
        Self {
            targets: [0; MAX_FAN_OUT],
            target_count: 0,
            is_multicast: false,
            _padding: [0; 30],
        }
    }

    /// Returns the active target node IDs as a slice.
    #[inline]
    #[must_use]
    pub fn target_ids(&self) -> &[u32] {
        &self.targets[..self.target_count as usize]
    }

    /// Returns whether this entry has no targets (terminal/sink node).
    #[inline]
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.target_count == 0
    }
}

impl std::fmt::Debug for RoutingEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutingEntry")
            .field("targets", &self.target_ids())
            .field("target_count", &self.target_count)
            .field("is_multicast", &self.is_multicast)
            .finish_non_exhaustive()
    }
}

/// Pre-computed routing table for O(1) DAG event dispatch.
///
/// Built from a finalized [`StreamingDag`] during topology construction
/// (Ring 2). Provides cache-aligned O(1) lookups indexed by `(node_id, port)`.
///
/// # Performance Target
///
/// Route lookup: < 50ns (single cache-line read).
#[derive(Debug)]
pub struct RoutingTable {
    /// Flat array of routing entries, indexed by `node_id * MAX_PORTS + port`.
    routes: Box<[RoutingEntry]>,
    /// Maximum node ID in the table (for bounds checking).
    max_node_id: u32,
}

impl RoutingTable {
    /// Builds a routing table from a finalized [`StreamingDag`].
    ///
    /// Scans all nodes and their outgoing edges to populate routing entries.
    /// All targets for a node are collected into a single entry at port 0.
    ///
    /// # Arguments
    ///
    /// * `dag` - A finalized `StreamingDag` topology
    #[must_use]
    pub fn from_dag(dag: &StreamingDag) -> Self {
        let max_node_id = dag.nodes().keys().map(|n| n.0).max().unwrap_or(0);
        let table_size = (max_node_id as usize + 1) * MAX_PORTS;
        let mut routes = vec![RoutingEntry::empty(); table_size];

        for node in dag.nodes().values() {
            if node.outputs.is_empty() {
                continue; // Sink nodes have no routing entries
            }

            // Collect all downstream targets for this node.
            let mut targets = [0u32; MAX_FAN_OUT];
            let mut count = 0u8;

            for &edge_id in &node.outputs {
                if let Some(edge) = dag.edge(edge_id) {
                    if (count as usize) < MAX_FAN_OUT {
                        targets[count as usize] = edge.target.0;
                        count += 1;
                    }
                }
            }

            // Store at port 0 (primary output).
            let idx = node.id.0 as usize * MAX_PORTS;
            routes[idx] = RoutingEntry {
                targets,
                target_count: count,
                is_multicast: count > 1,
                _padding: [0; 30],
            };
        }

        Self {
            routes: routes.into_boxed_slice(),
            max_node_id,
        }
    }

    /// Looks up the routing entry for a `(source, port)` pair.
    ///
    /// # Arguments
    ///
    /// * `source` - The source node ID
    /// * `port` - The output port (typically 0)
    ///
    /// # Panics
    ///
    /// Panics if the computed index is out of bounds.
    #[inline]
    #[must_use]
    pub fn route(&self, source: NodeId, port: u8) -> &RoutingEntry {
        let idx = source.0 as usize * MAX_PORTS + port as usize;
        &self.routes[idx]
    }

    /// Convenience method to get targets for a node's primary output (port 0).
    #[inline]
    #[must_use]
    pub fn node_targets(&self, source: NodeId) -> &RoutingEntry {
        self.route(source, 0)
    }

    /// Returns the total number of routing entries in the table.
    #[inline]
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.routes.len()
    }

    /// Returns the maximum node ID covered by the table.
    #[inline]
    #[must_use]
    pub fn max_node_id(&self) -> u32 {
        self.max_node_id
    }
}
