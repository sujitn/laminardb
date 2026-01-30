//! Unit tests for DAG topology, builder, multicast, routing, and executor.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use super::builder::DagBuilder;
use super::error::DagError;
use super::executor::DagExecutor;
use super::multicast::MulticastBuffer;
use super::routing::{RoutingEntry, RoutingTable};
use super::topology::*;
use crate::operator::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};

/// Helper to create a simple int64 schema.
fn int_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]))
}

/// Helper to create a schema with two int64 fields.
fn two_field_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Helper to create a float64 schema (incompatible with int_schema).
fn float_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]))
}

/// Helper to create an empty schema (type-erased, compatible with anything).
fn empty_schema() -> Arc<Schema> {
    Arc::new(Schema::empty())
}

// ---- StreamingDag direct tests ----

#[test]
fn test_empty_dag() {
    let dag = StreamingDag::new();
    assert_eq!(dag.node_count(), 0);
    assert_eq!(dag.edge_count(), 0);
    assert!(dag.sources().is_empty());
    assert!(dag.sinks().is_empty());
    assert!(dag.execution_order().is_empty());
    assert!(!dag.is_finalized());
}

#[test]
fn test_empty_dag_finalize_error() {
    let mut dag = StreamingDag::new();
    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::EmptyDag)));
}

#[test]
fn test_add_single_node() {
    let mut dag = StreamingDag::new();
    let id = dag
        .add_node("source", DagNodeType::Source, int_schema())
        .unwrap();
    assert_eq!(id, NodeId(0));
    assert_eq!(dag.node_count(), 1);
    assert_eq!(dag.node_id_by_name("source"), Some(NodeId(0)));
    assert_eq!(dag.node_name(NodeId(0)), Some("source".to_string()));
}

#[test]
fn test_duplicate_node_error() {
    let mut dag = StreamingDag::new();
    dag.add_node("src", DagNodeType::Source, int_schema())
        .unwrap();
    let result = dag.add_node("src", DagNodeType::Source, int_schema());
    assert!(matches!(result, Err(DagError::DuplicateNode(_))));
}

#[test]
fn test_add_edge_node_not_found() {
    let mut dag = StreamingDag::new();
    let id = dag
        .add_node("a", DagNodeType::Source, int_schema())
        .unwrap();
    let result = dag.add_edge(id, NodeId(99));
    assert!(matches!(result, Err(DagError::NodeNotFound(_))));
}

#[test]
fn test_self_loop_detection() {
    let mut dag = StreamingDag::new();
    let id = dag
        .add_node("a", DagNodeType::StatefulOperator, int_schema())
        .unwrap();
    let result = dag.add_edge(id, id);
    assert!(matches!(result, Err(DagError::CycleDetected(_))));
}

// ---- DagBuilder tests ----

#[test]
fn test_linear_dag() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .sink("snk", schema.clone())
        .connect("src", "a")
        .connect("a", "b")
        .connect("b", "snk")
        .build()
        .unwrap();

    assert_eq!(dag.node_count(), 4);
    assert_eq!(dag.edge_count(), 3);
    assert!(dag.is_finalized());

    // All edges should be SPSC in a linear DAG
    for edge in dag.edges().values() {
        assert_eq!(edge.channel_type, DagChannelType::Spsc);
    }

    // Source and sink classification
    assert_eq!(dag.sources().len(), 1);
    assert_eq!(dag.sinks().len(), 1);

    // Topological order: src must come before a, a before b, b before snk
    let order = dag.execution_order();
    assert_eq!(order.len(), 4);
    let pos = |name: &str| {
        order
            .iter()
            .position(|&id| dag.node_name(id).unwrap() == name)
            .unwrap()
    };
    assert!(pos("src") < pos("a"));
    assert!(pos("a") < pos("b"));
    assert!(pos("b") < pos("snk"));

    // No shared stages
    assert!(dag.shared_stages().is_empty());
}

#[test]
fn test_fan_out_dag() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone())
                .branch("b", schema.clone())
                .branch("c", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .sink_for("c", "sink_c", schema.clone())
        .build()
        .unwrap();

    // src, shared, a, b, c, sink_a, sink_b, sink_c = 8
    assert_eq!(dag.node_count(), 8);
    assert_eq!(dag.sources().len(), 1);
    assert_eq!(dag.sinks().len(), 3);

    // shared has fan-out > 1, so it should be a shared stage
    let shared_id = dag.node_id_by_name("shared").unwrap();
    assert!(dag.shared_stages().contains_key(&shared_id));
    let meta = &dag.shared_stages()[&shared_id];
    assert_eq!(meta.consumer_count, 3);

    // Edges from shared to a/b/c should be SPMC
    let shared_node = dag.node(shared_id).unwrap();
    for &edge_id in &shared_node.outputs {
        let edge = dag.edge(edge_id).unwrap();
        assert_eq!(edge.channel_type, DagChannelType::Spmc);
    }
}

#[test]
fn test_fan_in_dag() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("a", schema.clone())
        .source("b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("a", "merge")
        .connect("b", "merge")
        .build()
        .unwrap();

    assert_eq!(dag.node_count(), 4);
    assert_eq!(dag.sources().len(), 2);
    assert_eq!(dag.sinks().len(), 1);

    // Edges to merge should be MPSC (fan-in)
    let merge_id = dag.node_id_by_name("merge").unwrap();
    let merge_node = dag.node(merge_id).unwrap();
    assert_eq!(merge_node.inputs.len(), 2);
    for &edge_id in &merge_node.inputs {
        let edge = dag.edge(edge_id).unwrap();
        assert_eq!(edge.channel_type, DagChannelType::Mpsc);
    }
}

#[test]
fn test_diamond_dag() {
    let schema = int_schema();

    // Diamond: src -> {a, b} -> merge -> snk
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("merge", schema.clone())
        .connect("a", "merge")
        .connect("b", "merge")
        .sink_for("merge", "snk", schema.clone())
        .build()
        .unwrap();

    assert_eq!(dag.node_count(), 5);
    assert_eq!(dag.sources().len(), 1);
    assert_eq!(dag.sinks().len(), 1);

    // Topological order: src before a and b, a and b before merge, merge before snk
    let order = dag.execution_order();
    let pos = |name: &str| {
        order
            .iter()
            .position(|&id| dag.node_name(id).unwrap() == name)
            .unwrap()
    };
    assert!(pos("src") < pos("a"));
    assert!(pos("src") < pos("b"));
    assert!(pos("a") < pos("merge"));
    assert!(pos("b") < pos("merge"));
    assert!(pos("merge") < pos("snk"));

    // src has fan-out > 1, so it should be shared
    let src_id = dag.node_id_by_name("src").unwrap();
    assert!(dag.shared_stages().contains_key(&src_id));

    // Edges from src to a/b should be SPMC (fan-out from src)
    let src_node = dag.node(src_id).unwrap();
    for &edge_id in &src_node.outputs {
        let edge = dag.edge(edge_id).unwrap();
        assert_eq!(edge.channel_type, DagChannelType::Spmc);
    }

    // Edges from a/b to merge should be MPSC (fan-in to merge)
    let merge_id = dag.node_id_by_name("merge").unwrap();
    let merge_node = dag.node(merge_id).unwrap();
    for &edge_id in &merge_node.inputs {
        let edge = dag.edge(edge_id).unwrap();
        assert_eq!(edge.channel_type, DagChannelType::Mpsc);
    }
}

#[test]
fn test_cycle_detection() {
    // Build a DAG where A -> B -> C, then try to add C -> A
    let schema = int_schema();
    let mut dag = StreamingDag::new();
    let a = dag
        .add_node("a", DagNodeType::StatefulOperator, schema.clone())
        .unwrap();
    let b = dag
        .add_node("b", DagNodeType::StatefulOperator, schema.clone())
        .unwrap();
    let c = dag
        .add_node("c", DagNodeType::StatefulOperator, schema.clone())
        .unwrap();
    dag.add_edge(a, b).unwrap();
    dag.add_edge(b, c).unwrap();
    dag.add_edge(c, a).unwrap(); // Creates a cycle

    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::CycleDetected(_))));
}

#[test]
fn test_disconnected_node_source_no_outputs() {
    let schema = int_schema();
    let mut dag = StreamingDag::new();
    dag.add_node("src", DagNodeType::Source, schema.clone())
        .unwrap();
    // Source with no outputs is disconnected
    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::DisconnectedNode(_))));
}

#[test]
fn test_disconnected_node_sink_no_inputs() {
    let schema = int_schema();
    let mut dag = StreamingDag::new();
    dag.add_node("snk", DagNodeType::Sink, schema.clone())
        .unwrap();
    // Sink with no inputs is disconnected
    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::DisconnectedNode(_))));
}

#[test]
fn test_disconnected_operator() {
    let schema = int_schema();
    let mut dag = StreamingDag::new();
    let src = dag
        .add_node("src", DagNodeType::Source, schema.clone())
        .unwrap();
    let snk = dag
        .add_node("snk", DagNodeType::Sink, schema.clone())
        .unwrap();
    dag.add_edge(src, snk).unwrap();

    // Add a disconnected operator
    dag.add_node("orphan", DagNodeType::StatefulOperator, schema)
        .unwrap();

    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::DisconnectedNode(_))));
}

#[test]
fn test_builder_node_not_found() {
    let schema = int_schema();
    let result = DagBuilder::new()
        .source("src", schema.clone())
        .connect("src", "nonexistent")
        .build();
    assert!(matches!(result, Err(DagError::NodeNotFound(_))));
}

#[test]
fn test_topological_order_complex() {
    //        src
    //       / | \
    //      a  b  c
    //       \ | /
    //       merge
    //         |
    //        snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("c", schema.clone())
        .operator("merge", schema.clone())
        .sink("snk", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .connect("src", "c")
        .connect("a", "merge")
        .connect("b", "merge")
        .connect("c", "merge")
        .connect("merge", "snk")
        .build()
        .unwrap();

    let order = dag.execution_order();
    assert_eq!(order.len(), 6);

    let pos = |name: &str| {
        order
            .iter()
            .position(|&id| dag.node_name(id).unwrap() == name)
            .unwrap()
    };

    // src must be first (only source)
    assert_eq!(pos("src"), 0);
    // a, b, c must come after src and before merge
    assert!(pos("a") < pos("merge"));
    assert!(pos("b") < pos("merge"));
    assert!(pos("c") < pos("merge"));
    // snk must be last
    assert!(pos("merge") < pos("snk"));
}

#[test]
fn test_channel_type_derivation() {
    let schema = int_schema();

    // src -> shared -> {a, b}  AND  {c, d} -> merge -> snk
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone()).branch("b", schema.clone())
        })
        .source("c", schema.clone())
        .source("d", schema.clone())
        .operator("merge", schema.clone())
        .connect("c", "merge")
        .connect("d", "merge")
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .sink_for("merge", "sink_merge", schema.clone())
        .build()
        .unwrap();

    // SPSC: src -> shared (1 output, 1 input)
    let src_id = dag.node_id_by_name("src").unwrap();
    let src_node = dag.node(src_id).unwrap();
    let src_edge = dag.edge(src_node.outputs[0]).unwrap();
    assert_eq!(src_edge.channel_type, DagChannelType::Spsc);

    // SPMC: shared -> a, shared -> b (fan-out = 2)
    let shared_id = dag.node_id_by_name("shared").unwrap();
    let shared_node = dag.node(shared_id).unwrap();
    for &eid in &shared_node.outputs {
        assert_eq!(dag.edge(eid).unwrap().channel_type, DagChannelType::Spmc);
    }

    // MPSC: c -> merge, d -> merge (fan-in = 2)
    let merge_id = dag.node_id_by_name("merge").unwrap();
    let merge_node = dag.node(merge_id).unwrap();
    for &eid in &merge_node.inputs {
        assert_eq!(dag.edge(eid).unwrap().channel_type, DagChannelType::Mpsc);
    }
}

#[test]
fn test_shared_stage_detection() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone()).branch("b", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .build()
        .unwrap();

    // Only "shared" should be a shared stage (fan-out > 1)
    assert_eq!(dag.shared_stages().len(), 1);
    let shared_id = dag.node_id_by_name("shared").unwrap();
    assert!(dag.shared_stages().contains_key(&shared_id));

    let meta = &dag.shared_stages()[&shared_id];
    assert_eq!(meta.consumer_count, 2);
    assert_eq!(meta.producer_node, shared_id);
}

#[test]
fn test_source_sink_classification() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("s1", schema.clone())
        .source("s2", schema.clone())
        .operator("op", schema.clone())
        .sink("k1", schema.clone())
        .sink("k2", schema.clone())
        .connect("s1", "op")
        .connect("s2", "op")
        .connect("op", "k1")
        .connect("op", "k2")
        .build()
        .unwrap();

    assert_eq!(dag.sources().len(), 2);
    assert_eq!(dag.sinks().len(), 2);

    // Source nodes should have no inputs
    for &src_id in dag.sources() {
        assert_eq!(dag.incoming_edge_count(src_id), 0);
    }

    // Sink nodes should have no outputs
    for &snk_id in dag.sinks() {
        assert_eq!(dag.outgoing_edge_count(snk_id), 0);
    }
}

#[test]
fn test_fan_out_builder() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .connect("src", "shared")
        .operator("shared", schema.clone())
        .fan_out("shared", |b| {
            b.branch("x", schema.clone())
                .branch("y", schema.clone())
                .stateless_branch("z", schema.clone())
        })
        .sink_for("x", "sx", schema.clone())
        .sink_for("y", "sy", schema.clone())
        .sink_for("z", "sz", schema.clone())
        .build()
        .unwrap();

    // Verify stateless branch is StatelessOperator
    let z_id = dag.node_id_by_name("z").unwrap();
    let z_node = dag.node(z_id).unwrap();
    assert_eq!(z_node.node_type, DagNodeType::StatelessOperator);

    // Verify stateful branches are StatefulOperator
    let x_id = dag.node_id_by_name("x").unwrap();
    let x_node = dag.node(x_id).unwrap();
    assert_eq!(x_node.node_type, DagNodeType::StatefulOperator);
}

#[test]
fn test_max_fan_out_limit() {
    let schema = int_schema();
    let mut dag = StreamingDag::new();
    let src = dag
        .add_node("src", DagNodeType::Source, schema.clone())
        .unwrap();

    // Add MAX_FAN_OUT + 1 targets
    for i in 0..=MAX_FAN_OUT {
        let name = format!("t{i}");
        let target = dag
            .add_node(&name, DagNodeType::Sink, schema.clone())
            .unwrap();
        dag.add_edge(src, target).unwrap();
    }

    let result = dag.finalize();
    assert!(matches!(
        result,
        Err(DagError::FanOutLimitExceeded { .. })
    ));
}

#[test]
fn test_schema_compatibility_pass() {
    // Same schemas should be compatible
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    assert!(dag.validate().is_ok());
}

#[test]
fn test_schema_compatibility_empty_is_compatible() {
    // Empty schema (type-erased) is compatible with anything
    let dag = DagBuilder::new()
        .source("src", empty_schema())
        .sink_for("src", "snk", int_schema())
        .build()
        .unwrap();

    assert!(dag.validate().is_ok());
}

#[test]
fn test_schema_mismatch_field_count() {
    let dag = DagBuilder::new()
        .source("src", int_schema())
        .sink_for("src", "snk", two_field_schema())
        .build()
        .unwrap();

    let result = dag.validate();
    assert!(matches!(result, Err(DagError::SchemaMismatch { .. })));
}

#[test]
fn test_schema_mismatch_type() {
    let dag = DagBuilder::new()
        .source("src", int_schema())
        .sink_for("src", "snk", float_schema())
        .build()
        .unwrap();

    let result = dag.validate();
    assert!(matches!(result, Err(DagError::SchemaMismatch { .. })));
}

#[test]
fn test_materialized_view_node() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .materialized_view("mv1", schema.clone())
        .connect("src", "mv1")
        .sink_for("mv1", "snk", schema.clone())
        .build()
        .unwrap();

    let mv_id = dag.node_id_by_name("mv1").unwrap();
    let mv_node = dag.node(mv_id).unwrap();
    assert_eq!(mv_node.node_type, DagNodeType::MaterializedView);
}

#[test]
fn test_edge_ports() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("s1", schema.clone())
        .source("s2", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("s1", "op")
        .connect("s2", "op")
        .build()
        .unwrap();

    // op has two inputs, so the target_port should differ
    let op_id = dag.node_id_by_name("op").unwrap();
    let op_node = dag.node(op_id).unwrap();
    let ports: Vec<u8> = op_node
        .inputs
        .iter()
        .map(|&eid| dag.edge(eid).unwrap().target_port)
        .collect();
    assert_eq!(ports.len(), 2);
    // Ports should be 0, 1 (assigned incrementally)
    assert!(ports.contains(&0));
    assert!(ports.contains(&1));
}

#[test]
fn test_default_builder() {
    let builder = DagBuilder::default();
    let schema = int_schema();
    let dag = builder
        .source("src", schema.clone())
        .sink_for("src", "snk", schema)
        .build()
        .unwrap();
    assert_eq!(dag.node_count(), 2);
}

#[test]
fn test_dag_debug_format() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema)
        .build()
        .unwrap();

    let debug = format!("{dag:?}");
    assert!(debug.contains("StreamingDag"));
    assert!(debug.contains("node_count: 2"));
}

// ---- MulticastBuffer tests ----

#[test]
fn test_multicast_single_consumer() {
    let buf = MulticastBuffer::new(4, 1);
    buf.publish(42u64).unwrap();
    let val = buf.consume(0);
    assert_eq!(val, Some(42));
}

#[test]
fn test_multicast_multiple_consumers() {
    let buf = MulticastBuffer::new(4, 3);
    buf.publish(100u64).unwrap();

    // All 3 consumers should read the same value.
    assert_eq!(buf.consume(0), Some(100));
    assert_eq!(buf.consume(1), Some(100));
    assert_eq!(buf.consume(2), Some(100));
}

#[test]
fn test_multicast_backpressure() {
    let buf = MulticastBuffer::new(2, 1);
    buf.publish(1u64).unwrap();
    buf.publish(2u64).unwrap();

    // Buffer full (2 slots, neither consumed).
    let result = buf.publish(3u64);
    assert!(result.is_err());
}

#[test]
fn test_multicast_slot_reuse() {
    let buf = MulticastBuffer::new(2, 1);
    buf.publish(1u64).unwrap();
    buf.publish(2u64).unwrap();

    // Consume first slot to free it.
    assert_eq!(buf.consume(0), Some(1));

    // Slot 0 should be free now, publish should succeed.
    buf.publish(3u64).unwrap();

    // Consume remaining values in order.
    assert_eq!(buf.consume(0), Some(2));
    assert_eq!(buf.consume(0), Some(3));
}

#[test]
fn test_multicast_partial_consume() {
    let buf = MulticastBuffer::new(2, 2);
    buf.publish(10u64).unwrap();

    // Only consumer 0 reads.
    assert_eq!(buf.consume(0), Some(10));

    // Slot still in use by consumer 1.
    buf.publish(20u64).unwrap();

    // Buffer full: consumer 1 hasn't freed slot 0.
    let result = buf.publish(30u64);
    assert!(result.is_err());

    // Consumer 1 reads, freeing slot 0.
    assert_eq!(buf.consume(1), Some(10));

    // Now publish should succeed.
    buf.publish(30u64).unwrap();
}

#[test]
fn test_multicast_wrap_around() {
    let buf = MulticastBuffer::new(2, 1);

    for i in 0u64..10 {
        buf.publish(i).unwrap();
        assert_eq!(buf.consume(0), Some(i));
    }

    assert_eq!(buf.write_position(), 10);
    assert_eq!(buf.read_position(0), 10);
}

#[test]
fn test_multicast_empty_consume() {
    let buf: MulticastBuffer<u64> = MulticastBuffer::new(4, 2);

    // No data published, consume should return None.
    assert_eq!(buf.consume(0), None);
    assert_eq!(buf.consume(1), None);
}

#[test]
fn test_multicast_accessors() {
    let buf: MulticastBuffer<u64> = MulticastBuffer::new(8, 3);
    assert_eq!(buf.capacity(), 8);
    assert_eq!(buf.consumer_count(), 3);
    assert_eq!(buf.write_position(), 0);
    assert_eq!(buf.read_position(0), 0);
    assert_eq!(buf.read_position(1), 0);
    assert_eq!(buf.read_position(2), 0);

    // Debug format should work.
    let debug = format!("{buf:?}");
    assert!(debug.contains("MulticastBuffer"));
}

#[test]
fn test_multicast_sequential_values() {
    let buf = MulticastBuffer::new(4, 2);

    buf.publish(10u64).unwrap();
    buf.publish(20u64).unwrap();
    buf.publish(30u64).unwrap();

    // Consumer 0 reads all three.
    assert_eq!(buf.consume(0), Some(10));
    assert_eq!(buf.consume(0), Some(20));
    assert_eq!(buf.consume(0), Some(30));
    assert_eq!(buf.consume(0), None);

    // Consumer 1 also reads all three.
    assert_eq!(buf.consume(1), Some(10));
    assert_eq!(buf.consume(1), Some(20));
    assert_eq!(buf.consume(1), Some(30));
    assert_eq!(buf.consume(1), None);
}

// ---- RoutingTable tests ----

#[test]
fn test_routing_table_build() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);
    assert!(table.entry_count() > 0);
}

#[test]
fn test_routing_table_linear() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);

    // src -> op: single target, not multicast.
    let src_id = dag.node_id_by_name("src").unwrap();
    let entry = table.node_targets(src_id);
    assert_eq!(entry.target_count, 1);
    assert!(!entry.is_multicast);
    assert_eq!(entry.targets[0], dag.node_id_by_name("op").unwrap().0);

    // op -> snk: single target, not multicast.
    let op_id = dag.node_id_by_name("op").unwrap();
    let entry = table.node_targets(op_id);
    assert_eq!(entry.target_count, 1);
    assert!(!entry.is_multicast);
    assert_eq!(entry.targets[0], dag.node_id_by_name("snk").unwrap().0);
}

#[test]
fn test_routing_table_fan_out() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone())
                .branch("b", schema.clone())
                .branch("c", schema.clone())
        })
        .sink_for("a", "sa", schema.clone())
        .sink_for("b", "sb", schema.clone())
        .sink_for("c", "sc", schema.clone())
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);

    // shared -> {a, b, c}: multicast with 3 targets.
    let shared_id = dag.node_id_by_name("shared").unwrap();
    let entry = table.node_targets(shared_id);
    assert_eq!(entry.target_count, 3);
    assert!(entry.is_multicast);

    // Check targets include a, b, c.
    let target_ids: Vec<u32> = entry.target_ids().to_vec();
    assert!(target_ids.contains(&dag.node_id_by_name("a").unwrap().0));
    assert!(target_ids.contains(&dag.node_id_by_name("b").unwrap().0));
    assert!(target_ids.contains(&dag.node_id_by_name("c").unwrap().0));
}

#[test]
fn test_routing_table_terminal() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);

    // Sink node has no targets (terminal).
    let snk_id = dag.node_id_by_name("snk").unwrap();
    let entry = table.node_targets(snk_id);
    assert_eq!(entry.target_count, 0);
    assert!(!entry.is_multicast);
    assert!(entry.is_terminal());
}

#[test]
fn test_routing_entry_cache_alignment() {
    assert_eq!(std::mem::size_of::<RoutingEntry>(), 64);
    assert_eq!(std::mem::align_of::<RoutingEntry>(), 64);
}

#[test]
fn test_routing_table_diamond() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .connect("a", "merge")
        .connect("b", "merge")
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);

    // src -> {a, b}: multicast.
    let src_id = dag.node_id_by_name("src").unwrap();
    let entry = table.node_targets(src_id);
    assert_eq!(entry.target_count, 2);
    assert!(entry.is_multicast);

    // a -> merge: single target.
    let a_id = dag.node_id_by_name("a").unwrap();
    let entry = table.node_targets(a_id);
    assert_eq!(entry.target_count, 1);
    assert!(!entry.is_multicast);
    assert_eq!(entry.targets[0], dag.node_id_by_name("merge").unwrap().0);

    // merge -> snk: single target.
    let merge_id = dag.node_id_by_name("merge").unwrap();
    let entry = table.node_targets(merge_id);
    assert_eq!(entry.target_count, 1);
    assert!(!entry.is_multicast);
}

#[test]
fn test_routing_table_max_node_id() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("s", schema.clone())
        .sink_for("s", "k", schema.clone())
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);
    // Node IDs are 0 and 1, so max_node_id should be 1.
    assert_eq!(table.max_node_id(), 1);
}

// ---- DagExecutor tests ----

/// Helper to create a test event with the given timestamp and a single-column payload.
fn test_event(timestamp: i64, value: i64) -> Event {
    let array = Arc::new(Int64Array::from(vec![value]));
    let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
    Event {
        timestamp,
        data: batch,
    }
}

/// Extract the i64 payload value from a test event.
fn event_value(event: &Event) -> i64 {
    let col = event
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    col.value(0)
}

/// A passthrough operator that forwards events unchanged.
struct PassthroughOperator;

impl Operator for PassthroughOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let mut v = OutputVec::new();
        v.push(Output::Event(event.clone()));
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "passthrough".to_string(),
            data: Vec::new(),
        }
    }
    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

/// A doubling operator that multiplies the event value by 2.
struct DoublingOperator;

impl Operator for DoublingOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let val = event_value(event);
        let mut v = OutputVec::new();
        v.push(Output::Event(test_event(event.timestamp, val * 2)));
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "doubling".to_string(),
            data: Vec::new(),
        }
    }
    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

/// A filter operator that drops events where value <= threshold.
struct FilterOperator {
    threshold: i64,
}

impl Operator for FilterOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let val = event_value(event);
        let mut v = OutputVec::new();
        if val > self.threshold {
            v.push(Output::Event(event.clone()));
        }
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "filter".to_string(),
            data: self.threshold.to_le_bytes().to_vec(),
        }
    }
    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError> {
        if state.data.len() == 8 {
            self.threshold = i64::from_le_bytes(state.data.try_into().unwrap());
        }
        Ok(())
    }
}

/// An adding operator that adds a constant to the event value.
struct AddOperator {
    addend: i64,
}

impl Operator for AddOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let val = event_value(event);
        let mut v = OutputVec::new();
        v.push(Output::Event(test_event(event.timestamp, val + self.addend)));
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "add".to_string(),
            data: self.addend.to_le_bytes().to_vec(),
        }
    }
    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

#[test]
fn test_executor_linear_dag() {
    // src -> op -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 42)).unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 42);
    assert_eq!(outputs[0].timestamp, 1000);
}

#[test]
fn test_executor_fan_out() {
    // src -> shared -> {a, b} -> {sink_a, sink_b}
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone())
                .branch("b", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let sink_a_id = dag.node_id_by_name("sink_a").unwrap();
    let sink_b_id = dag.node_id_by_name("sink_b").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 99)).unwrap();

    // Both sinks should receive the event.
    let out_a = executor.take_sink_outputs(sink_a_id);
    let out_b = executor.take_sink_outputs(sink_b_id);
    assert_eq!(out_a.len(), 1);
    assert_eq!(out_b.len(), 1);
    assert_eq!(event_value(&out_a[0]), 99);
    assert_eq!(event_value(&out_b[0]), 99);
}

#[test]
fn test_executor_fan_in() {
    // {src_a, src_b} -> merge -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src_a", schema.clone())
        .source("src_b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src_a", "merge")
        .connect("src_b", "merge")
        .build()
        .unwrap();

    let src_a_id = dag.node_id_by_name("src_a").unwrap();
    let src_b_id = dag.node_id_by_name("src_b").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor
        .process_event(src_a_id, test_event(1000, 10))
        .unwrap();
    executor
        .process_event(src_b_id, test_event(2000, 20))
        .unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 2);
    let values: Vec<i64> = outputs.iter().map(event_value).collect();
    assert!(values.contains(&10));
    assert!(values.contains(&20));
}

#[test]
fn test_executor_diamond() {
    // src -> {a, b} -> merge -> snk
    // a doubles, b adds 100
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .connect("a", "merge")
        .connect("b", "merge")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let a_id = dag.node_id_by_name("a").unwrap();
    let b_id = dag.node_id_by_name("b").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(a_id, Box::new(DoublingOperator));
    executor.register_operator(b_id, Box::new(AddOperator { addend: 100 }));

    executor.process_event(src_id, test_event(1000, 5)).unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 2);

    let values: Vec<i64> = outputs.iter().map(event_value).collect();
    // a doubles: 5 * 2 = 10
    // b adds 100: 5 + 100 = 105
    assert!(values.contains(&10));
    assert!(values.contains(&105));
}

#[test]
fn test_executor_operator_integration() {
    // src -> double -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("double", schema.clone())
        .sink_for("double", "snk", schema.clone())
        .connect("src", "double")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let double_id = dag.node_id_by_name("double").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(double_id, Box::new(DoublingOperator));

    executor.process_event(src_id, test_event(1000, 7)).unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 14); // 7 * 2
}

#[test]
fn test_executor_passthrough_no_operator() {
    // Without a registered operator, nodes act as passthrough.
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    // No operator registered for "op" — passthrough behavior.
    executor.process_event(src_id, test_event(500, 33)).unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 33);
}

#[test]
fn test_executor_filter_operator() {
    // src -> filter(threshold=50) -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("filter", schema.clone())
        .sink_for("filter", "snk", schema.clone())
        .connect("src", "filter")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let filter_id = dag.node_id_by_name("filter").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(filter_id, Box::new(FilterOperator { threshold: 50 }));

    // Event with value 30 should be dropped.
    executor
        .process_event(src_id, test_event(1000, 30))
        .unwrap();
    let outputs = executor.take_sink_outputs(snk_id);
    assert!(outputs.is_empty());

    // Event with value 70 should pass through.
    executor
        .process_event(src_id, test_event(2000, 70))
        .unwrap();
    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 70);
}

#[test]
fn test_executor_metrics_tracking() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 1)).unwrap();

    let m = executor.metrics();
    // Three nodes processed: src, op, snk.
    // src and op each process 1 event = 2 events_processed.
    // snk gets collected directly, not processed through operator.
    assert!(m.events_processed >= 2);
    // src routes to op, op routes to snk.
    assert!(m.events_routed >= 1);
    // snk has no input queue events to skip (it gets events).
    // The executor processes in topological order, some nodes have empty queues.
    // In a linear DAG with one event, all nodes receive it, so nodes_skipped may be 0.
}

#[test]
fn test_executor_metrics_reset() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 1)).unwrap();

    assert!(executor.metrics().events_processed > 0);
    executor.reset_metrics();
    assert_eq!(executor.metrics().events_processed, 0);
    assert_eq!(executor.metrics().events_routed, 0);
}

#[test]
fn test_executor_empty_queue_skip() {
    // Fan-in: {src_a, src_b} -> merge -> snk
    // Only inject into src_a. src_b has no events, so it gets skipped.
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src_a", schema.clone())
        .source("src_b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src_a", "merge")
        .connect("src_b", "merge")
        .build()
        .unwrap();

    let src_a_id = dag.node_id_by_name("src_a").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor
        .process_event(src_a_id, test_event(1000, 1))
        .unwrap();

    // src_b had no events, so it should have been skipped.
    let m = executor.metrics();
    assert!(m.nodes_skipped > 0);
}

#[test]
fn test_executor_multiple_events() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("double", schema.clone())
        .sink_for("double", "snk", schema.clone())
        .connect("src", "double")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let double_id = dag.node_id_by_name("double").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(double_id, Box::new(DoublingOperator));

    // Process three events.
    for i in 1..=3 {
        executor
            .process_event(src_id, test_event(i * 1000, i * 10))
            .unwrap();
    }

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 3);
    assert_eq!(event_value(&outputs[0]), 20); // 10 * 2
    assert_eq!(event_value(&outputs[1]), 40); // 20 * 2
    assert_eq!(event_value(&outputs[2]), 60); // 30 * 2
}

#[test]
fn test_executor_node_not_found() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    let result = executor.process_event(NodeId(999), test_event(1000, 1));
    assert!(result.is_err());
}

#[test]
fn test_executor_take_all_sink_outputs() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone())
                .branch("b", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 55)).unwrap();

    let all_outputs = executor.take_all_sink_outputs();
    assert_eq!(all_outputs.len(), 2);

    // Both sinks should have received the event.
    for (_sink_id, events) in &all_outputs {
        assert_eq!(events.len(), 1);
        assert_eq!(event_value(&events[0]), 55);
    }
}

#[test]
fn test_executor_source_and_sink_accessors() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("s1", schema.clone())
        .source("s2", schema.clone())
        .operator("op", schema.clone())
        .sink("k1", schema.clone())
        .connect("s1", "op")
        .connect("s2", "op")
        .connect("op", "k1")
        .build()
        .unwrap();

    let executor = DagExecutor::from_dag(&dag);
    assert_eq!(executor.source_nodes().len(), 2);
    assert_eq!(executor.sink_nodes().len(), 1);
}

#[test]
fn test_executor_node_type() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let op_id = dag.node_id_by_name("op").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let executor = DagExecutor::from_dag(&dag);
    assert_eq!(executor.node_type(src_id), Some(DagNodeType::Source));
    assert_eq!(
        executor.node_type(op_id),
        Some(DagNodeType::StatefulOperator)
    );
    assert_eq!(executor.node_type(snk_id), Some(DagNodeType::Sink));
    assert_eq!(executor.node_type(NodeId(999)), None);
}

#[test]
fn test_executor_checkpoint() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let op_id = dag.node_id_by_name("op").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(op_id, Box::new(PassthroughOperator));

    let states = executor.checkpoint();
    assert_eq!(states.len(), 1);
    assert!(states.contains_key(&op_id));
    assert_eq!(states[&op_id].operator_id, "passthrough");
}

#[test]
fn test_executor_multicast_metrics() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone())
                .branch("b", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 1)).unwrap();

    // shared -> {a, b} is a multicast.
    assert!(executor.metrics().multicast_publishes > 0);
}

#[test]
fn test_executor_chained_operators() {
    // src -> double -> add(100) -> snk
    // Input: 5 -> double: 10 -> add(100): 110
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("double", schema.clone())
        .operator("add", schema.clone())
        .sink_for("add", "snk", schema.clone())
        .connect("src", "double")
        .connect("double", "add")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let double_id = dag.node_id_by_name("double").unwrap();
    let add_id = dag.node_id_by_name("add").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(double_id, Box::new(DoublingOperator));
    executor.register_operator(add_id, Box::new(AddOperator { addend: 100 }));

    executor.process_event(src_id, test_event(1000, 5)).unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 110); // (5 * 2) + 100
}

#[test]
fn test_executor_take_sink_outputs_drains() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 1)).unwrap();

    // First take returns the event.
    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);

    // Second take should be empty (already drained).
    let outputs = executor.take_sink_outputs(snk_id);
    assert!(outputs.is_empty());
}

#[test]
fn test_executor_debug_format() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let executor = DagExecutor::from_dag(&dag);
    let debug = format!("{executor:?}");
    assert!(debug.contains("DagExecutor"));
    assert!(debug.contains("slot_count"));
}

#[test]
fn test_executor_complex_dag_filter_and_transform() {
    // Complex DAG:
    //   src -> filter(>50) -> double -> snk
    //
    // Input values: 30, 60, 40, 80
    // After filter(>50): 60, 80
    // After double: 120, 160
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("filter", schema.clone())
        .operator("double", schema.clone())
        .sink_for("double", "snk", schema.clone())
        .connect("src", "filter")
        .connect("filter", "double")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let filter_id = dag.node_id_by_name("filter").unwrap();
    let double_id = dag.node_id_by_name("double").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(filter_id, Box::new(FilterOperator { threshold: 50 }));
    executor.register_operator(double_id, Box::new(DoublingOperator));

    for (i, val) in [30, 60, 40, 80].iter().enumerate() {
        executor
            .process_event(src_id, test_event((i as i64 + 1) * 1000, *val))
            .unwrap();
    }

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 2);
    assert_eq!(event_value(&outputs[0]), 120); // 60 * 2
    assert_eq!(event_value(&outputs[1]), 160); // 80 * 2
}
