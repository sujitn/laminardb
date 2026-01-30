//! Unit tests for DAG topology and builder.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use super::builder::DagBuilder;
use super::error::DagError;
use super::topology::*;

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
