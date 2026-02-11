//! Plan compiler infrastructure for Ring 0 event processing.
//!
//! This module provides the foundation for compiling `DataFusion` logical plans
//! into native functions that operate on a fixed-layout row format.
//!
//! # Components
//!
//! - [`row`]: Fixed-layout [`EventRow`] format with zero-copy field access
//! - [`bridge`]: [`RowBatchBridge`] for converting rows back to Arrow `RecordBatch`
//! - [`policy`]: [`BatchPolicy`] and [`BackpressureStrategy`] for bridge configuration
//! - [`pipeline_bridge`]: Ring 0 / Ring 1 SPSC bridge with watermark-aware batching
//! - [`metrics`]: Query lifecycle types (always available, not JIT-gated)
//!
//! ## JIT Compilation (requires `jit` feature)
//!
//! - [`error`]: Error types and compiled function pointer wrappers
//! - [`expr`]: Cranelift-based expression compiler
//! - [`fold`]: Constant folding pre-pass
//! - [`jit`]: Cranelift JIT context management
//! - [`pipeline`]: Pipeline types and compiled pipeline wrapper
//! - [`extractor`]: Pipeline extraction from `DataFusion` logical plans
//! - [`pipeline_compiler`]: Cranelift codegen for fused pipelines
//! - [`cache`]: Compiler cache for compiled pipelines
//! - [`fallback`]: Fallback mechanism for uncompilable pipelines
//! - [`query`]: [`StreamingQuery`](query::StreamingQuery) lifecycle management

pub mod bridge;
pub mod metrics;
pub mod pipeline_bridge;
pub mod policy;
pub mod row;

#[cfg(feature = "jit")]
pub mod cache;
#[cfg(feature = "jit")]
pub mod error;
#[cfg(feature = "jit")]
pub mod expr;
#[cfg(feature = "jit")]
pub mod extractor;
#[cfg(feature = "jit")]
pub mod fallback;
#[cfg(feature = "jit")]
pub mod fold;
#[cfg(feature = "jit")]
pub mod jit;
#[cfg(feature = "jit")]
pub mod pipeline;
#[cfg(feature = "jit")]
pub mod pipeline_compiler;
#[cfg(feature = "jit")]
pub mod query;

pub use bridge::{BridgeError, RowBatchBridge};
pub use metrics::{
    QueryConfig, QueryError, QueryId, QueryMetadata, QueryMetrics, QueryState, StateStoreConfig,
    SubmitResult,
};
pub use pipeline_bridge::{
    BridgeConsumer, BridgeMessage, BridgeStats, BridgeStatsSnapshot, PipelineBridge,
    PipelineBridgeError, Ring1Action, create_pipeline_bridge,
};
pub use policy::{BackpressureStrategy, BatchPolicy};
pub use row::{EventRow, FieldLayout, FieldType, MutableEventRow, RowError, RowSchema};

#[cfg(feature = "jit")]
pub use error::{CompileError, CompiledExpr, ExtractError, FilterFn, MaybeCompiledExpr, ScalarFn};
#[cfg(feature = "jit")]
pub use expr::ExprCompiler;
#[cfg(feature = "jit")]
pub use jit::JitContext;

// F080: Plan Compiler Core types
#[cfg(feature = "jit")]
pub use cache::CompilerCache;
#[cfg(feature = "jit")]
pub use extractor::{ExtractedPlan, PipelineExtractor};
#[cfg(feature = "jit")]
pub use fallback::ExecutablePipeline;
#[cfg(feature = "jit")]
pub use pipeline::{
    CompiledPipeline, Pipeline, PipelineAction, PipelineBreaker, PipelineFn, PipelineId,
    PipelineStage, PipelineStats,
};
#[cfg(feature = "jit")]
pub use pipeline_compiler::PipelineCompiler;

// F082: Streaming Query Lifecycle
#[cfg(feature = "jit")]
pub use query::{StreamingQuery, StreamingQueryBuilder};
