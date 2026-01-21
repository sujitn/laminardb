# F006: Basic SQL Parser

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F006 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F005 |
| **Owner** | TBD |

## Summary

Extend DataFusion's SQL parser to support LaminarDB-specific streaming SQL extensions including window functions, watermarks, EMIT clauses, and CREATE SOURCE/SINK statements.

## Goals

- Parse streaming DDL (CREATE SOURCE, CREATE SINK)
- Support window function syntax (TUMBLE, HOP, SESSION)
- Handle EMIT clause for output control
- Parse watermark definitions

## Technical Design

```rust
pub enum StreamingStatement {
    CreateSource {
        name: String,
        columns: Vec<ColumnDef>,
        watermark: Option<WatermarkDef>,
        with_options: HashMap<String, String>,
    },
    CreateSink {
        name: String,
        columns: Vec<ColumnDef>,
        with_options: HashMap<String, String>,
    },
}

pub struct WatermarkDef {
    pub column: String,
    pub expression: Expr,
}
```

## Completion Checklist

- [ ] DDL parsing implemented
- [ ] Window functions parsed
- [ ] EMIT clause supported
- [ ] Unit tests passing
