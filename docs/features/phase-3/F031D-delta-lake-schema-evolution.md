# F031D: Delta Lake Schema Evolution

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F031D |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | S (2-3 days) |
| **Dependencies** | F031A (Delta Lake I/O) |
| **Blocks** | None |
| **Blocked By** | F031A (`deltalake` crate integration) |
| **Created** | 2026-02-02 |

## Summary

Implement automatic schema evolution for the Delta Lake Sink when `schema.evolution = true`. Detects additive column changes (new columns in incoming batches) and type widening (e.g., Int32 -> Int64) and updates the Delta Lake table schema accordingly. Schema evolution is opt-in and disabled by default.

F031 parses the `schema_evolution` config flag. F031D implements the schema merge logic and Delta Lake metadata update.

## Requirements

### Functional Requirements

- **FR-1**: Detect new columns in incoming `RecordBatch` not present in table schema
- **FR-2**: Add new columns to Delta table schema (additive evolution)
- **FR-3**: Detect safe type widening (Int8→Int16→Int32→Int64, Float32→Float64, Utf8→LargeUtf8)
- **FR-4**: Reject unsafe type changes (e.g., Int64→Utf8, Float64→Int32) with clear error
- **FR-5**: Schema changes recorded in Delta log metadata action
- **FR-6**: Existing Parquet files remain readable (nulls for new columns)
- **FR-7**: When `schema_evolution = false`, reject schema mismatches with `SchemaMismatch` error

### Non-Functional Requirements

- **NFR-1**: Schema comparison is O(columns), not O(rows)
- **NFR-2**: Schema evolution happens at most once per `open()` or when first mismatch detected
- **NFR-3**: No data rewriting — evolution is metadata-only

## Technical Design

### Schema Merge Logic

```rust
/// Result of comparing input schema against table schema.
#[derive(Debug)]
pub enum SchemaDiff {
    /// Schemas are identical.
    Identical,
    /// Input has new columns (additive evolution needed).
    NewColumns(Vec<ArrowField>),
    /// Input has wider types (type widening needed).
    TypeWidening(Vec<(String, DataType, DataType)>),  // (name, old, new)
    /// Mixed new columns and type widening.
    Mixed {
        new_columns: Vec<ArrowField>,
        widened: Vec<(String, DataType, DataType)>,
    },
    /// Incompatible change (cannot evolve).
    Incompatible(String),
}

/// Compares input schema against existing table schema.
pub fn diff_schemas(
    table_schema: &ArrowSchema,
    input_schema: &ArrowSchema,
) -> SchemaDiff { ... }

/// Safe type widening rules.
fn is_safe_widening(from: &DataType, to: &DataType) -> bool {
    matches!(
        (from, to),
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (DataType::Int16, DataType::Int32 | DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::Float32, DataType::Float64)
        | (DataType::Utf8, DataType::LargeUtf8)
        | (DataType::Binary, DataType::LargeBinary)
    )
}
```

### Integration with DeltaLakeSink

```rust
impl DeltaLakeSink {
    /// Handles schema evolution when a mismatch is detected.
    async fn handle_schema_mismatch(
        &mut self,
        input_schema: &ArrowSchema,
    ) -> Result<(), ConnectorError> {
        let table_schema = self.schema()
            .ok_or(ConnectorError::InvalidState { ... })?;

        let diff = diff_schemas(&table_schema, input_schema);

        match diff {
            SchemaDiff::Identical => Ok(()),
            SchemaDiff::Incompatible(msg) => {
                Err(ConnectorError::SchemaMismatch(msg))
            }
            _ if !self.config.schema_evolution => {
                Err(ConnectorError::SchemaMismatch(
                    "schema mismatch detected but schema.evolution is disabled".into()
                ))
            }
            SchemaDiff::NewColumns(cols) | SchemaDiff::Mixed { new_columns: cols, .. } => {
                self.evolve_schema(cols).await
            }
            SchemaDiff::TypeWidening(widened) => {
                self.widen_types(widened).await
            }
        }
    }
}
```

## Test Plan

### Unit Tests

- [ ] `test_diff_identical_schemas` - Returns Identical
- [ ] `test_diff_new_column` - Detects new column
- [ ] `test_diff_type_widening_int` - Int32->Int64 detected
- [ ] `test_diff_type_widening_float` - Float32->Float64 detected
- [ ] `test_diff_type_widening_string` - Utf8->LargeUtf8 detected
- [ ] `test_diff_incompatible_change` - Int64->Utf8 rejected
- [ ] `test_diff_mixed` - New columns + type widening together
- [ ] `test_safe_widening_rules` - All valid pairs
- [ ] `test_schema_evolution_disabled_rejects` - Error when disabled

### Integration Tests

- [ ] `test_evolve_add_column` - New column added to Delta table
- [ ] `test_evolve_widen_type` - Type widened in metadata
- [ ] `test_old_files_readable_after_evolution` - Nulls for new columns

## Completion Checklist

- [ ] `SchemaDiff` enum with `diff_schemas()` function
- [ ] `is_safe_widening()` type compatibility rules
- [ ] `evolve_schema()` for additive columns via Delta metadata action
- [ ] `widen_types()` for safe type widening
- [ ] Integration into `write_batch()` schema check
- [ ] Clear error messages when evolution is disabled
- [ ] Unit tests passing (9+ tests)
- [ ] Integration tests passing (3+ tests)
- [ ] Documentation updated

## References

- [F031: Delta Lake Sink](F031-delta-lake-sink.md)
- [F031A: Delta Lake I/O](F031A-delta-lake-io.md)
- [Delta Lake Schema Evolution](https://docs.delta.io/latest/delta-update.html#schema-evolution)
- [Arrow Schema compatibility](https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html)
