//! Primary-key-based reference table store.
//!
//! Supports upsert, delete, and lookup by primary key for dimension/reference
//! tables used in enrichment joins (e.g., `JOIN instruments ON t.symbol = i.symbol`).

use std::collections::HashMap;

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;

use crate::error::DbError;

/// Internal state for a single reference table.
#[allow(dead_code)]
struct TableState {
    /// Arrow schema for this table.
    schema: SchemaRef,
    /// Name of the primary key column.
    primary_key: String,
    /// Index of the primary key column in the schema.
    pk_index: usize,
    /// Rows keyed by primary key value (stringified).
    rows: HashMap<String, RecordBatch>,
    /// Whether the table has been fully populated (e.g., initial snapshot done).
    ready: bool,
    /// Connector type backing this table, if any.
    connector: Option<String>,
}

/// Primary-key-based reference table store.
///
/// Manages dimension/reference tables with upsert/delete/lookup semantics.
/// Each table has a designated primary key column used to key individual rows.
pub(crate) struct TableStore {
    tables: HashMap<String, TableState>,
}

impl TableStore {
    /// Create an empty table store.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a new table with the given schema and primary key column.
    ///
    /// # Errors
    ///
    /// Returns an error if the primary key column does not exist in the schema,
    /// or if a table with the same name already exists.
    pub fn create_table(
        &mut self,
        name: &str,
        schema: SchemaRef,
        primary_key: &str,
    ) -> Result<(), DbError> {
        if self.tables.contains_key(name) {
            return Err(DbError::TableAlreadyExists(name.to_string()));
        }
        let pk_index = schema
            .index_of(primary_key)
            .map_err(|_| DbError::InvalidOperation(format!(
                "Primary key column '{primary_key}' not found in table '{name}'"
            )))?;
        self.tables.insert(
            name.to_string(),
            TableState {
                schema,
                primary_key: primary_key.to_string(),
                pk_index,
                rows: HashMap::new(),
                ready: false,
                connector: None,
            },
        );
        Ok(())
    }

    /// Remove a table. Returns `true` if it existed.
    pub fn drop_table(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    /// Check if a table exists.
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// List all table names.
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get the schema for a table.
    pub fn table_schema(&self, name: &str) -> Option<SchemaRef> {
        self.tables.get(name).map(|t| t.schema.clone())
    }

    /// Get the primary key column name for a table.
    pub fn primary_key(&self, name: &str) -> Option<&str> {
        self.tables.get(name).map(|t| t.primary_key.as_str())
    }

    /// Get the row count for a table.
    pub fn table_row_count(&self, name: &str) -> usize {
        self.tables.get(name).map_or(0, |t| t.rows.len())
    }

    /// Check if a table is ready (fully populated).
    #[allow(dead_code)]
    pub fn is_ready(&self, name: &str) -> bool {
        self.tables.get(name).is_some_and(|t| t.ready)
    }

    /// Set the ready flag for a table.
    #[allow(dead_code)]
    pub fn set_ready(&mut self, name: &str, ready: bool) {
        if let Some(t) = self.tables.get_mut(name) {
            t.ready = ready;
        }
    }

    /// Set the connector type for a table.
    pub fn set_connector(&mut self, name: &str, connector_type: &str) {
        if let Some(t) = self.tables.get_mut(name) {
            t.connector = Some(connector_type.to_string());
        }
    }

    /// Get the connector type for a table.
    pub fn connector(&self, name: &str) -> Option<&str> {
        self.tables
            .get(name)
            .and_then(|t| t.connector.as_deref())
    }

    /// Upsert rows from a `RecordBatch` into a table.
    ///
    /// Extracts the primary key column, slices the batch into per-row batches,
    /// and inserts/overwrites each row by its PK value.
    ///
    /// Returns the number of rows upserted.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist.
    pub fn upsert(&mut self, name: &str, batch: &RecordBatch) -> Result<usize, DbError> {
        let state = self
            .tables
            .get_mut(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))?;

        let pk_col = batch.column(state.pk_index);
        let count = batch.num_rows();

        for i in 0..count {
            let key = extract_pk_string(pk_col, i);
            let row = batch.slice(i, 1);
            state.rows.insert(key, row);
        }

        Ok(count)
    }

    /// Delete a row by primary key. Returns `true` if the key existed.
    #[allow(dead_code)]
    pub fn delete(&mut self, name: &str, key: &str) -> bool {
        self.tables
            .get_mut(name)
            .is_some_and(|t| t.rows.remove(key).is_some())
    }

    /// Look up a single row by primary key.
    #[allow(dead_code)]
    pub fn lookup(&self, name: &str, key: &str) -> Option<RecordBatch> {
        self.tables
            .get(name)
            .and_then(|t| t.rows.get(key).cloned())
    }

    /// Concatenate all rows into a single `RecordBatch`.
    ///
    /// Returns `None` if the table does not exist. Returns an empty batch
    /// (with correct schema) if the table has no rows.
    pub fn to_record_batch(&self, name: &str) -> Option<RecordBatch> {
        let state = self.tables.get(name)?;
        if state.rows.is_empty() {
            return Some(RecordBatch::new_empty(state.schema.clone()));
        }
        let batches: Vec<&RecordBatch> = state.rows.values().collect();
        arrow::compute::concat_batches(&state.schema, batches.iter().copied()).ok()
    }
}

/// Extract the string representation of a primary key value at a given row.
fn extract_pk_string(col: &dyn Array, row: usize) -> String {
    // Try StringArray first (most common for PKs)
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return arr.value(row).to_string();
    }
    // Fall back to generic display via arrow's array formatting
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        return arr.value(row).to_string();
    }
    // Generic fallback using array_value_to_string
    arrow::util::display::array_value_to_string(col, row).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn make_batch(ids: &[i32], names: &[&str], prices: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Float64Array::from(prices.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_create_table_validates_pk() {
        let mut store = TableStore::new();
        let result = store.create_table("t", test_schema(), "id");
        assert!(result.is_ok());
        assert!(store.has_table("t"));
    }

    #[test]
    fn test_create_table_rejects_missing_pk() {
        let mut store = TableStore::new();
        let result = store.create_table("t", test_schema(), "nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_create_table_rejects_duplicate() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        let result = store.create_table("t", test_schema(), "id");
        assert!(matches!(result, Err(DbError::TableAlreadyExists(_))));
    }

    #[test]
    fn test_upsert_and_lookup() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch = make_batch(&[1], &["Widget"], &[9.99]);
        let count = store.upsert("t", &batch).unwrap();
        assert_eq!(count, 1);
        assert_eq!(store.table_row_count("t"), 1);

        let row = store.lookup("t", "1").unwrap();
        assert_eq!(row.num_rows(), 1);
        let names = row.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "Widget");
    }

    #[test]
    fn test_upsert_multiple_rows() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch = make_batch(&[1, 2, 3], &["A", "B", "C"], &[1.0, 2.0, 3.0]);
        let count = store.upsert("t", &batch).unwrap();
        assert_eq!(count, 3);
        assert_eq!(store.table_row_count("t"), 3);
    }

    #[test]
    fn test_upsert_overwrites_existing() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch1 = make_batch(&[1], &["Old"], &[1.0]);
        store.upsert("t", &batch1).unwrap();

        let batch2 = make_batch(&[1], &["New"], &[2.0]);
        store.upsert("t", &batch2).unwrap();

        assert_eq!(store.table_row_count("t"), 1);
        let row = store.lookup("t", "1").unwrap();
        let names = row.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "New");
    }

    #[test]
    fn test_delete_existing_key() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch = make_batch(&[1], &["Widget"], &[9.99]);
        store.upsert("t", &batch).unwrap();

        assert!(store.delete("t", "1"));
        assert_eq!(store.table_row_count("t"), 0);
        assert!(store.lookup("t", "1").is_none());
    }

    #[test]
    fn test_delete_missing_key() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(!store.delete("t", "999"));
    }

    #[test]
    fn test_lookup_missing() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(store.lookup("t", "1").is_none());
        assert!(store.lookup("nosuch", "1").is_none());
    }

    #[test]
    fn test_table_names_and_counts() {
        let mut store = TableStore::new();
        assert!(store.table_names().is_empty());

        store.create_table("a", test_schema(), "id").unwrap();
        store.create_table("b", test_schema(), "id").unwrap();

        let mut names = store.table_names();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
        assert!(store.has_table("a"));
        assert!(!store.has_table("c"));
    }

    #[test]
    fn test_to_record_batch() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        // Empty table returns empty batch
        let batch = store.to_record_batch("t").unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), test_schema());

        // With data
        store
            .upsert("t", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
            .unwrap();
        let batch = store.to_record_batch("t").unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Missing table
        assert!(store.to_record_batch("nosuch").is_none());
    }

    #[test]
    fn test_drop_table() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(store.drop_table("t"));
        assert!(!store.has_table("t"));
        assert!(!store.drop_table("t"));
    }

    #[test]
    fn test_ready_flag() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(!store.is_ready("t"));

        store.set_ready("t", true);
        assert!(store.is_ready("t"));

        store.set_ready("t", false);
        assert!(!store.is_ready("t"));
    }

    #[test]
    fn test_connector_tracking() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(store.connector("t").is_none());

        store.set_connector("t", "kafka");
        assert_eq!(store.connector("t"), Some("kafka"));
    }
}
