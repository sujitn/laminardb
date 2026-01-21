---
paths:
  - "**/*.rs"
---

# Rust Style Guide

## Formatting

- Run `cargo fmt` before committing
- Max line length: 100 characters
- Use 4 spaces for indentation

## Naming Conventions

```rust
// Types: PascalCase
pub struct WindowState { }
pub enum EventType { }
pub trait Operator { }

// Functions/methods: snake_case
fn process_event() { }
impl Operator {
    fn emit_result(&self) { }
}

// Constants: SCREAMING_SNAKE_CASE
const MAX_BATCH_SIZE: usize = 1024;
static DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

// Modules: snake_case
mod window_operators;
mod state_store;
```

## Error Handling

```rust
// Use thiserror for library errors
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("key not found: {0}")]
    KeyNotFound(String),
    
    #[error("storage error: {0}")]
    Storage(#[from] std::io::Error),
}

// Use anyhow for application code
fn main() -> anyhow::Result<()> {
    let config = Config::load()?;
    Ok(())
}

// Propagate with ? operator
fn get_value(&self, key: &[u8]) -> Result<Vec<u8>, StateError> {
    let data = self.storage.read(key)?;
    Ok(data)
}
```

## Documentation

```rust
/// Brief one-line description.
///
/// More detailed explanation if needed. Can span
/// multiple lines.
///
/// # Arguments
///
/// * `key` - The key to look up
/// * `default` - Value to return if key not found
///
/// # Returns
///
/// The value associated with the key, or the default.
///
/// # Errors
///
/// Returns `StateError::Storage` if the underlying storage fails.
///
/// # Examples
///
/// ```
/// let store = StateStore::new();
/// let value = store.get_or_default(b"key", b"default");
/// ```
pub fn get_or_default(&self, key: &[u8], default: &[u8]) -> Result<Vec<u8>, StateError> {
    // ...
}
```

## Imports

```rust
// Group imports: std, external crates, internal modules
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use tokio::sync::mpsc;

use crate::state::StateStore;
use super::Operator;
```

## Type Annotations

- Prefer explicit types for public APIs
- Use type inference for local variables when obvious
- Always annotate closure parameters in complex cases

```rust
// Good: explicit public API
pub fn process(&self, batch: RecordBatch) -> Result<RecordBatch, ProcessError>

// Good: inferred local
let count = items.len();

// Good: annotated complex closure
let processor = |event: &Event| -> Option<Output> {
    // ...
};
```
