# F-CLOUD-003: Secret Masking & Safe Logging

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CLOUD-003 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | S (1 day) |
| **Dependencies** | F-CLOUD-001 (Credential Resolver) |
| **Blocks** | None (quality improvement) |
| **Created** | 2026-02-02 |

## Summary

Implement secret masking for connector configuration to ensure credentials never appear in log output, `Debug`/`Display` representations, error messages, or metric labels. Provides a shared `SecretMasker` utility and `Display`/`Debug` implementations for `DeltaLakeSinkConfig` and `ConnectorConfig` that redact sensitive keys.

This is critical for production deployments where logs may be shipped to centralized systems (ELK, Datadog, CloudWatch) and credentials must not leak.

## Requirements

### Functional Requirements

- **FR-1**: Identify secret keys by pattern: any key containing `secret`, `password`, `key`, `token`, `credential`, `sas` (case-insensitive)
- **FR-2**: Replace secret values with `***` in Display/Debug output
- **FR-3**: Provide `ConnectorConfig::display_redacted()` method for safe logging
- **FR-4**: Provide `ResolvedStorageOptions::display_redacted()` for resolver output
- **FR-5**: Ensure `DeltaLakeSinkConfig` Debug output redacts `storage_options` secrets
- **FR-6**: Ensure tracing log statements in `open()` never include secrets
- **FR-7**: Provide `SecretMasker::redact_map()` for arbitrary `HashMap<String, String>`

### Non-Functional Requirements

- **NFR-1**: Zero performance impact on hot path (masking only called during open/close/error)
- **NFR-2**: No false positives on common non-secret keys (e.g., `aws_region`, `table.path`)
- **NFR-3**: Works with all connector types (Kafka SASL, Postgres password, Delta storage)

## Technical Design

### Data Structures

```rust
/// Utility for masking secret values in configuration maps.
pub struct SecretMasker;

/// Patterns that indicate a key holds a secret value.
const SECRET_PATTERNS: &[&str] = &[
    "secret",
    "password",
    "account_key",
    "access_key",
    "token",
    "credential",
    "sas",
    "private_key",
];

impl SecretMasker {
    /// Returns true if the key name suggests it holds a secret value.
    pub fn is_secret_key(key: &str) -> bool {
        let lower = key.to_lowercase();
        SECRET_PATTERNS.iter().any(|p| lower.contains(p))
    }

    /// Returns a redacted copy of the map, replacing secret values with "***".
    pub fn redact_map(map: &HashMap<String, String>) -> HashMap<String, String> {
        map.iter()
            .map(|(k, v)| {
                if Self::is_secret_key(k) {
                    (k.clone(), "***".to_string())
                } else {
                    (k.clone(), v.clone())
                }
            })
            .collect()
    }

    /// Formats a map for display with secrets redacted.
    pub fn display_map(map: &HashMap<String, String>) -> String {
        let mut pairs: Vec<_> = map.iter().collect();
        pairs.sort_by_key(|(k, _)| k.as_str());
        pairs
            .iter()
            .map(|(k, v)| {
                if Self::is_secret_key(k) {
                    format!("{k}=***")
                } else {
                    format!("{k}={v}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}
```

### DeltaLakeSinkConfig Integration

```rust
impl fmt::Display for DeltaLakeSinkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DeltaLakeSinkConfig {{ table_path: {}, mode: {}, guarantee: {}, storage: {{{}}} }}",
            self.table_path,
            self.write_mode,
            self.delivery_guarantee,
            SecretMasker::display_map(&self.storage_options),
        )
    }
}

impl fmt::Debug for DeltaLakeSinkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeltaLakeSinkConfig")
            .field("table_path", &self.table_path)
            .field("write_mode", &self.write_mode)
            .field("delivery_guarantee", &self.delivery_guarantee)
            .field("storage_options", &SecretMasker::redact_map(&self.storage_options))
            .field("partition_columns", &self.partition_columns)
            .field("target_file_size", &self.target_file_size)
            .field("max_buffer_records", &self.max_buffer_records)
            .field("schema_evolution", &self.schema_evolution)
            // ... other non-secret fields
            .finish()
    }
}
```

### ConnectorConfig Integration

```rust
impl ConnectorConfig {
    /// Returns a display-safe representation with secrets redacted.
    pub fn display_redacted(&self) -> String {
        let redacted = SecretMasker::redact_map(self.properties());
        format!("ConnectorConfig({}, {{{}}})",
            self.connector_type(),
            SecretMasker::display_map(&redacted),
        )
    }
}
```

### Module Structure

```
crates/laminar-connectors/src/
  storage/
    masking.rs          # SecretMasker, SECRET_PATTERNS
```

## Test Plan

### Unit Tests

- [ ] `test_is_secret_key_access_key` - `aws_access_key_id` is not secret (it's the ID, not the key)
- [ ] `test_is_secret_key_secret_key` - `aws_secret_access_key` is secret
- [ ] `test_is_secret_key_password` - `password` is secret
- [ ] `test_is_secret_key_sas_token` - `azure_storage_sas_token` is secret
- [ ] `test_is_secret_key_account_key` - `azure_storage_account_key` is secret
- [ ] `test_is_secret_key_region` - `aws_region` is NOT secret
- [ ] `test_is_secret_key_table_path` - `table.path` is NOT secret
- [ ] `test_is_secret_key_private_key` - `google_private_key` is secret
- [ ] `test_redact_map` - Secrets replaced with `***`, non-secrets preserved
- [ ] `test_redact_map_empty` - Empty map returns empty
- [ ] `test_display_map_sorted` - Output is sorted by key
- [ ] `test_display_map_redacted` - Secrets show `***` in display
- [ ] `test_delta_config_debug_redacted` - Debug output hides secrets
- [ ] `test_delta_config_display_redacted` - Display output hides secrets
- [ ] `test_connector_config_display_redacted` - ConnectorConfig safe display

## Completion Checklist

- [ ] `SecretMasker` utility with `is_secret_key()`, `redact_map()`, `display_map()`
- [ ] `SECRET_PATTERNS` covering all cloud credential key patterns
- [ ] `DeltaLakeSinkConfig` Display/Debug implementations with masking
- [ ] `ConnectorConfig::display_redacted()` method
- [ ] Audit all `tracing::info!`/`debug!`/`warn!` in connector `open()` for secret leaks
- [ ] Unit tests passing (15+ tests)
- [ ] Code reviewed

## References

- [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
- [F-CLOUD-001: Storage Credential Resolver](F-CLOUD-001-credential-resolver.md)
