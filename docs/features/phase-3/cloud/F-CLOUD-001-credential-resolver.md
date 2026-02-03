# F-CLOUD-001: Storage Credential Resolver

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CLOUD-001 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P0 |
| **Effort** | M (2-3 days) |
| **Dependencies** | None (new shared module) |
| **Blocks** | F-CLOUD-002, F-CLOUD-003, F031A, F032, F033 |
| **Created** | 2026-02-02 |

## Summary

Implement a shared `StorageCredentialResolver` that unifies cloud credential resolution across all lakehouse connectors. Resolves credentials via a priority chain: explicit `storage.*` config options > environment variables > instance metadata / default credential providers. This eliminates credential logic duplication between Delta Lake (F031), Iceberg (F032), and Parquet Source (F033).

The resolver lives in `laminar-connectors/src/storage/` and is used by all connectors that interact with cloud object stores.

## Requirements

### Functional Requirements

- **FR-1**: Detect cloud provider from `table_path` URI scheme (`s3://`, `az://`, `abfss://`, `gs://`, `file://`, local path)
- **FR-2**: Resolve AWS credentials: explicit keys > `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars > `AWS_PROFILE` > instance metadata (IMDSv2)
- **FR-3**: Resolve Azure credentials: explicit keys > `AZURE_STORAGE_ACCOUNT_NAME`/`AZURE_STORAGE_ACCOUNT_KEY` env vars > `AZURE_STORAGE_SAS_TOKEN` > Managed Identity
- **FR-4**: Resolve GCS credentials: explicit path > `GOOGLE_APPLICATION_CREDENTIALS` env var > Workload Identity
- **FR-5**: Support MinIO/S3-compatible endpoints via `aws_endpoint` + `aws_s3_allow_unsafe_rename`
- **FR-6**: Return merged `HashMap<String, String>` ready for `object_store` / `deltalake` crate consumption
- **FR-7**: Support STS session tokens for temporary AWS credentials (`AWS_SESSION_TOKEN`)
- **FR-8**: Detect local paths (no cloud credentials needed) and short-circuit

### Non-Functional Requirements

- **NFR-1**: Credential resolution must be sync (called during `open()`, not on hot path)
- **NFR-2**: Environment variable reads should be cached per `open()` call (not read on every batch)
- **NFR-3**: No cloud SDK dependencies - only resolve env vars and merge into HashMap; actual SDK auth happens in `object_store`/`deltalake`
- **NFR-4**: Must work on Linux, macOS, and Windows

## Technical Design

### Architecture

```
ConnectorConfig (SQL WITH clause)
    |
    | properties_with_prefix("storage.")
    v
+--------------------------------------------+
| StorageCredentialResolver                  |
|                                            |
| 1. detect_provider(table_path) -> Provider |
| 2. resolve(config_opts) -> ResolvedOpts    |
|    a. Start with explicit storage.* keys   |
|    b. Fill gaps from env vars              |
|    c. Fill gaps from default provider      |
| 3. Return merged HashMap<String, String>   |
+--------------------------------------------+
    |
    v
deltalake::open_table_with_storage_options(path, resolved_opts)
```

### Data Structures

```rust
/// Cloud storage provider detected from URI scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageProvider {
    /// Amazon S3 or S3-compatible (MinIO, LocalStack).
    AwsS3,
    /// Azure Data Lake Storage Gen2 / Azure Blob Storage.
    AzureAdls,
    /// Google Cloud Storage.
    Gcs,
    /// Local filesystem (no credentials needed).
    Local,
}

impl StorageProvider {
    /// Detects the storage provider from a table path URI.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(StorageProvider::detect("s3://bucket/path"), StorageProvider::AwsS3);
    /// assert_eq!(StorageProvider::detect("az://container/path"), StorageProvider::AzureAdls);
    /// assert_eq!(StorageProvider::detect("gs://bucket/path"), StorageProvider::Gcs);
    /// assert_eq!(StorageProvider::detect("/local/path"), StorageProvider::Local);
    /// ```
    pub fn detect(table_path: &str) -> Self { ... }
}

/// Resolved storage credentials ready for object_store / deltalake.
#[derive(Debug, Clone)]
pub struct ResolvedStorageOptions {
    /// Detected cloud provider.
    pub provider: StorageProvider,
    /// Merged options (explicit config + env vars + defaults).
    /// Keys match what `deltalake`/`object_store` expect.
    pub options: HashMap<String, String>,
    /// Which keys were resolved from environment variables.
    pub env_resolved_keys: Vec<String>,
    /// Whether any credentials were found at all.
    pub has_credentials: bool,
}

/// Storage credential resolver.
///
/// Resolves credentials by priority chain:
/// 1. Explicit `storage.*` keys from SQL WITH clause
/// 2. Environment variables
/// 3. Instance metadata / default credential provider
pub struct StorageCredentialResolver;

impl StorageCredentialResolver {
    /// Resolves storage credentials for the given table path.
    ///
    /// Merges explicit options with environment variable fallbacks
    /// appropriate for the detected cloud provider.
    ///
    /// # Arguments
    ///
    /// * `table_path` - URI of the table (s3://, az://, gs://, or local path)
    /// * `explicit_options` - Options from SQL WITH clause (storage.* prefix already stripped)
    ///
    /// # Returns
    ///
    /// `ResolvedStorageOptions` with merged credentials ready for `object_store`.
    pub fn resolve(
        table_path: &str,
        explicit_options: &HashMap<String, String>,
    ) -> ResolvedStorageOptions { ... }
}
```

### Environment Variable Mapping

```rust
/// AWS S3 environment variable fallbacks.
const AWS_ENV_MAPPING: &[(&str, &str)] = &[
    ("aws_access_key_id",     "AWS_ACCESS_KEY_ID"),
    ("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY"),
    ("aws_region",            "AWS_REGION"),
    ("aws_session_token",     "AWS_SESSION_TOKEN"),
    ("aws_endpoint",          "AWS_ENDPOINT_URL"),
    ("aws_profile",           "AWS_PROFILE"),
];

/// Azure ADLS environment variable fallbacks.
const AZURE_ENV_MAPPING: &[(&str, &str)] = &[
    ("azure_storage_account_name", "AZURE_STORAGE_ACCOUNT_NAME"),
    ("azure_storage_account_key",  "AZURE_STORAGE_ACCOUNT_KEY"),
    ("azure_storage_sas_token",    "AZURE_STORAGE_SAS_TOKEN"),
    ("azure_storage_client_id",    "AZURE_CLIENT_ID"),
    ("azure_storage_tenant_id",    "AZURE_TENANT_ID"),
];

/// GCS environment variable fallbacks.
const GCS_ENV_MAPPING: &[(&str, &str)] = &[
    ("google_service_account_path", "GOOGLE_APPLICATION_CREDENTIALS"),
    ("google_service_account_key",  "GOOGLE_SERVICE_ACCOUNT_KEY"),
];
```

### Resolution Algorithm

```rust
impl StorageCredentialResolver {
    pub fn resolve(
        table_path: &str,
        explicit_options: &HashMap<String, String>,
    ) -> ResolvedStorageOptions {
        let provider = StorageProvider::detect(table_path);

        // Local paths need no credentials.
        if provider == StorageProvider::Local {
            return ResolvedStorageOptions {
                provider,
                options: explicit_options.clone(),
                env_resolved_keys: vec![],
                has_credentials: false,
            };
        }

        let mut resolved = explicit_options.clone();
        let mut env_resolved = Vec::new();

        let env_mapping = match provider {
            StorageProvider::AwsS3 => AWS_ENV_MAPPING,
            StorageProvider::AzureAdls => AZURE_ENV_MAPPING,
            StorageProvider::Gcs => GCS_ENV_MAPPING,
            StorageProvider::Local => &[],
        };

        // Fill gaps from environment variables.
        for (option_key, env_var) in env_mapping {
            if !resolved.contains_key(*option_key) {
                if let Ok(val) = std::env::var(env_var) {
                    if !val.is_empty() {
                        resolved.insert(option_key.to_string(), val);
                        env_resolved.push(option_key.to_string());
                    }
                }
            }
        }

        let has_credentials = match provider {
            StorageProvider::AwsS3 => {
                resolved.contains_key("aws_access_key_id")
                    || resolved.contains_key("aws_profile")
            }
            StorageProvider::AzureAdls => {
                resolved.contains_key("azure_storage_account_key")
                    || resolved.contains_key("azure_storage_sas_token")
                    || resolved.contains_key("azure_storage_client_id")
            }
            StorageProvider::Gcs => {
                resolved.contains_key("google_service_account_path")
                    || resolved.contains_key("google_service_account_key")
            }
            StorageProvider::Local => false,
        };

        ResolvedStorageOptions {
            provider,
            options: resolved,
            env_resolved_keys: env_resolved,
            has_credentials,
        }
    }
}
```

### Module Structure

```
crates/laminar-connectors/src/
  storage/
    mod.rs              # Module root, re-exports
    provider.rs         # StorageProvider enum, detect()
    resolver.rs         # StorageCredentialResolver, resolve()
    env_mapping.rs      # Per-provider env var mappings
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| DeltaLakeSinkConfig | `lakehouse/delta_config.rs` | Call `StorageCredentialResolver::resolve()` in `from_config()` |
| IcebergSinkConfig | `lakehouse/iceberg_config.rs` (future) | Same pattern |
| ConnectorConfig | `config.rs` | No changes needed (prefix extraction already works) |

## Test Plan

### Unit Tests

- [ ] `test_detect_s3_scheme` - `s3://bucket/path` -> AwsS3
- [ ] `test_detect_s3a_scheme` - `s3a://bucket/path` -> AwsS3
- [ ] `test_detect_azure_scheme` - `az://container/path` -> AzureAdls
- [ ] `test_detect_abfss_scheme` - `abfss://container@account/path` -> AzureAdls
- [ ] `test_detect_gcs_scheme` - `gs://bucket/path` -> Gcs
- [ ] `test_detect_local_absolute` - `/data/tables/t1` -> Local
- [ ] `test_detect_local_relative` - `./data/tables` -> Local
- [ ] `test_detect_file_scheme` - `file:///data/tables` -> Local
- [ ] `test_detect_windows_path` - `C:\data\tables` -> Local
- [ ] `test_resolve_local_no_credentials` - Local path returns empty
- [ ] `test_resolve_s3_explicit_keys` - Explicit keys used as-is
- [ ] `test_resolve_s3_env_fallback` - Env vars fill missing keys
- [ ] `test_resolve_s3_explicit_overrides_env` - Explicit takes priority
- [ ] `test_resolve_s3_session_token` - STS token resolved
- [ ] `test_resolve_s3_custom_endpoint` - MinIO endpoint support
- [ ] `test_resolve_azure_account_key` - Azure key resolution
- [ ] `test_resolve_azure_sas_token` - SAS token resolution
- [ ] `test_resolve_azure_managed_identity` - Client ID / tenant ID
- [ ] `test_resolve_gcs_service_account_path` - JSON path resolution
- [ ] `test_resolve_gcs_inline_key` - Inline JSON key
- [ ] `test_env_resolved_keys_tracking` - Tracks which keys came from env
- [ ] `test_has_credentials_true_s3` - Has credentials when keys present
- [ ] `test_has_credentials_false_local` - No credentials for local
- [ ] `test_empty_env_var_not_used` - Empty env vars skipped
- [ ] `test_resolve_aws_profile` - AWS_PROFILE fallback

### Integration Tests

- [ ] `test_delta_config_uses_resolver` - DeltaLakeSinkConfig calls resolver
- [ ] `test_resolver_with_real_env_vars` - Set/unset env vars in test

## Completion Checklist

- [ ] `StorageProvider` enum with `detect()` method
- [ ] `StorageCredentialResolver` with `resolve()` method
- [ ] `ResolvedStorageOptions` result type
- [ ] AWS S3 env var mapping (6 keys)
- [ ] Azure ADLS env var mapping (5 keys)
- [ ] GCS env var mapping (2 keys)
- [ ] Integration into `DeltaLakeSinkConfig::from_config()`
- [ ] Unit tests passing (25+ tests)
- [ ] Documentation with examples
- [ ] Code reviewed

## References

- [object_store AmazonS3Builder](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html)
- [object_store MicrosoftAzureBuilder](https://docs.rs/object_store/latest/object_store/azure/struct.MicrosoftAzureBuilder.html)
- [object_store GoogleCloudStorageBuilder](https://docs.rs/object_store/latest/object_store/gcp/struct.GoogleCloudStorageBuilder.html)
- [deltalake storage options](https://delta-io.github.io/delta-rs/usage/loading-table/#storage-options)
- [AWS credential provider chain](https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html)
