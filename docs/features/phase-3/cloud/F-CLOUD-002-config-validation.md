# F-CLOUD-002: Cloud Config Validation

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CLOUD-002 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P0 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F-CLOUD-001 (Credential Resolver) |
| **Blocks** | F031A, F032, F033 |
| **Created** | 2026-02-02 |

## Summary

Implement per-cloud-provider configuration validation that catches missing or invalid storage credentials at connector `open()` time, before any I/O is attempted. The validator uses the `StorageProvider` detected by F-CLOUD-001 and the resolved credentials to produce clear, actionable error messages.

This prevents confusing downstream errors from `object_store` or `deltalake` when credentials are missing or malformed. Validation is invoked as part of `DeltaLakeSinkConfig::validate()` and any future lakehouse connector config validation.

## Requirements

### Functional Requirements

- **FR-1**: Validate S3 paths have `aws_region` (from config or env)
- **FR-2**: Validate S3 paths have either `aws_access_key_id` + `aws_secret_access_key` or `aws_profile` (warn if neither — may still work with instance metadata)
- **FR-3**: Validate Azure paths have `azure_storage_account_name`
- **FR-4**: Validate Azure paths have one of: `azure_storage_account_key`, `azure_storage_sas_token`, or `azure_storage_client_id` (warn if none — may use Managed Identity)
- **FR-5**: Validate GCS paths have `google_service_account_path` or `google_service_account_key` (warn if neither — may use Workload Identity)
- **FR-6**: Validate custom S3 endpoints (MinIO) have `aws_endpoint` and `aws_s3_allow_unsafe_rename`
- **FR-7**: Validate local paths exist and are writable (best-effort)
- **FR-8**: Return structured `ValidationResult` with errors (hard stop) and warnings (may still work)
- **FR-9**: Produce human-readable error messages that include the missing key name AND the fallback env var name

### Non-Functional Requirements

- **NFR-1**: Validation must be fast (< 1ms, no network calls)
- **NFR-2**: Warning vs error distinction: missing credentials that *might* be resolved by instance metadata are warnings, not errors
- **NFR-3**: Error messages must be actionable (tell the user exactly what to add)

## Technical Design

### Data Structures

```rust
/// Result of cloud configuration validation.
#[derive(Debug, Clone)]
pub struct CloudValidationResult {
    /// Hard errors that prevent connector from opening.
    pub errors: Vec<CloudValidationError>,
    /// Soft warnings (may still work with instance metadata).
    pub warnings: Vec<CloudValidationWarning>,
}

impl CloudValidationResult {
    /// Returns true if there are no hard errors.
    pub fn is_valid(&self) -> bool { self.errors.is_empty() }
}

/// A hard validation error.
#[derive(Debug, Clone)]
pub struct CloudValidationError {
    /// The missing or invalid configuration key.
    pub key: String,
    /// The fallback environment variable (if applicable).
    pub env_var: Option<String>,
    /// Human-readable error message.
    pub message: String,
}

/// A soft validation warning.
#[derive(Debug, Clone)]
pub struct CloudValidationWarning {
    /// What might be missing.
    pub key: String,
    /// Why it might still work.
    pub message: String,
}

/// Validates resolved storage options for a given provider.
pub struct CloudConfigValidator;

impl CloudConfigValidator {
    /// Validates the resolved storage options for the given provider.
    ///
    /// # Arguments
    ///
    /// * `provider` - Detected cloud provider
    /// * `resolved` - Resolved options from F-CLOUD-001
    ///
    /// # Returns
    ///
    /// `CloudValidationResult` with any errors or warnings.
    pub fn validate(
        provider: StorageProvider,
        resolved: &ResolvedStorageOptions,
    ) -> CloudValidationResult { ... }
}
```

### Validation Rules

```rust
// AWS S3 validation
fn validate_s3(resolved: &ResolvedStorageOptions) -> CloudValidationResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    // Region is required (no instance metadata fallback for region)
    if !resolved.options.contains_key("aws_region") {
        errors.push(CloudValidationError {
            key: "aws_region".into(),
            env_var: Some("AWS_REGION".into()),
            message: "S3 paths require 'storage.aws_region' or AWS_REGION env var".into(),
        });
    }

    // Credentials: warn if missing (instance metadata may provide them)
    if !resolved.options.contains_key("aws_access_key_id")
        && !resolved.options.contains_key("aws_profile")
    {
        warnings.push(CloudValidationWarning {
            key: "aws_access_key_id".into(),
            message: "No AWS credentials found in config or environment. \
                      Falling back to instance metadata (IAM role). \
                      Set 'storage.aws_access_key_id' or AWS_ACCESS_KEY_ID if needed.".into(),
        });
    }

    // If access key provided, secret key must also be provided
    if resolved.options.contains_key("aws_access_key_id")
        && !resolved.options.contains_key("aws_secret_access_key")
    {
        errors.push(CloudValidationError {
            key: "aws_secret_access_key".into(),
            env_var: Some("AWS_SECRET_ACCESS_KEY".into()),
            message: "'aws_access_key_id' provided without 'aws_secret_access_key'".into(),
        });
    }

    CloudValidationResult { errors, warnings }
}

// Azure validation
fn validate_azure(resolved: &ResolvedStorageOptions) -> CloudValidationResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    // Account name is always required
    if !resolved.options.contains_key("azure_storage_account_name") {
        errors.push(CloudValidationError {
            key: "azure_storage_account_name".into(),
            env_var: Some("AZURE_STORAGE_ACCOUNT_NAME".into()),
            message: "Azure paths require 'storage.azure_storage_account_name' \
                      or AZURE_STORAGE_ACCOUNT_NAME env var".into(),
        });
    }

    // Credentials: warn if missing (Managed Identity may provide them)
    if !resolved.options.contains_key("azure_storage_account_key")
        && !resolved.options.contains_key("azure_storage_sas_token")
        && !resolved.options.contains_key("azure_storage_client_id")
    {
        warnings.push(CloudValidationWarning {
            key: "azure_storage_account_key".into(),
            message: "No Azure credentials found. Falling back to Managed Identity. \
                      Set 'storage.azure_storage_account_key' or \
                      AZURE_STORAGE_ACCOUNT_KEY if needed.".into(),
        });
    }

    CloudValidationResult { errors, warnings }
}

// GCS validation
fn validate_gcs(resolved: &ResolvedStorageOptions) -> CloudValidationResult {
    let mut warnings = Vec::new();

    if !resolved.options.contains_key("google_service_account_path")
        && !resolved.options.contains_key("google_service_account_key")
    {
        warnings.push(CloudValidationWarning {
            key: "google_service_account_path".into(),
            message: "No GCS credentials found. Falling back to Workload Identity / \
                      Application Default Credentials. \
                      Set 'storage.google_service_account_path' or \
                      GOOGLE_APPLICATION_CREDENTIALS if needed.".into(),
        });
    }

    CloudValidationResult { errors: vec![], warnings }
}
```

### Integration with DeltaLakeSinkConfig

```rust
impl DeltaLakeSinkConfig {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        // ... existing validation ...

        // Cloud-specific validation (after credential resolution)
        let resolved = StorageCredentialResolver::resolve(
            &self.table_path,
            &self.storage_options,
        );
        let validation = CloudConfigValidator::validate(
            resolved.provider,
            &resolved,
        );

        if !validation.is_valid() {
            let msg = validation.errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(ConnectorError::ConfigurationError(msg));
        }

        // Log warnings
        for warning in &validation.warnings {
            tracing::warn!(key = %warning.key, "{}", warning.message);
        }

        Ok(())
    }
}
```

## Test Plan

### Unit Tests

- [ ] `test_validate_s3_all_present` - No errors when all S3 keys present
- [ ] `test_validate_s3_missing_region` - Error on missing region
- [ ] `test_validate_s3_missing_credentials_warns` - Warning (not error) when keys missing
- [ ] `test_validate_s3_access_key_without_secret` - Error on partial key pair
- [ ] `test_validate_s3_profile_sufficient` - aws_profile alone is valid
- [ ] `test_validate_azure_all_present` - No errors when all Azure keys present
- [ ] `test_validate_azure_missing_account_name` - Error on missing account
- [ ] `test_validate_azure_sas_token_sufficient` - SAS token alone is valid
- [ ] `test_validate_azure_missing_credentials_warns` - Warning when keys missing
- [ ] `test_validate_gcs_all_present` - No errors when GCS keys present
- [ ] `test_validate_gcs_missing_credentials_warns` - Warning when keys missing
- [ ] `test_validate_local_no_validation` - Local paths always pass
- [ ] `test_validation_result_is_valid` - `is_valid()` checks errors only
- [ ] `test_error_messages_include_env_var` - Actionable error messages
- [ ] `test_delta_config_validate_calls_cloud` - Integration with DeltaLakeSinkConfig

## Completion Checklist

- [ ] `CloudConfigValidator` with per-provider rules
- [ ] `CloudValidationResult`, `CloudValidationError`, `CloudValidationWarning` types
- [ ] S3 validation rules (region required, credential pair check)
- [ ] Azure validation rules (account name required)
- [ ] GCS validation rules (warning-only for missing keys)
- [ ] Local path validation (skip cloud checks)
- [ ] Integration into `DeltaLakeSinkConfig::validate()`
- [ ] Actionable error messages with env var hints
- [ ] Unit tests passing (15+ tests)
- [ ] Code reviewed

## References

- [F-CLOUD-001: Storage Credential Resolver](F-CLOUD-001-credential-resolver.md)
- [AWS credential resolution order](https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html)
- [Azure identity credential types](https://learn.microsoft.com/en-us/rust/api/azure_identity/)
