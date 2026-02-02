# Cloud Storage Infrastructure

> Shared cloud storage credential management, validation, and security for all lakehouse connectors.
> Applies to: F031 (Delta Lake), F032 (Iceberg), F033 (Parquet Source), and future cloud-backed connectors.

## Overview

Provides a unified infrastructure layer for resolving, validating, and securely handling
cloud storage credentials across all LaminarDB connectors that write to or read from
object stores (S3, Azure ADLS, GCS, MinIO).

**Key Design Principles**:
- Credential resolution chain: explicit config > environment variables > instance metadata
- Per-provider validation: URI scheme determines required fields
- Secret masking by default: credentials never appear in logs, Debug, or Display output
- Shared across all lakehouse connectors (Delta Lake, Iceberg, Parquet)

## Architecture

```
SQL WITH clause                   Environment                 Instance Metadata
+-------------------+            +-------------------+        +-------------------+
| storage.aws_*     |            | AWS_ACCESS_KEY_ID |        | EC2/ECS IAM Role  |
| storage.azure_*   |            | AZURE_STORAGE_*   |        | Azure MI          |
| storage.google_*  |            | GOOGLE_APPLICATION|        | GCP Workload ID   |
+--------+----------+            +--------+----------+        +--------+----------+
         |                                |                            |
         v                                v                            v
    +----+------------------------------------------------+------------+
    |              StorageCredentialResolver                            |
    |  - resolve_credentials(table_path, storage_options)              |
    |  - validate_for_provider(scheme, resolved_options)               |
    |  - redact_secrets(options) -> safe display                       |
    +------------------------------------------------------------------+
         |                    |                     |
         v                    v                     v
    Delta Lake Sink     Iceberg Sink         Parquet Source
    (F031)              (F032)               (F033)
```

## Features

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-CLOUD-001 | Storage Credential Resolver | P0 | ✅ Done | [Link](F-CLOUD-001-credential-resolver.md) |
| F-CLOUD-002 | Cloud Config Validation | P0 | ✅ Done | [Link](F-CLOUD-002-config-validation.md) |
| F-CLOUD-003 | Secret Masking & Safe Logging | P1 | ✅ Done | [Link](F-CLOUD-003-secret-masking.md) |

## Dependency Graph

```
F-CLOUD-001 (Credential Resolver)
    |
    +---> F-CLOUD-002 (Config Validation)
    |         |
    +---> F-CLOUD-003 (Secret Masking)
              |
    +---------+---------+---------+
    |                   |         |
F031 (Delta)     F032 (Iceberg)  F033 (Parquet)
```

## Implementation Order

1. **Phase 1: Core Resolver** - F-CLOUD-001 (credential chain, env var fallback)
2. **Phase 2: Validation** - F-CLOUD-002 (per-provider required fields)
3. **Phase 3: Security** - F-CLOUD-003 (masking, safe logging)

## Cloud Provider Matrix

| Provider | URI Scheme | Credential Types | Env Vars |
|----------|-----------|------------------|----------|
| AWS S3 | `s3://` | Static keys, IAM role, STS | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_SESSION_TOKEN` |
| Azure ADLS | `az://`, `abfss://` | Account key, SAS token, Managed Identity | `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_STORAGE_SAS_TOKEN` |
| GCS | `gs://` | Service account JSON, Workload Identity | `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_SERVICE_ACCOUNT_KEY` |
| MinIO | `s3://` + custom endpoint | Static keys | Same as S3 + `AWS_ENDPOINT` |
| Local | `/path` or `file://` | None | None |

## Success Criteria

- [x] All 3 feature specifications created
- [x] Credential resolution works for S3, Azure, GCS, and local paths
- [x] Environment variable fallback resolves missing config keys
- [x] Per-provider validation catches missing required fields early
- [x] Secrets never appear in logs, Debug output, or error messages
- [x] Shared resolver used by F031 (Delta Lake) — F032, F033 pending
- [x] 82+ tests across all features (exceeded 40+ target)

## References

- [F031: Delta Lake Sink](../F031-delta-lake-sink.md)
- [F032: Iceberg Sink](../F032-iceberg-sink.md)
- [F033: Parquet Source](../F033-parquet-source.md)
- [object_store crate](https://docs.rs/object_store) - Unified cloud storage abstraction
- [deltalake storage options](https://delta-io.github.io/delta-rs/usage/loading-table/#storage-options)
