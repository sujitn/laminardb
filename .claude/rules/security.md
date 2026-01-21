---
paths:
  - "crates/laminar-auth/**/*.rs"
  - "crates/laminar-admin/**/*.rs"
  - "crates/laminar-server/**/*.rs"
---

# Security Requirements

## Authentication

### JWT Validation

```rust
// Always validate:
// 1. Signature (using configured public key)
// 2. Expiration (exp claim)
// 3. Issuer (iss claim matches expected)
// 4. Audience (aud claim matches this service)

fn validate_jwt(token: &str, config: &AuthConfig) -> Result<Claims, AuthError> {
    let validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[&config.expected_issuer]);
    validation.set_audience(&[&config.service_name]);
    
    let token_data = decode::<Claims>(
        token,
        &config.decoding_key,
        &validation,
    )?;
    
    Ok(token_data.claims)
}
```

### mTLS for Service-to-Service

```rust
// Require client certificates for internal APIs
let tls_config = ServerConfig::builder()
    .with_client_cert_verifier(
        AllowAnyAuthenticatedClient::new(root_cert_store)
    )
    .with_single_cert(server_certs, server_key)?;
```

## Authorization

### RBAC Enforcement

```rust
// Check permissions before every operation
fn execute_query(&self, ctx: &Context, query: &str) -> Result<QueryResult> {
    // 1. Check role has query permission
    self.authz.require_permission(ctx.user(), Permission::Query)?;
    
    // 2. Check table-level access
    for table in parse_tables(query)? {
        self.authz.require_table_access(ctx.user(), &table, Access::Read)?;
    }
    
    // 3. Apply RLS filters
    let filtered_query = self.rls.apply_filters(ctx.user(), query)?;
    
    self.execute_internal(filtered_query)
}
```

### ABAC Policy Evaluation

```rust
// Evaluate attribute-based policies
fn check_policy(&self, ctx: &PolicyContext) -> PolicyDecision {
    for policy in &self.policies {
        match policy.evaluate(ctx) {
            PolicyResult::Allow => return PolicyDecision::Allow,
            PolicyResult::Deny(reason) => return PolicyDecision::Deny(reason),
            PolicyResult::NotApplicable => continue,
        }
    }
    PolicyDecision::Deny("no matching policy".into())
}
```

## Input Validation

### SQL Injection Prevention

```rust
// NEVER interpolate user input into SQL
// BAD:
let query = format!("SELECT * FROM {} WHERE id = {}", table, id);

// GOOD: Use parameterized queries
let query = "SELECT * FROM $1 WHERE id = $2";
let result = client.query(query, &[&table, &id])?;

// GOOD: Whitelist table names
fn validate_table_name(name: &str) -> Result<&str, ValidationError> {
    const ALLOWED: &[&str] = &["events", "users", "metrics"];
    if ALLOWED.contains(&name) {
        Ok(name)
    } else {
        Err(ValidationError::InvalidTable(name.to_string()))
    }
}
```

### Size Limits

```rust
// Enforce limits on all inputs
const MAX_QUERY_SIZE: usize = 1_000_000;  // 1MB
const MAX_BATCH_SIZE: usize = 10_000;
const MAX_KEY_SIZE: usize = 1024;
const MAX_VALUE_SIZE: usize = 10_000_000;  // 10MB

fn validate_query(query: &str) -> Result<(), ValidationError> {
    if query.len() > MAX_QUERY_SIZE {
        return Err(ValidationError::QueryTooLarge);
    }
    Ok(())
}
```

## Secrets Management

```rust
// Never log secrets
// BAD:
tracing::info!("Connecting with password: {}", password);

// GOOD: Use secret wrapper
pub struct Secret<T>(T);

impl<T> std::fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[REDACTED]")
    }
}

// Never hardcode secrets
// BAD:
const API_KEY: &str = "sk-1234567890";

// GOOD: Load from environment or secret store
let api_key = std::env::var("API_KEY")
    .map_err(|_| ConfigError::MissingSecret("API_KEY"))?;
```

## Error Handling

```rust
// Never expose internal details in error messages
// BAD:
Err(format!("Database error: {}", internal_error))

// GOOD: Log details internally, return generic error
tracing::error!(error = ?internal_error, "Database query failed");
Err(ApiError::InternalError)  // Generic message to client
```

## Audit Logging

```rust
// Log all security-relevant operations
fn grant_permission(&self, ctx: &Context, user: &str, perm: Permission) -> Result<()> {
    // Perform the operation
    self.authz.grant(user, perm)?;
    
    // Audit log
    tracing::info!(
        target: "audit",
        actor = %ctx.user(),
        action = "grant_permission",
        subject = %user,
        permission = ?perm,
        "Permission granted"
    );
    
    Ok(())
}
```
