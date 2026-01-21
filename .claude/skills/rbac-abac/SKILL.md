---
name: rbac-abac
description: Authorization patterns including Role-Based Access Control (RBAC), Attribute-Based Access Control (ABAC), and Row-Level Security (RLS) for LaminarDB.
---

# RBAC & ABAC Skill

## Authorization Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Authorization Engine                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Request ──▶ [Authentication] ──▶ [Authorization] ──▶ Execute │
│                     │                    │                      │
│                     ▼                    ▼                      │
│              ┌───────────┐        ┌───────────┐                │
│              │   JWT /   │        │   RBAC    │                │
│              │   mTLS    │        │   ABAC    │                │
│              └───────────┘        │   RLS     │                │
│                                   └───────────┘                │
│                                         │                       │
│                                         ▼                       │
│                                  ┌───────────┐                 │
│                                  │  Policy   │                 │
│                                  │  Engine   │                 │
│                                  └───────────┘                 │
└─────────────────────────────────────────────────────────────────┘
```

## RBAC Implementation

```rust
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    // Database-level
    CreateDatabase,
    DropDatabase,
    
    // Table-level
    CreateTable,
    DropTable,
    Select,
    Insert,
    Update,
    Delete,
    
    // Query-level
    ExecuteQuery,
    CreateView,
    
    // Admin
    ManageUsers,
    ManageRoles,
    ViewMetrics,
}

#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub permissions: HashSet<Permission>,
    pub inherits: Vec<String>,  // Parent roles
}

pub struct RbacEngine {
    roles: HashMap<String, Role>,
    user_roles: HashMap<String, HashSet<String>>,
}

impl RbacEngine {
    pub fn check_permission(&self, user: &str, permission: Permission) -> bool {
        let user_roles = match self.user_roles.get(user) {
            Some(roles) => roles,
            None => return false,
        };
        
        for role_name in user_roles {
            if self.role_has_permission(role_name, &permission) {
                return true;
            }
        }
        false
    }
    
    fn role_has_permission(&self, role_name: &str, permission: &Permission) -> bool {
        let role = match self.roles.get(role_name) {
            Some(r) => r,
            None => return false,
        };
        
        if role.permissions.contains(permission) {
            return true;
        }
        
        // Check inherited roles
        for parent in &role.inherits {
            if self.role_has_permission(parent, permission) {
                return true;
            }
        }
        false
    }
}
```

## ABAC Implementation

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbacPolicy {
    pub id: String,
    pub name: String,
    pub effect: PolicyEffect,
    pub condition: PolicyCondition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyEffect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyCondition {
    // Attribute comparisons
    Equals { attribute: String, value: AttributeValue },
    In { attribute: String, values: Vec<AttributeValue> },
    
    // Logical operators
    And(Vec<PolicyCondition>),
    Or(Vec<PolicyCondition>),
    Not(Box<PolicyCondition>),
    
    // Time-based
    TimeRange { start: String, end: String },
    
    // Custom expression
    Expression(String),
}

pub struct AbacEngine {
    policies: Vec<AbacPolicy>,
}

impl AbacEngine {
    pub fn evaluate(&self, context: &PolicyContext) -> PolicyDecision {
        let mut applicable_policies = Vec::new();
        
        for policy in &self.policies {
            if self.condition_matches(&policy.condition, context) {
                applicable_policies.push(policy);
            }
        }
        
        // Deny takes precedence
        for policy in &applicable_policies {
            if matches!(policy.effect, PolicyEffect::Deny) {
                return PolicyDecision::Deny(policy.name.clone());
            }
        }
        
        // Check for any allow
        for policy in &applicable_policies {
            if matches!(policy.effect, PolicyEffect::Allow) {
                return PolicyDecision::Allow;
            }
        }
        
        PolicyDecision::Deny("no matching policy".into())
    }
}

#[derive(Debug, Clone)]
pub struct PolicyContext {
    pub user: String,
    pub user_attributes: HashMap<String, AttributeValue>,
    pub resource: String,
    pub resource_attributes: HashMap<String, AttributeValue>,
    pub action: String,
    pub environment: HashMap<String, AttributeValue>,
}
```

## Row-Level Security (RLS)

```rust
pub struct RlsEngine {
    policies: HashMap<String, Vec<RlsPolicy>>,  // table -> policies
}

#[derive(Debug, Clone)]
pub struct RlsPolicy {
    pub name: String,
    pub table: String,
    pub command: RlsCommand,
    pub filter_expr: Expr,  // DataFusion expression
}

#[derive(Debug, Clone)]
pub enum RlsCommand {
    Select,
    Insert,
    Update,
    Delete,
    All,
}

impl RlsEngine {
    pub fn apply_filters(
        &self,
        table: &str,
        command: RlsCommand,
        context: &SecurityContext,
    ) -> Option<Expr> {
        let policies = self.policies.get(table)?;
        
        let applicable: Vec<_> = policies
            .iter()
            .filter(|p| p.command == command || matches!(p.command, RlsCommand::All))
            .collect();
        
        if applicable.is_empty() {
            return None;
        }
        
        // Combine with OR (any policy allows access)
        let mut combined = applicable[0].filter_expr.clone();
        for policy in &applicable[1..] {
            combined = combined.or(policy.filter_expr.clone());
        }
        
        // Substitute user context
        Some(self.substitute_context(combined, context))
    }
}
```

## SQL Syntax

```sql
-- Create roles
CREATE ROLE analyst;
CREATE ROLE admin INHERIT analyst;

-- Grant permissions
GRANT SELECT ON TABLE orders TO analyst;
GRANT ALL ON TABLE orders TO admin;

-- Assign roles to users
GRANT analyst TO alice;
GRANT admin TO bob;

-- Set user attributes (for ABAC)
ALTER USER alice SET ATTRIBUTE department = 'trading';
ALTER USER alice SET ATTRIBUTE region = 'US-East';

-- Create ABAC policy
CREATE POLICY trading_access ON trades
    FOR SELECT
    USING (
        user_attribute('department') = 'trading'
        OR user_has_role('admin')
    );

-- Create RLS policy
CREATE POLICY region_filter ON trades
    FOR SELECT
    USING (region = current_user_attribute('region'));

-- Create time-based policy
CREATE POLICY business_hours ON sensitive_data
    FOR SELECT
    USING (
        current_time() BETWEEN '09:00' AND '17:00'
        AND current_day_of_week() BETWEEN 1 AND 5
    );
```

## Authentication Integration

```rust
// JWT Authentication
pub struct JwtAuthenticator {
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtAuthenticator {
    pub fn authenticate(&self, token: &str) -> Result<AuthenticatedUser, AuthError> {
        let token_data = decode::<Claims>(token, &self.decoding_key, &self.validation)?;
        
        Ok(AuthenticatedUser {
            id: token_data.claims.sub,
            roles: token_data.claims.roles,
            attributes: token_data.claims.attributes,
        })
    }
}

// mTLS Authentication
pub struct MtlsAuthenticator {
    ca_cert: Certificate,
}

impl MtlsAuthenticator {
    pub fn authenticate(&self, cert: &Certificate) -> Result<AuthenticatedUser, AuthError> {
        // Verify certificate chain
        cert.verify(&self.ca_cert)?;
        
        // Extract identity from CN or SAN
        let cn = cert.subject_common_name()?;
        
        Ok(AuthenticatedUser {
            id: cn,
            roles: self.lookup_roles(&cn)?,
            attributes: self.lookup_attributes(&cn)?,
        })
    }
}
```

## Configuration

```toml
[auth]
enabled = true
method = "jwt"  # or "mtls", "ldap"

[auth.jwt]
issuer = "https://auth.example.com"
audience = "laminardb"
public_key_path = "/etc/laminardb/jwt-public.pem"

[auth.mtls]
ca_cert_path = "/etc/laminardb/ca.crt"
require_client_cert = true

[authorization]
default_policy = "deny"  # deny or allow
enable_rls = true
enable_abac = true
```
