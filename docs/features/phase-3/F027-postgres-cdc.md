# F027: PostgreSQL CDC Source

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F027 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F001 |
| **Owner** | TBD |

## Summary

Capture changes from PostgreSQL using logical replication. Supports insert, update, delete operations with before/after images.

## Goals

- Logical replication slot consumption
- Full CDC events (insert/update/delete)
- Before and after images for updates
- Schema change handling

## Technical Design

```rust
pub struct PostgresCdcSource {
    client: Client,
    slot_name: String,
    publication: String,
}

pub enum CdcOperation {
    Insert { after: Row },
    Update { before: Option<Row>, after: Row },
    Delete { before: Row },
}
```

## SQL Syntax

```sql
CREATE SOURCE TABLE users_cdc (
    id BIGINT,
    name VARCHAR,
    email VARCHAR,
    _op VARCHAR,  -- I/U/D
    _ts TIMESTAMP
) WITH (
    connector = 'postgres-cdc',
    hostname = 'localhost',
    database = 'mydb',
    table = 'users',
    slot.name = 'laminardb_slot'
);
```

## Completion Checklist

- [ ] Replication slot consumption working
- [ ] All operation types captured
- [ ] Before/after images correct
- [ ] Schema changes handled
