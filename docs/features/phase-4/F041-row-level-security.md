# F041: Row-Level Security

## Metadata
| Field | Value |
|-------|-------|
| **ID** | F041 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 4 |
| **Dependencies** | F039 |

## Summary
Automatically filter rows based on user context.

## SQL Syntax
```sql
CREATE POLICY region_filter ON orders
    FOR SELECT
    USING (region = current_user_attribute('region'));
```

## Completion Checklist
- [ ] Row filtering working
- [ ] Filter pushdown optimized
