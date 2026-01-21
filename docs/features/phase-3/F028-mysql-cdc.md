# F028: MySQL CDC Source

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F028 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F027 |
| **Owner** | TBD |

## Summary

Capture changes from MySQL using binlog replication. Similar to PostgreSQL CDC but using MySQL's binary log.

## Completion Checklist

- [ ] Binlog consumption working
- [ ] All operation types captured
- [ ] GTID tracking implemented
