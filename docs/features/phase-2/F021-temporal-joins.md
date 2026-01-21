# F021: Temporal Joins

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F021 |
| **Status** | 📝 Draft |
| **Priority** | P2 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F020 |
| **Owner** | TBD |

## Summary

Join with versioned tables using point-in-time lookups. Temporal joins return the table value that was valid at the event's timestamp.

## SQL Syntax

```sql
SELECT o.*, r.rate
FROM orders o
JOIN currency_rates FOR SYSTEM_TIME AS OF o.ts r
    ON o.currency = r.currency;
```

## Completion Checklist

- [ ] Versioned lookup working
- [ ] SQL syntax complete
- [ ] Integration tests passing
