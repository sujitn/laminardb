# /gate-check

Validate phase completion criteria.

## Usage

```
/gate-check phase-{N}
```

## Example

```
/gate-check phase-1
```

## Process

1. **Load Phase Definition**
   - Read phase success criteria from stage-gate agent
   - Load all features for the phase from INDEX.md

2. **Feature Status Check**
   - Verify all phase features are ✅ Done
   - Check for any blockers or dependencies

3. **Success Criteria Validation**
   - Run benchmarks to verify performance targets
   - Check test coverage
   - Verify documentation completeness

4. **Generate Report**
   - Use stage-gate agent for validation
   - Produce detailed phase completion report

## Phase Criteria Summary

### Phase 1: Core Engine
- 500K events/sec/core throughput
- < 500ns state lookup p99
- Tumbling windows working
- Basic checkpointing

### Phase 2: Production Hardening  
- Thread-per-core architecture
- Sliding/session windows
- Exactly-once semantics
- 10-second recovery

### Phase 3: Connectors
- Kafka source/sink
- PostgreSQL CDC
- Lakehouse integration

### Phase 4: Enterprise
- RBAC/ABAC complete
- Row-level security
- Audit logging
- Security audit passed

### Phase 5: Admin
- Dashboard deployed
- Metrics working
- Query console functional

## Output

```
## Phase Gate: Phase {N}

### Status: [PASS / FAIL]

### Feature Completion
| Feature | Status |
|---------|--------|
| FXXX | ✅/🚧/📝 |

### Success Criteria
| Criterion | Target | Actual | Status |
|-----------|--------|--------|--------|
| Throughput | 500K/s | 520K/s | ✅ |

### Blockers
[List any blocking issues]

### Recommendation
[Proceed / Hold / Action needed]
```
