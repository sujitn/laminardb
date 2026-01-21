# F{XXX}: {Feature Title}

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F{XXX} |
| **Status** | 📝 Draft |
| **Priority** | P{0-3} |
| **Phase** | {1-5} |
| **Effort** | {S/M/L/XL} |
| **Dependencies** | {F001, F002, ...} |
| **Owner** | TBD |
| **Created** | {YYYY-MM-DD} |
| **Updated** | {YYYY-MM-DD} |

## Summary

{One paragraph explaining what this feature does and why it's needed}

## Goals

- {Primary goal}
- {Secondary goal}
- {Tertiary goal}

## Non-Goals

- {What this feature explicitly does NOT do}
- {Scope boundaries}

## Technical Design

### Architecture

{Which ring? Which crate? How does it fit into the system?}

### API/Interface

```rust
/// {Brief description}
pub trait {TraitName} {
    /// {Method description}
    fn method(&self, param: Type) -> Result<Output, Error>;
}
```

### Data Structures

```rust
/// {Description}
pub struct {StructName} {
    /// {Field description}
    pub field: Type,
}
```

### Algorithm/Flow

1. {Step 1}
2. {Step 2}
3. {Step 3}

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| {ErrorType} | {When it occurs} | {How to handle} |

## Test Plan

### Unit Tests

- [ ] `test_{function}_{scenario}_returns_{expected}`
- [ ] `test_{function}_{error_case}_returns_error`

### Integration Tests

- [ ] End-to-end: {scenario description}
- [ ] Failure mode: {failure scenario}

### Property Tests

- [ ] {Invariant that should always hold}

### Benchmarks

- [ ] `bench_{operation}` - Target: {X}μs
- [ ] `bench_{throughput}` - Target: {Y}/sec

## Rollout Plan

1. **Phase 1**: Implementation + unit tests
2. **Phase 2**: Integration tests
3. **Phase 3**: Benchmarks + optimization
4. **Phase 4**: Documentation
5. **Phase 5**: Code review + merge

## Open Questions

- [ ] {Question that needs resolution}

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## Notes

{Free-form notes, decisions, observations}

## References

- {Link to relevant documentation}
- {Link to related features}
- {Link to ADRs}
