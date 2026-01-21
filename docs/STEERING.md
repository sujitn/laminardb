# Steering Document

> Last Updated: January 2026

## Current Focus

**Phase 1: Core Engine** - Building the foundational streaming engine

### Sprint Priority

1. **F001 - Core Reactor Event Loop** (P0)
   - Single-threaded event loop foundation
   - Must achieve 500K events/sec baseline

2. **F003 - State Store Interface** (P0)
   - Key-value storage for operator state
   - Target: < 500ns lookup latency

3. **F004 - Tumbling Windows** (P0)
   - First window type implementation
   - Validates operator framework

## Active Decisions

### Decided

| Decision | Choice | Rationale | ADR |
|----------|--------|-----------|-----|
| Hash map implementation | FxHashMap | Faster than std HashMap for small keys | ADR-001 |
| Serialization format | bincode | Fastest for Rust types | ADR-002 |
| Async runtime | tokio (Ring 1 only) | Industry standard, not on hot path | - |

### Pending

| Decision | Options | Deadline | Owner |
|----------|---------|----------|-------|
| WAL format | Custom vs RocksDB | End of Phase 1 | TBD |
| Timer wheel implementation | Hierarchical vs Hashed | Week 2 | TBD |

## Blocked Items

| Item | Blocker | Unblock Action |
|------|---------|----------------|
| F003 State Store | F001 Reactor | Complete reactor first |
| Benchmarking | CI setup | Set up benchmark CI |

## Technical Debt

- [ ] Add property-based tests for serialization
- [ ] Document unsafe blocks in core crate
- [ ] Add tracing spans for debugging

## Team Agreements

### Code Review

- All Ring 0 code requires performance review
- Security code requires security review
- Architecture changes need architect sign-off

### Definition of Done

- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests if applicable
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG entry added

## Upcoming Milestones

| Milestone | Target Date | Status |
|-----------|-------------|--------|
| Phase 1 Complete | TBD | 🚧 In Progress |
| First public demo | TBD | 📋 Planned |
| Phase 2 Start | TBD | 📋 Planned |

## Communication

- **Daily**: Quick status in CONTEXT.md
- **Weekly**: Update STEERING.md priorities
- **Phase End**: Full retrospective and planning
