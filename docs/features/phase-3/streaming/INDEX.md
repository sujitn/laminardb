# Streaming API Feature Index

> **Phase**: 3 - Connectors & Integration
> **Status**: 📝 Draft (All Tier 1 specs complete)
> **Reference**: [Research Document](../../../research/laminardb-streaming-api-research.md)

## Overview

LaminarDB's in-memory streaming API provides **embedded Kafka Streams-like semantics** without external dependencies.

### Core Principles

1. **Zero overhead by default** - Checkpointing disabled unless enabled
2. **Zero configuration for channel types** - Automatically derived from topology
3. **Composable settings** - No artificial tiers, just independent options
4. **Future-proof** - Clean extension points for Kafka, CDC

## Feature Summary

| Tier | Total | Draft | In Progress | Done |
|------|-------|-------|-------------|------|
| Tier 1 (Core) | 7 | 0 | 0 | 7 |
| Tier 2 (Production) | 6 | 5 | 0 | 1 |
| Tier 3 (Cross-Language) | 3 | 0 | 0 | 0 |

---

## Tier 1: Core (P0) - 1-2 weeks

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-STREAM-001 | Ring Buffer | P0 | ✅ | [Link](F-STREAM-001-ring-buffer.md) |
| F-STREAM-002 | SPSC Channel | P0 | ✅ | [Link](F-STREAM-002-spsc-channel.md) |
| F-STREAM-003 | MPSC Auto-Upgrade | P0 | ✅ | [Link](F-STREAM-003-mpsc-upgrade.md) |
| F-STREAM-004 | Source | P0 | ✅ | [Link](F-STREAM-004-source.md) |
| F-STREAM-005 | Sink | P0 | ✅ | [Link](F-STREAM-005-sink.md) |
| F-STREAM-006 | Subscription | P0 | ✅ | [Link](F-STREAM-006-subscription.md) |
| F-STREAM-007 | SQL DDL | P0 | ✅ | [Link](F-STREAM-007-sql-ddl.md) |

### Tier 1 Constraints
- Block backpressure only
- SpinYield wait strategy only
- No checkpointing

---

## Tier 2: Production (P1) - 2-3 weeks

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-STREAM-010 | Broadcast Channel | P1 | 📝 | [Link](F-STREAM-010-broadcast-channel.md) |
| F-STREAM-011 | All Backpressure | P1 | 📝 | TBD |
| F-STREAM-012 | All Wait Strategies | P1 | 📝 | TBD |
| F-STREAM-013 | Checkpointing | P1 | ✅ | [Link](F-STREAM-013-checkpointing.md) |
| F-STREAM-014 | WAL | P1 | 📝 | TBD |
| F-STREAM-015 | Recovery | P1 | 📝 | TBD |

---

## Tier 3: Cross-Language (P1/P2) - 3-4 weeks

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-STREAM-020 | C ABI | P2 | 📝 | TBD |
| F-STREAM-021 | Java Bindings | P2 | 📝 | TBD |
| F-STREAM-022 | Python Bindings | P2 | 📝 | TBD |

---

## Dependency Graph

```
Tier 1 (Core):
F-STREAM-001 (Ring Buffer)
       │
       ├──► F-STREAM-002 (SPSC) ──► F-STREAM-003 (MPSC Upgrade)
       │                                    │
       │                                    ▼
       │                           F-STREAM-004 (Source)
       │                                    │
       └──► F-STREAM-005 (Sink) ◄───────────┘
                   │
                   ▼
            F-STREAM-006 (Subscription)

F-STREAM-007 (SQL DDL) ──► integrates with F-STREAM-004, F-STREAM-005

Tier 2 (Production):
F-STREAM-005 ──► F-STREAM-010 (Broadcast)

F-STREAM-001 ──► F-STREAM-013 (Checkpointing) ──► F-STREAM-014 (WAL)
                                                          │
                                                          ▼
                                                  F-STREAM-015 (Recovery)

Tier 3 (Cross-Language):
All of above ──► F-STREAM-020 (C ABI) ──┬──► F-STREAM-021 (Java)
                                        │
                                        └──► F-STREAM-022 (Python)
```

---

## Configuration Summary

### What Users Configure

| Setting | Default | Description |
|---------|---------|-------------|
| buffer_size | 65536 | Ring buffer slots |
| backpressure | Block | Full buffer behavior |
| wait_strategy | SpinYield(100) | Empty buffer wait |
| checkpoint_interval | None (disabled) | Snapshot frequency |
| wal_mode | None (disabled) | Write-ahead log |
| watermark | None | Event-time processing |

### What's Automatic (NEVER User-Configurable)

| Aspect | How Derived |
|--------|-------------|
| SPSC vs MPSC | Auto-upgrade on `source.clone()` |
| SPSC vs Broadcast (sink) | Query plan analysis |
| Partitioning | GROUP BY keys |

---

## Quick Reference

### Rust API

```rust
// Open database
let db = LaminarDB::open()?;

// Single producer (SPSC)
let source = db.source::<Trade>("trades")?;
source.push(trade)?;

// Multiple producers (Auto MPSC)
let src2 = source.clone();  // Triggers upgrade
std::thread::spawn(move || src2.push(trade2));

// Consumer
let sink = db.sink::<OHLCBar>("ohlc_1min")?;
for batch in sink.subscribe() {
    process(batch);
}
```

### SQL DDL

```sql
-- Source (no channel specification)
CREATE SOURCE trades (
    symbol VARCHAR NOT NULL,
    price DOUBLE NOT NULL,
    ts TIMESTAMP NOT NULL,
    WATERMARK FOR ts AS ts - INTERVAL '100ms'
) WITH (
    buffer_size = 131072,
    checkpoint_interval = '10 seconds'
);

-- Sink (channel auto-derived)
CREATE SINK ohlc_output FROM ohlc_1min;
```

---

## Performance Targets

| Metric | Target | Feature |
|--------|--------|---------|
| Ring buffer push | < 50ns | F-STREAM-001 |
| SPSC channel | < 50ns | F-STREAM-002 |
| MPSC channel | < 150ns | F-STREAM-003 |
| Source push | < 100ns | F-STREAM-004 |
| Subscription poll | < 50ns | F-STREAM-006 |
| Throughput/core | > 10M ops/sec | All |

---

## References

- [Streaming API Research](../../../research/laminardb-streaming-api-research.md)
- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)
- [Phase 2: F014 SPSC Queues](../../phase-2/F014-spsc-queues.md)
- [Phase 2: F071 Zero-Allocation](../../phase-2/F071-zero-allocation-enforcement.md)
