# Roadmap

## Overview

LaminarDB development is organized into 5 phases, each building on the previous.

```
┌─────────────────────────────────────────────────────────────────┐
│                        Development Phases                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Phase 1        Phase 2        Phase 3        Phase 4    Phase 5│
│  ┌──────┐       ┌──────┐       ┌──────┐       ┌──────┐  ┌──────┐│
│  │ Core │──────▶│Harden│──────▶│Connect│─────▶│Secure│─▶│Admin ││
│  │Engine│       │ ing  │       │ ors  │       │      │  │      ││
│  └──────┘       └──────┘       └──────┘       └──────┘  └──────┘│
│                                                                 │
│  Reactor        Thread/Core    Kafka          RBAC      Dashboard│
│  State          Windows        CDC            ABAC      Metrics │
│  Windows        Exactly-Once   Lakehouse      Audit     Console │
│  DataFusion     Joins                         RLS       Health  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Core Engine

**Goal**: Build the foundational streaming engine with basic functionality.

**Success Criteria**:
- [ ] 500K events/sec/core throughput
- [ ] < 500ns state lookup p99
- [ ] Tumbling windows operational
- [ ] Basic WAL and checkpointing

### Features

| ID | Feature | Priority | Status |
|----|---------|----------|--------|
| F001 | Core Reactor Event Loop | P0 | 📝 Draft |
| F002 | Memory-Mapped State Store | P0 | 📝 Draft |
| F003 | State Store Interface | P0 | 📝 Draft |
| F004 | Tumbling Windows | P0 | 📝 Draft |
| F005 | DataFusion Integration | P0 | 📝 Draft |
| F006 | Basic SQL Parser | P0 | 📝 Draft |
| F007 | Write-Ahead Log | P1 | 📝 Draft |
| F008 | Basic Checkpointing | P1 | 📝 Draft |
| F009 | Event Time Processing | P1 | 📝 Draft |
| F010 | Watermarks | P1 | 📝 Draft |
| F011 | EMIT Clause | P2 | 📝 Draft |
| F012 | Late Data Handling | P2 | 📝 Draft |

---

## Phase 2: Production Hardening

**Goal**: Make the engine production-ready with advanced features.

**Success Criteria**:
- [ ] Thread-per-core architecture operational
- [ ] Sliding and session windows working
- [ ] Exactly-once semantics verified
- [ ] < 10 second checkpoint recovery

### Features

| ID | Feature | Priority | Status |
|----|---------|----------|--------|
| F013 | Thread-Per-Core Architecture | P0 | 📝 Draft |
| F014 | SPSC Queue Communication | P0 | 📝 Draft |
| F015 | CPU Pinning | P1 | 📝 Draft |
| F016 | Sliding Windows | P0 | 📝 Draft |
| F017 | Session Windows | P1 | 📝 Draft |
| F018 | Hopping Windows | P1 | 📝 Draft |
| F019 | Stream-Stream Joins | P0 | 📝 Draft |
| F020 | Lookup Joins | P0 | 📝 Draft |
| F021 | Temporal Joins | P2 | 📝 Draft |
| F022 | Incremental Checkpointing | P1 | 📝 Draft |
| F023 | Exactly-Once Sinks | P0 | 📝 Draft |
| F024 | Two-Phase Commit | P1 | 📝 Draft |

---

## Phase 3: Connectors & Integration

**Goal**: Connect to external systems for real-world data pipelines.

**Success Criteria**:
- [ ] Kafka source/sink operational
- [ ] PostgreSQL CDC working
- [ ] Delta Lake/Iceberg integration complete

### Features

| ID | Feature | Priority | Status |
|----|---------|----------|--------|
| F025 | Kafka Source Connector | P0 | 📝 Draft |
| F026 | Kafka Sink Connector | P0 | 📝 Draft |
| F027 | PostgreSQL CDC Source | P0 | 📝 Draft |
| F027B | PostgreSQL Sink | P0 | 📝 Draft |
| F028 | MySQL CDC Source | P1 | 📝 Draft |
| F029 | MongoDB CDC Source | P2 | 📝 Draft |
| F030 | Redis Lookup Table | P1 | 📝 Draft |
| F031 | Delta Lake Sink | P0 | 📝 Draft |
| F032 | Iceberg Sink | P1 | 📝 Draft |
| F033 | Parquet File Source | P2 | 📝 Draft |
| F034 | Connector SDK | P1 | 📝 Draft |

---

## Phase 4: Enterprise & Security

**Goal**: Add enterprise security features for production deployments.

**Success Criteria**:
- [ ] RBAC fully implemented
- [ ] ABAC policies working
- [ ] Row-level security operational
- [ ] Audit logging complete

### Features

| ID | Feature | Priority | Status |
|----|---------|----------|--------|
| F035 | Authentication Framework | P0 | 📝 Draft |
| F036 | JWT Authentication | P0 | 📝 Draft |
| F037 | mTLS Authentication | P1 | 📝 Draft |
| F038 | LDAP Integration | P2 | 📝 Draft |
| F039 | Role-Based Access Control | P0 | 📝 Draft |
| F040 | Attribute-Based Access Control | P1 | 📝 Draft |
| F041 | Row-Level Security | P0 | 📝 Draft |
| F042 | Column-Level Security | P2 | 📝 Draft |
| F043 | Audit Logging | P0 | 📝 Draft |
| F044 | Encryption at Rest | P1 | 📝 Draft |
| F045 | Key Management | P1 | 📝 Draft |

---

## Phase 5: Admin & Observability

**Goal**: Provide operational tools for managing LaminarDB.

**Success Criteria**:
- [ ] Admin dashboard deployed
- [ ] Real-time metrics working
- [ ] Query console functional
- [ ] Health checks operational

### Features

| ID | Feature | Priority | Status |
|----|---------|----------|--------|
| F046 | Admin REST API | P0 | 📝 Draft |
| F047 | Web Dashboard | P0 | 📝 Draft |
| F048 | Real-Time Metrics | P0 | 📝 Draft |
| F049 | SQL Query Console | P0 | 📝 Draft |
| F050 | Prometheus Export | P1 | 📝 Draft |
| F051 | OpenTelemetry Tracing | P1 | 📝 Draft |
| F052 | Health Check Endpoints | P0 | 📝 Draft |
| F053 | Alerting Integration | P2 | 📝 Draft |
| F054 | Configuration Management | P1 | 📝 Draft |
| F055 | CLI Tools | P1 | 📝 Draft |

---

## Timeline (Estimated)

| Phase | Duration | Dependencies |
|-------|----------|--------------|
| Phase 1 | 8-10 weeks | None |
| Phase 2 | 6-8 weeks | Phase 1 |
| Phase 3 | 6-8 weeks | Phase 2 |
| Phase 4 | 4-6 weeks | Phase 1 |
| Phase 5 | 4-6 weeks | Phase 1 |

**Note**: Phases 4 and 5 can be developed in parallel with Phases 2-3 since they build on Phase 1 independently.
