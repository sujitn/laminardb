# Getting Started with LaminarDB Development

This guide explains how to use the Claude Code scaffold for LaminarDB development.

## Initial Setup

### 1. Create the Repository

```bash
# Create new repository
mkdir laminardb && cd laminardb
git init

# Copy scaffold files (or extract from tar.gz)
tar -xzf laminardb-scaffold.tar.gz
mv laminardb-scaffold/* .
mv laminardb-scaffold/.claude .
rmdir laminardb-scaffold

# Initial commit
git add .
git commit -m "Initial project scaffold"
```

### 2. Initialize Claude Code

```bash
# Start Claude Code in the project
cd laminardb
claude

# Claude will automatically read CLAUDE.md
```

### 3. Check Status

```
> /status
```

This shows current phase, feature progress, and priorities.

---

## Development Workflow

### Starting a Session

1. **Claude reads context automatically** from:
   - `CLAUDE.md` (project overview)
   - `docs/STEERING.md` (priorities)
   - `docs/CONTEXT.md` (where we left off)

2. **Check what to work on**:
   ```
   > /status
   ```

3. **Review open items**:
   ```
   > Show me the current sprint focus from STEERING.md
   ```

### Creating a New Feature

1. **Generate specification**:
   ```
   > /new-feature F056 "Support for hopping windows"
   ```

2. **Review and refine the spec**:
   ```
   > Show me the spec for F056
   > Add a section about memory usage
   ```

3. **Start implementation**:
   ```
   > Let's implement F056 starting with the data structures
   ```

### Implementing a Feature

1. **Let Claude Read First**:
   ```
   > Read the spec for F001 and the thread-per-core skill
   ```

2. **Implement incrementally**:
   ```
   > Create the Reactor struct in crates/laminar-core/src/reactor/mod.rs
   > Now add the event loop method
   > Add unit tests for the reactor
   ```

3. **Run tests frequently**:
   ```
   > Run cargo test for the reactor module
   ```

### Code Review with Agents

After implementation, invoke specialized agents:

```
> Use the performance-auditor agent to check the reactor for hot path violations
```

```
> Use the architect agent to review the state store design
```

```
> Use the security-reviewer agent to audit the auth module
```

### Completing a Feature

1. **Run completion checklist**:
   ```
   > /complete-feature F001
   ```

2. **Address any failures**:
   ```
   > The benchmark target wasn't met. Let's optimize the hot path.
   ```

3. **Mark as done** (automatic if checklist passes)

### Ending a Session

**Always update CONTEXT.md**:
```
> Update CONTEXT.md with what we accomplished and where we left off
```

---

## Governance Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                      Development Flow                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. START SESSION                                               │
│     └─> Claude reads CLAUDE.md, STEERING.md, CONTEXT.md        │
│                                                                 │
│  2. CREATE FEATURE                                              │
│     └─> /new-feature F025 "Kafka Source"                       │
│         └─> feature-spec-writer agent generates spec           │
│         └─> Saved to docs/features/phase-3/F025-kafka-source.md│
│                                                                 │
│  3. IMPLEMENT                                                   │
│     └─> Skills auto-activate based on code being written       │
│     └─> Rules enforce quality (performance, security, style)   │
│     └─> Hooks warn about main branch, unsafe blocks            │
│                                                                 │
│  4. QUALITY GATES                                               │
│     └─> security-reviewer agent audits auth code               │
│     └─> performance-auditor validates benchmarks               │
│     └─> architect agent reviews design decisions               │
│                                                                 │
│  5. COMPLETE FEATURE                                            │
│     └─> /complete-feature F025                                 │
│         └─> Runs checklist (tests, docs, benchmarks)           │
│         └─> Updates INDEX.md                                   │
│                                                                 │
│  6. PHASE GATE                                                  │
│     └─> /gate-check phase-1                                    │
│         └─> stage-gate agent validates all features complete   │
│         └─> Checks success criteria met                        │
│         └─> Generates phase completion report                  │
│                                                                 │
│  7. END SESSION                                                 │
│     └─> Claude updates CONTEXT.md with current state           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Best Practices

### 1. Let Claude Read First
Before asking Claude to implement, let it read relevant context:
```
> Read the spec for F001 and the thread-per-core skill
```

### 2. Use Agents for Review
After implementation, invoke specialized agents:
```
> Use the performance-auditor agent to check for hot path violations
```

### 3. Keep Context Updated
At session end, always update CONTEXT.md:
```
> Update CONTEXT.md with what we accomplished and where we left off
```

### 4. Use ADRs for Decisions
Document non-obvious choices:
```
> /adr "FxHashMap vs hashbrown for state store"
```

### 5. Run /status Frequently
Stay oriented with the project status:
```
> /status
```

### 6. Trust the Skills
Skills contain domain expertise - Claude will apply them automatically when relevant.

---

## Troubleshooting

### Claude doesn't know about a feature
```
> Read docs/features/INDEX.md and the spec for F{NNN}
```

### Claude violates a rule
```
> Read .claude/rules/performance.md and apply it to this code
```

### Need to see all available context
```
> Show me what's in CLAUDE.md and STEERING.md
```

### Feature seems stuck
```
> What's blocking F{NNN}? Check the dependencies in INDEX.md
```
