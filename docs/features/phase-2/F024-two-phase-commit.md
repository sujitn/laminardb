# F024: Two-Phase Commit

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F024 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F023 |
| **Owner** | TBD |

## Summary

Implement distributed two-phase commit for exactly-once across multiple sinks. Coordinates atomic commits when writing to multiple destinations.

## Goals

- Coordinator/participant protocol
- Timeout and recovery handling
- Presumed abort semantics
- Logging for crash recovery

## Technical Design

```rust
pub struct TwoPhaseCoordinator {
    participants: Vec<Box<dyn TwoPhaseParticipant>>,
    log: TransactionLog,
}

impl TwoPhaseCoordinator {
    pub async fn commit(&mut self, tx: Transaction) -> Result<()> {
        // Phase 1: Prepare
        self.log.write(TxState::Preparing(tx.id));
        for p in &mut self.participants {
            p.prepare(&tx).await?;
        }
        
        // Phase 2: Commit
        self.log.write(TxState::Committing(tx.id));
        for p in &mut self.participants {
            p.commit(&tx).await?;
        }
        
        self.log.write(TxState::Committed(tx.id));
        Ok(())
    }
}
```

## Completion Checklist

- [ ] 2PC protocol implemented
- [ ] Crash recovery working
- [ ] Timeout handling tested
- [ ] Multiple participants coordinated
