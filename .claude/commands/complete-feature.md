# /complete-feature

Mark a feature as complete after validation.

## Usage

```
/complete-feature F{NNN}
```

## Example

```
/complete-feature F001
```

## Completion Checklist

The command runs through this checklist:

### Code Quality
- [ ] All code implemented per specification
- [ ] `cargo clippy -- -D warnings` passes
- [ ] `cargo fmt --check` passes
- [ ] No `unsafe` without `// SAFETY:` comment

### Testing
- [ ] Unit tests written and passing
- [ ] Integration tests written and passing
- [ ] Code coverage > 80% for new code
- [ ] Property tests for critical invariants

### Performance
- [ ] Benchmarks written
- [ ] Performance targets met
- [ ] No hot path regressions

### Documentation
- [ ] Public APIs documented with `///` comments
- [ ] README updated if needed
- [ ] CHANGELOG entry added
- [ ] ADR written for significant decisions

### Review
- [ ] Code reviewed (or self-reviewed with agents)
- [ ] Architect agent approved (if architectural changes)
- [ ] Security reviewer approved (if auth/security code)
- [ ] Performance auditor approved (if hot path code)

## Process

1. **Load Feature Spec**
   - Read from `docs/features/phase-{N}/F{XXX}-*.md`

2. **Run Checklist**
   - Go through each item
   - Note any failures

3. **If All Pass**
   - Update feature status to ✅ Done
   - Update INDEX.md
   - Add completion date

4. **If Any Fail**
   - List failing items
   - Suggest remediation steps
   - Keep status as 🚧 In Progress

## Output

```
## Feature Completion: F{XXX} - {Title}

### Checklist Results
| Item | Status |
|------|--------|
| Code implemented | ✅ |
| Tests passing | ✅ |
| ... | ... |

### Result: [COMPLETE / INCOMPLETE]

### Next Steps (if incomplete)
1. [Action needed]
```
