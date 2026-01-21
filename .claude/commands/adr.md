# /adr

Create an Architecture Decision Record.

## Usage

```
/adr "{Decision Title}"
```

## Example

```
/adr "Use FxHashMap instead of std HashMap for state store"
```

## ADR Template

```markdown
# ADR-{NNN}: {Title}

## Status

[Proposed | Accepted | Deprecated | Superseded by ADR-XXX]

## Date

{YYYY-MM-DD}

## Context

What is the issue that we're seeing that is motivating this decision?

## Decision

What is the change that we're proposing and/or doing?

## Consequences

What becomes easier or more difficult to do because of this change?

### Positive
- ...

### Negative
- ...

### Neutral
- ...

## Alternatives Considered

### Alternative 1: {Name}
- Description
- Pros
- Cons
- Why rejected

### Alternative 2: {Name}
- ...

## References

- [Link to relevant documentation]
- [Link to benchmarks]
- [Related ADRs]
```

## Process

1. **Generate ADR Number**
   - Check `docs/adr/` for existing ADRs
   - Assign next sequential number

2. **Create ADR File**
   - Save as `docs/adr/ADR-{NNN}-{slug}.md`

3. **Document Decision**
   - Fill in context, decision, consequences
   - List alternatives considered
   - Include supporting evidence (benchmarks, etc.)

4. **Link Related Work**
   - Reference from relevant feature specs
   - Update ARCHITECTURE.md if significant

## When to Write an ADR

- Choosing between competing approaches
- Deviating from common patterns
- Making irreversible decisions
- Trade-offs with significant consequences
- Decisions others might question later

## Output

ADR file created at `docs/adr/ADR-{NNN}-{slug}.md`
