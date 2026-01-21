# /new-feature

Create a new feature specification.

## Usage

```
/new-feature F{NNN} "{Feature Title}"
```

## Example

```
/new-feature F056 "Support for hopping windows"
```

## Process

1. **Generate Feature ID**
   - Check `docs/features/INDEX.md` for next available ID
   - Use format `FXXX` (zero-padded)

2. **Determine Phase**
   - Check feature description against phase definitions
   - Assign to appropriate phase (1-5)

3. **Create Specification**
   - Use the feature-spec-writer agent
   - Generate from `docs/features/TEMPLATE.md`
   - Save to `docs/features/phase-{N}/F{XXX}-{slug}.md`

4. **Update INDEX.md**
   - Add entry to feature index
   - Set initial status as 📝 Draft

5. **Identify Dependencies**
   - Check for prerequisite features
   - Update dependency graph

## Output

- Feature spec file created
- INDEX.md updated
- Dependencies identified
