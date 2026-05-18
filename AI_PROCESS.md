# AI Process

This document records how AI tools were used while building the email
ingestion pipeline. It is updated as each phase lands.

## Stages

### Planning

- Used Cursor / an LLM as a thinking partner to turn the assignment spec
  (`assignment (2).md`) into a phased implementation plan
  (`.cursor/plans/email-ingestion-pipeline_5583be93.plan.md`).
- The plan covers locked design decisions (identity, CDC, output format),
  the SQLite schema, on-disk staging layout, the crash-safety protocol, the
  edge-case decisions table, and the phased TODO list.

### Scaffolding (Phase 0)

- Used the assistant to generate the `pyproject.toml`, package skeleton,
  `.gitignore` additions, and these documentation skeletons in one pass so
  later phases have a stable shape to fill in.

### To be filled in as later phases land

- Phase 1: SQLite schema, identity rules, unit tests.
- Phase 2: scanner + CDC.
- Phase 3: unpacker registry.
- Phase 4: staging + orchestrator.
- Phase 5: crash recovery.
- Phase 6: edge-case fixtures + tests.
- Phase 7: CLI + full README.
