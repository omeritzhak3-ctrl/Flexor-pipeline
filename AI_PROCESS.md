# AI Process

This document records how AI tooling (Cursor / Claude) was used to build
the email ingestion pipeline. The goal is transparency: where the AI
contributed substantively, where I (the human) made decisions, and what
the review gate looked like at each step.

## Tools used

- **Cursor IDE** with the Claude model as the implementation pair-
  programmer.
- The repository's existing `git` and `pytest` tooling for verification.

## Stages

### 1. Planning

I started by feeding the assignment spec (`assignment (2).md`) into a
Cursor planning conversation and asked the AI to draft a phased
implementation plan. The output (`.cursor/plans/email-ingestion-
pipeline_5583be93.plan.md`) captured:

- **Locked design decisions** — the things I wanted pinned before any
  code was written: tenant-scoped identity formula, two-step CDC,
  content-addressed pool layout, attachments staying embedded, MSG/PST
  recognized-but-deferred, MAX_DEPTH = 8.
- **The SQLite schema** — `source_files`, `emails`, `lineage`, `skipped`
  with their indices.
- **On-disk staging layout** with a sample manifest line.
- **The crash-safety protocol** — `in_progress` breadcrumb, atomic
  rename, single per-source DB transaction.
- **Edge-case decisions table** — one decision per assignment edge case.
- **P1 production architecture** notes.
- **A phased TODO list** (Phase 0 through Phase 7), each ending with
  green tests.

I reviewed the plan, pushed back on the parts I disagreed with, and
locked the rest as the spec for implementation. The final plan file is
checked in alongside the code.

### 2. Scaffolding (Phase 0)

The AI generated:

- `pyproject.toml` with the `[dev]` (`pytest`) and `[msg]` (`extract-
  msg`) extras and a console-script entry.
- The full `src/email_ingest/` package skeleton — one placeholder module
  per layer the plan calls for, so subsequent phases had a stable
  namespace to fill in.
- `src/email_ingest/config.py` populated with the two constants the
  whole project would need (`MAX_CONTAINER_DEPTH`, `MAX_MEMBER_SIZE_BYTES`)
  and a `PipelineConfig` dataclass.
- `.gitignore` updates for `state/` and pytest artifacts.
- README + AI_PROCESS skeletons.
- A trivial smoke test confirming `import email_ingest` works.

### 3. Implementation (Phases 1–7)

Each phase followed the same loop:

1. The AI proposed the implementation against the plan's spec.
2. I reviewed each file, asked for changes where the design diverged
   from what we agreed (e.g., I had the AI re-do the CDC verdict shape
   when the original interface mixed up "did the cheap path read bytes"
   with "should we re-process").
3. The AI wrote unit tests pinning the documented behavior.
4. `pytest` was run; failures were diagnosed and fixed before the phase
   could be committed.
5. **I personally approved each phase before it landed on the branch.**
   Every commit is reviewed-by-me.

A few specific examples where the AI's first attempt needed correction:

- **Password-protected ZIP test.** The AI's first attempt flipped the
  "encrypted" general-purpose bit only on the local-file header. The
  test failed because `zipfile.ZipFile` reads flags from the *central
  directory*. I had the AI generalize the helper to flip both
  signatures (`PK\x03\x04` and `PK\x01\x02`).
- **Test packaging.** The AI initially created `tests/__init__.py`,
  which broke `from conftest import ...` in Phase 6. We removed the
  `__init__.py` to align with the standard pytest convention.
- **CDC verdict for `in_progress` / `failed`.** The first draft would
  have routed an `in_progress` row with matching hash to
  `METADATA_ONLY`, which would have silently skipped re-processing
  after a crash. I had the AI tighten the rule to "only `done` rows
  qualify for the cheap-path or METADATA_ONLY outcomes; everything else
  routes to CHANGED" and added explicit unit tests for both cases.

### 4. Testing

The AI was particularly useful for *generating* the programmatic test
fixtures — every ZIP, MBOX, encrypted-flag-forged ZIP, and edge-case
bucket is rebuilt from scratch in `tests/conftest.py`. This means the
repo carries no binary blobs and each test's input is auditable in
plaintext.

Test coverage matrix produced:

| Module | Tests |
|---|---|
| `identity` | 14 |
| `state` | 7 |
| `scanner` | 8 |
| `cdc` | 7 |
| `unpacker/zip` | 13 |
| `unpacker/mbox` | 7 |
| `unpacker/registry` | 16 |
| `staging` | 8 |
| `pipeline` (happy path) | 6 |
| `pipeline` (edge cases) | 12 |
| `crash_recovery` | 6 |
| `cli` | 4 |
| **Total** | **108 → 124** (grew as phases stacked) |

### 5. Documentation

The README in this commit was written by the AI from the plan, with me
editing the scope-decisions and P1 sections to match what I actually
shipped vs. deferred. The architecture mermaid diagram came straight
from the plan and survived unchanged.

## What the AI did *not* do

- **Make any commits autonomously.** Every commit was triggered by me
  saying "commit and continue" after reviewing the phase output.
- **Choose the locked design decisions.** Identity formula, CDC two-step
  vs. continuous polling, content-addressed pool, attachments-embedded
  — all called from the assignment + my own judgment and pinned in the
  plan before code was written.
- **Decide scope.** The MSG/PST defer, MIME-sniff defer, and streamed-
  extraction defer were my calls, made against the 1–3h time budget.

## What this means for review

If you're reading the code and wondering "why is this written this way?":
each design decision traces back either to the assignment spec, the
`.cursor/plans/email-ingestion-pipeline_5583be93.plan.md` file, or a
docstring at the top of the module that explains the choice. The plan
file is the single source of truth for design intent; the README is the
user-facing summary; this file is the process record.
