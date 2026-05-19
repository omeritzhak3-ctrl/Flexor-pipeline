# Email File Ingestion Pipeline — Take-Home Assignment

## Overview

You are building the ingestion head of a data platform. Your system receives email files from customers via cloud storage buckets and must prepare them for downstream normalization.

Your pipeline's job: **discover files, unpack containers, deduplicate, and produce a clean set of individual email files ready for normalization** — with full traceability to where each file came from.

**Use local filesystem as your "cloud storage" and any local persistence (SQLite, files, etc.) for state.** Your design should show awareness of what changes at production scale.

**Language:** Python
**Time:** 1-3 hours.

**Important:** You might not have time to implement everything. Read all the requirements first, then decide what to prioritize for P0. We want to see how you scope and make trade-offs. Document what you chose to implement, what you deferred, and why.

We encourage the use of AI tools. As part of your submission, include all specification documents, design docs, or planning files you generated during the process, and a brief description of how you used AI tools at each stage.

---

## 1. How Data Arrives

Customers push email files into their cloud bucket organized by date folders:

```
<namespace>/
  timestamp=2024-07-15/
    email_001.eml
    email_002.html
    batch_archive.zip
  timestamp=2024-07-16/
    email_003.eml
    another_archive.zip
    mailbox_dump.mbox
  timestamp=2024-07-17/
    email_004.msg
```

**Key constraints:**
- We do **not** know when the customer starts or finishes uploading to a date folder
- Multiple uploads can land in the same date folder at different times
- It must support both **backfill** (all historical data when a customer is first onboarded — runs once) and **incremental** (only new files, runs every 15 minutes)

### Supported email file formats

| Format | Type | Description |
|--------|------|-------------|
| **EML** | Single email | RFC 822 email message |
| **HTML** | Single email | HTML email export |
| **MSG** | Single email | Outlook message format |
| **MBOX** | Container | Mailbox archive — multiple emails in one file |
| **PST** | Container | Outlook personal storage — multiple emails |
| **ZIP** | Container | Archive that can contain any of the above, including other containers |

**Container formats** (ZIP, MBOX, PST) hold multiple emails or even other containers inside them. A ZIP can contain an MBOX which contains individual emails. Nesting can be arbitrarily deep.

Note: customers sometimes include irrelevant files in their uploads (images, spreadsheets, etc.) that cannot be parsed as emails.

---

## 2. Requirements

### P0 — Core Pipeline

#### File Discovery & Change Data Capture (CDC)

- Scan date-partitioned directories: `<namespace>/timestamp=YYYY-MM-DD/<files>`
- **Backfill mode**: Process all existing date directories when pipeline first runs
- **Incremental mode**: Process only new/unprocessed files on subsequent runs
- Pipeline state must survive restarts — if the pipeline crashes and restarts, it must not reprocess files it already handled

#### Container Unpacking

- Unpack container formats (ZIP, MBOX, PST) to discover individual email files inside
- Containers can hold other containers (e.g., ZIP containing MBOX files, ZIP containing ZIPs)
- Preserve full lineage: for every individual email file, you must be able to trace back exactly where it came from (which container, which path within that container, etc.)

#### Deduplication

- Design a unique identifier for each email file that your pipeline discovers
- The same email file must never be processed twice, whether it appears in the same batch or across different pipeline runs
- Important edge case: different container formats may extract files with identical filenames. For example, two different PST archives might each unpack emails as `001.eml`, `002.eml`, ..., `n.eml` — but these are NOT the same emails

#### Output

- A file system (directory structure or similar) containing the deduplicated individual email files, organized so downstream consumers can read them
- For each staged file:
  - Its unique identifier
  - Full lineage (tracing back to original source in the bucket)
  - The date partition it belongs to
- A record of any files that were skipped and why

#### Attachments

- Emails may contain attachments. Consider how your pipeline should handle attachments so they remain associated with their parent email and are accessible alongside it.

### P1 — Advanced

#### Scaling Considerations

- Think about what scaling problems this pipeline might encounter in production and how the design should adapt. Consider: large archives, thousands of files per partition, multiple customers, etc.
- Document your analysis and intended architecture to scale — no implementation needed.

---

## 3. Test Data

We provide a starter set of test fixtures in the `test_data/` directory. **You must add test fixtures and tests that cover the edge cases listed below.**

### Provided fixtures

```
test_data/
  namespace_a/
    timestamp=2024-07-15/
      simple_email.eml            # Basic EML file
      another_email.html          # HTML email export
      batch.zip                   # Contains: invoice.eml, receipt.eml
      conversations.mbox          # MBOX with 3 email messages
    timestamp=2024-07-16/
      new_email.eml               # Another EML file
```

### Edge cases to cover

You decide the correct behavior for each — document your decisions.

1. Same filename appears in different date partitions (e.g., `email.eml` in both `timestamp=2024-07-15/` and `timestamp=2024-07-16/`)
2. A ZIP containing another ZIP containing email files
3. Two different MBOXes that unpack into files with identical names (e.g., both produce `001.eml`, `002.eml`)
4. A password-protected ZIP
5. A corrupted/unreadable ZIP
6. Non-email files mixed in with emails (e.g., `.png`, `.xlsx`)
7. An empty container (ZIP or MBOX with no contents)
8. Pipeline crashes mid-run and restarts
9. Same file uploaded to the same date partition across two different pipeline runs
10. A deeply nested container chain (e.g., ZIP -> ZIP -> MBOX -> emails)

---

## 4. Deliverables

**Code**: A working Python project with:
- Pipeline implementation covering your P0 priorities
- Tests that demonstrate correctness for happy paths and edge cases
- Extended test fixtures for the edge cases above

**README** must include:
1. **Setup & Run**: How to install deps, run pipeline, and run tests
2. **Design Document**:
   - Architecture overview (diagram encouraged)
   - Your unique identifier design and why you chose it
   - CDC strategy (backfill vs incremental)
   - How container unpacking and deduplication interact
   - What you would change for production scale
3. **Scope decisions**: What you chose to implement, what you deferred, and why
4. **Edge case decisions**: For each edge case, what behavior you chose and why

**AI Process Documentation**: Include all spec docs, design docs, or planning files generated during the process. Describe how you used AI tools and at which stages.

**Submission**: Git repo with meaningful commit history showing your development process.
