# Data Dictionary

## Canonical fields we're standardizing toward (target Silver schema)

| Canonical field         | Meaning                                                         |
|--------------------------|-----------------------------------------------------------------|
| job_id                   | Unique ID, source-prefixed (e.g. `remotive_12345`)               |
| source                   | Which API this came from                                        |
| title                    | Job title                                                        |
| company_name             | Employer name                                                   |
| location_raw             | Location as the source wrote it (unstandardized)                 |
| is_remote                | Boolean, normalized across sources                                |
| tags_raw                 | Skills/tags as the source provided them                          |
| posted_date               | When the posting was published                                    |
| salary_min / salary_max  | If available (many postings won't have this)                      |
| description               | Full text (may contain HTML)                                      |
| apply_url                 | Link to apply                                                     |
| ingested_at               | When *we* pulled this record (our own field, not the source's)    |

## Known per-source field mapping (CONFIRMED against real API output, 2026-09-15)

| Canonical field | Remotive field | Arbeitnow field | RemoteOK field |
|---|---|---|---|
| job_id | `id` | `slug` | `id` |
| title | `title` | `title` | `position` |
| company_name | `company_name` | `company_name` | `company` |
| location_raw | `candidate_required_location` | `location` | `location` |
| is_remote | (implicit — always true) | `remote` (bool) | (no field — implicit true, same as Remotive) |
| tags_raw | `tags` | `tags` | `tags` |
| posted_date | `publication_date` | `created_at` | `date` (also `epoch`, unix int) |
| salary | `salary` (free text, not min/max) | (not present) | `salary_min` / `salary_max` (structured) |
| description | `description` | `description` | `description` |
| apply_url | `url` | `url` | `apply_url` |

## Known quirks (confirmed)

- **RemoteOK**: raw array element `[0]` is an API-terms/legal notice, not a
  job — confirmed. Filtering on `"id" in item` correctly drops it (100 raw
  elements -> 99 real jobs).
- **RemoteOK has no `remote` field at all**. RemoteOK is a remote-jobs-only
  board, same as Remotive: `is_remote` is hardcoded `true` for BOTH these
  sources, and Arbeitnow's `remote` boolean is our only source with genuine
  variation.
- **Salary shape differs structurally, not just by name**: Remotive gives a
  single free-text `salary` string (e.g. could be `"$90k - $110k"` or empty);
  RemoteOK gives structured `salary_min`/`salary_max` ints; Arbeitnow has no
  salary field at all. Silver (Stage 7) will need to parse Remotive's text
  field to extract min/max where possible, and accept nulls everywhere else
  — this is a real, non-trivial cleaning step, not a formality.
- **RemoteOK also provides `epoch`** (unix timestamp) alongside `date` (ISO
  string) — we'll standardize on parsing `date`, but `epoch` is a handy
  cross-check if a date string ever fails to parse.

## Architectural notes carried from Stage 3/5

- These three APIs only return currently-active postings — a snapshot, not
  a historical archive. Historical depth is built two ways: ongoing daily
  incremental landing (Stage 3 onward) and a one-time historical seed
  dataset planned for Stage 6 (Bronze).
- Raw landing zone is a Databricks Unity Catalog Volume, not local disk or
  MinIO — Databricks Free Edition compute cannot reach a local machine, and
  a Volume is free, cloud-reachable storage under Free Edition without
  needing a separate AWS account.
- Databricks Free Edition serverless compute restricts outbound internet
  access to a limited set of trusted domains with no account-level policy
  console to change it — ingestion (which calls Remotive/Arbeitnow/RemoteOK)
  therefore runs on a local machine, never as Databricks notebook/job code.
