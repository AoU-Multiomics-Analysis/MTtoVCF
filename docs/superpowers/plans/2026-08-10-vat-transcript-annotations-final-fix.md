# VAT Transcript Annotation Final Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve the final Hail correctness findings for composite-key VAT inputs and deterministic transcript export, add executable pinned-Hail and WDL regressions, and document the VAT schema migration.

**Architecture:** Validate the complete source VAT row schema, then remove its source key before any projection so `(vid, transcript)` inputs cannot protect projected fields. Keep the transcript table keyed by genomic variant for the semi-join, then explicitly `order_by` genomic locus, alleles, and transcript before selecting the approved flat export columns. Exercise the production Hail helpers in the pinned production image and execute the optional WDL output expression with miniwdl.

**Tech Stack:** Python 3.11, Hail 0.2.134, WDL 1.0, miniwdl 1.14.2, Docker, standard-library `unittest`

## Global Constraints

- Preserve the existing VCF and `<OutputPrefix>.annotations.tsv.bgz` names and schemas.
- Preserve exactly the approved transcript output columns and order.
- Retain every matching VAT row, including duplicate transcript rows and intergenic rows.
- Order transcript output by GRCh38 locus, alleles, and transcript; do not rely on an empty `key_by()`.
- Keep consequence ranking and Ensembl normalization downstream.
- Emit the optional transcript output only when `AnnotateWithVAT=true`.
- Use `hailgenetics/hail:0.2.134-py3.11` for the real Hail integration test.

---

### Task 1: Composite-key and deterministic-order Hail regression

**Files:**
- Create: `tests/test_filter_and_write_mt_hail_integration.py`
- Modify: `scripts/filter_and_write_mt.py`

**Interfaces:**
- Consumes: a VAT Hail Table whose source key is `(vid, transcript)` and a retained-variant key table keyed by `(locus, alleles)`.
- Produces: `_prepare_vat_tables(path)` projections without protected source keys and `_prepare_transcript_annotations(transcript_ht, retained_keys)` with the approved flat schema and explicit genomic/transcript order.

- [ ] **Step 1: Write a real Hail integration test**

Create a five-row VAT fixture in deliberately non-genomic order: two transcript rows for one retained variant, one intergenic row, one later retained variant, and one filtered-out variant. Key the fixture by `vid` and `transcript`, call the production helpers, export BGZF, and assert the literal 17-column header, retained duplicate-variant row multiplicity, intergenic row, excluded variant, and genomic/transcript row order.

- [ ] **Step 2: Run the pinned-image test and verify RED**

Run:

```bash
docker run --rm --platform linux/amd64 \
  -v "$PWD:/workspace" -w /workspace \
  hailgenetics/hail:0.2.134-py3.11 \
  python3 -m unittest tests.test_filter_and_write_mt_hail_integration -v
```

Expected: FAIL in `_prepare_vat_tables` because `transcript` is a protected key field.

- [ ] **Step 3: Normalize the VAT source key**

After validating all required row fields, call `vat_source_ht.key_by()` before parsing and projecting fields. Do not alter the returned variant-level projection schema.

- [ ] **Step 4: Verify the protected-key failure is gone**

Re-run the pinned-image test. Expected: progress past `_prepare_vat_tables` and fail because `_prepare_transcript_annotations` is not yet implemented.

- [ ] **Step 5: Implement deterministic transcript preparation**

Move the semi-join, flat identity annotation, explicit `order_by(locus, alleles, transcript)`, and approved-column `select` into `_prepare_transcript_annotations`. Call it from `main`; do not change variant-level annotation selection or VCF INFO selection.

- [ ] **Step 6: Verify GREEN**

Re-run the pinned-image test. Expected: PASS with all five test requirements asserted from the exported BGZF.

---

### Task 2: Optional WDL output execution smoke

**Files:**
- Create: `tests/wdl/filtermt_optional_output_smoke.wdl`
- Create: `tests/test_filtermt_optional_output_smoke.py`

**Interfaces:**
- Consumes: the `TaskFilterMT.TranscriptAnnotations` output declaration from `workflow/FilterMT.wdl`.
- Produces: an executable miniwdl smoke proving the absent local path resolves to JSON `null` when `AnnotateWithVAT=false`.

- [ ] **Step 1: Add the executable regression**

Create a minimal WDL 1.0 workflow with the same optional-output expression and a command that creates no transcript path file. Add a Python test that first verifies the production declaration matches the fixture, then runs miniwdl and asserts the output is null.

- [ ] **Step 2: Run the smoke test**

Run:

```bash
python3 -m unittest tests.test_filtermt_optional_output_smoke -v
```

Expected: PASS when Docker daemon access is available. If sandbox access is denied, retain the runnable test and record the exact daemon error and command in the final report.

---

### Task 3: Input schema migration documentation

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: `REQUIRED_TRANSCRIPT_VAT_FIELDS` and `_prepare_vat_tables` validation behavior.
- Produces: user-facing source-field requirements, early failure behavior, and regeneration guidance for older precomputed VAT tables.

- [ ] **Step 1: Document required VAT fields and migration**

List `vid`, `dbsnp_rsid`, `gene_id`, `gene_symbol`, `transcript`, `is_canonical_transcript`, `consequence`, `aa_change`, `LoF`, `LoF_filter`, `LoF_flags`, `LoF_info`, `gvs_max_af`, and `gvs_max_subpop`. State that validation occurs before projection and missing fields are listed in an early error; older tables must be regenerated from a VAT export containing these columns.

---

### Task 4: Verification, review, report, and commit

**Files:**
- Create: `.superpowers/sdd/2026-08-10-vat-transcript-annotations/final-fix-report.md`

**Interfaces:**
- Consumes: Tasks 1-3 and the approved design/review package.
- Produces: fresh verification evidence, self-review, independent review when tooling permits, a detailed report, and one coherent commit or small commit set.

- [ ] **Step 1: Run focused and complete verification**

Run the focused unit tests, pinned-Hail Docker test, all unit tests, Python compilation, all five `miniwdl check` targets, the optional-output miniwdl smoke, `git diff --check`, and scoped diffs against `9d94bde` and `b8efb6a`.

- [ ] **Step 2: Review the complete diff**

Check every finding against code and executable evidence; confirm the existing VCF and variant-level names/selections are unchanged, no upstream consequence ranking was added, and no unrelated changes entered the wave.

- [ ] **Step 3: Write the final report**

Record status, files, each finding, red/green evidence, exact Docker/Hail and WDL commands/results, commits, self-review, and residual concerns.

- [ ] **Step 4: Commit**

Stage only the final-fix files and commit with an intentional message. Re-run final verification on the committed tree before reporting status.
