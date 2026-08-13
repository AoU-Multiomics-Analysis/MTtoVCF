# Hail LoF VCF and Single-Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Hail-native LoF workflow materialize the filtered LoF data once, emit HC+LC and HC-only carrier tables from shared data, and export/index a corresponding LoF-only VCF.

**Architecture:** The Hail script will apply the existing sample, BED, biallelic, PASS, AC, and AN filters, join VAT LoF annotations, and write one intermediate LoF MatrixTable under `CloudTmpdir`. It will export the unindexed LoF VCF from that materialized MatrixTable and aggregate carrier data once into a checkpointed intermediate carrier table; the HC+LC and HC-only files will be derived from that shared table. A separate WDL task will run `bcftools index --tbi --force` after Hail export, and the standalone and main workflows will expose both VCF outputs and the index.

**Tech Stack:** Python 3.11, Hail 0.2.134, WDL 1.0, `bcftools` from `ghcr.io/aou-multiomics-analysis/mttovcf/utils:main`, standard-library `unittest`, miniwdl.

## Global Constraints

- Preserve the existing LoF carrier table schemas and output names.
- Preserve HC-only semantics: HC output counts and lists only HC variants, even when the same sample/gene also has LC variants.
- Keep the existing MatrixTable filters and VAT fields required by the Hail-native workflow.
- Do not attempt to create the VCF index in Hail; indexing must run in a separate post hoc task.
- Keep LoF carrier extraction independent of parsing the exported VCF.
- Preserve `MakeLoFCarriers=false` as the main workflow default and keep all new outputs optional there.

---

### Task 1: Add regression contracts for shared materialization and outputs

**Files:**
- Modify: `tests/test_filter_and_write_mt_contract.py`
- Test: `workflow/HailLoFCarrierTable.wdl` and `main.wdl` source contracts

**Interfaces:**
- Consumes: the current Hail LoF script and WDL source.
- Produces: assertions for one materialized LoF MatrixTable, shared carrier aggregation, unindexed VCF export, post hoc indexing, and main-workflow propagation of VCF/index outputs.

- [ ] **Step 1: Write failing source-contract assertions**

Assert that the script contains one intermediate MatrixTable write/read, one shared carrier checkpoint, one HC+LC export, one HC-derived output path, and an `hl.export_vcf` call. Assert that the Hail workflow contains an `IndexLoFVCF` call using `bcftools index`, exposes `LoFVariantsVCF` and `LoFVariantsVCFIndex`, and that `main.wdl` calls the imported Hail workflow and propagates those outputs.

- [ ] **Step 2: Run the focused test to verify RED**

Run:

```bash
python3 -m unittest tests.test_filter_and_write_mt_contract -v
```

Expected: the new assertions fail because the current implementation performs separate carrier exports and has no LoF VCF/index outputs.

---

### Task 2: Refactor the Hail script around one materialized LoF dataset

**Files:**
- Modify: `scripts/extract_lof_carriers_hail.py`
- Test: `tests/test_filter_and_write_mt_contract.py`

**Interfaces:**
- Consumes: `--MatrixTable`, `--VATHailTable`, sample/BED/QC filters, `--OutputBucket`, `--OutputPrefix`, and `--CloudTmpdir`.
- Produces: `<OutputPrefix>.lof_variants.vcf.bgz`, `<OutputPrefix>.lof_carriers.HC.tsv.bgz`, `<OutputPrefix>.lof_carriers.HC_or_LC.tsv.bgz`, and local outpath files for those outputs.

- [ ] **Step 1: Add the shared intermediate path helpers and VCF annotation expressions**

Add deterministic paths beneath `CloudTmpdir` for the intermediate LoF MatrixTable and carrier table. Add flat row annotations for VCF export: `LOF_GENE_ID`, `LOF_GENE_SYMBOL`, and `LOF_CLASS`, each represented as sorted arrays so the exported VCF retains the VAT gene/class relationships without nested Hail structs.

- [ ] **Step 2: Materialize the filtered LoF MatrixTable once**

After `_filter_matrix_table` and the VAT row join, retain only `GT` entries and the LoF row annotations, add the flat VCF annotations, and write/read the intermediate MatrixTable with overwrite enabled. Export the unindexed compressed VCF from this materialized MatrixTable using `hl.export_vcf`; do not pass a Hail tabix-index option.

- [ ] **Step 3: Aggregate carrier data once and preserve HC-only semantics**

Explode LoF genes and filter to non-reference genotypes from the materialized MatrixTable. Aggregate one checkpointed carrier table per sample/gene containing all-class fields and HC-specific fields. Export HC+LC from the all-class fields and HC-only from the HC-specific fields filtered to rows with at least one HC variant. This makes both outputs depend on the same carrier aggregation and avoids a second scan of the source MatrixTable.

- [ ] **Step 4: Write all output path files**

Write `lof_variants_vcf_outpath.txt`, `lof_carriers_hc_outpath.txt`, and `lof_carriers_hc_or_lc_outpath.txt` after the corresponding exports. Keep existing table column names and ordering unchanged.

- [ ] **Step 5: Run focused tests and compile checks**

Run:

```bash
python3 -m unittest tests.test_filter_and_write_mt_contract -v
python3 -m py_compile scripts/extract_lof_carriers_hail.py
```

Expected: focused contracts and compilation pass.

---

### Task 3: Add post hoc VCF indexing to the standalone Hail workflow

**Files:**
- Modify: `workflow/HailLoFCarrierTable.wdl`
- Test: `tests/test_filter_and_write_mt_contract.py`

**Interfaces:**
- Consumes: the VCF path emitted by `ExtractHailLoFCarriers`.
- Produces: `LoFVariantsVCF` and `LoFVariantsVCFIndex` workflow outputs, with the index created by `bcftools` after Hail completes.

- [ ] **Step 1: Add the failing WDL contract**

Assert that the workflow calls a post hoc indexing task, that the task uses `ghcr.io/aou-multiomics-analysis/mttovcf/utils:main`, runs `bcftools index --tbi --force`, and outputs `<prefix>.vcf.bgz.tbi`.

- [ ] **Step 2: Implement `IndexLoFVCF`**

Add an indexing task with modest configurable resources and the same output-prefix convention as the existing `main.wdl` `IndexVCF` task. Call it after `ExtractHailLoFCarriers`; expose the VCF, VCF index, HC table, and HC+LC table from the workflow.

- [ ] **Step 3: Validate the standalone WDL**

Run:

```bash
miniwdl check workflow/HailLoFCarrierTable.wdl
```

Expected: no WDL errors.

---

### Task 4: Integrate the full Hail subworkflow into `main.wdl`

**Files:**
- Modify: `main.wdl`
- Modify: `README.md`
- Test: `tests/test_filter_and_write_mt_contract.py`

**Interfaces:**
- Consumes: existing `MakeLoFCarriers` inputs and runtime defaults.
- Produces: optional main-workflow outputs for HC, HC+LC, LoF VCF, and LoF VCF index.

- [ ] **Step 1: Switch the optional call from the task to the imported workflow**

Call `HailLoFCarrierTable.HailLoFCarrierTable` so the main workflow includes the standalone workflow’s post hoc indexing step. Preserve existing input names/defaults and keep `make_lof_carriers = false` in `VCFPostProcess`.

- [ ] **Step 2: Propagate VCF outputs**

Add optional `LoFVariantsVCF` and `LoFVariantsVCFIndex` outputs alongside the existing carrier outputs.

- [ ] **Step 3: Update documentation**

Document the LoF VCF and `.tbi` outputs, explain that indexing occurs in a separate `bcftools` task, and state that carrier tables are derived from shared Hail intermediates rather than parsing the VCF.

- [ ] **Step 4: Validate WDL and contracts**

Run:

```bash
miniwdl check main.wdl
python3 -m unittest tests.test_filter_and_write_mt_contract -v
```

Expected: both pass.

---

### Task 5: Full verification and commit

**Files:**
- Modify: `docs/superpowers/plans/2026-08-12-hail-lof-vcf-and-single-sweep.md`

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces: a verified implementation ready for review or publication.

- [ ] **Step 1: Run the full test suite**

Run:

```bash
python3 -m unittest discover -s tests -v
```

Expected: all runnable tests pass; only the existing pinned-Hail integration test is skipped when its image-specific condition is unavailable.

- [ ] **Step 2: Run final validation**

Run `git diff --check`, `python3 -m py_compile scripts/extract_lof_carriers_hail.py`, `miniwdl check workflow/HailLoFCarrierTable.wdl`, and `miniwdl check main.wdl`.

- [ ] **Step 3: Review the diff**

Confirm that the original input filters, table schemas, output names, and optional behavior remain intact; confirm the VCF index is created only by `bcftools`.

- [ ] **Step 4: Commit the implementation**

Stage only the plan, script, WDL, README, and test changes and commit them with:

```bash
git commit -m "Optimize Hail LoF outputs and export VCF"
```
