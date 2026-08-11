# VAT Transcript Annotation Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a non-breaking, transcript-level VAT annotation TSV for every variant retained by MTtoVCF and expose it as an optional WDL output.

**Architecture:** Read and parse the VAT Hail Table once, then derive the existing variant-level projection and a new transcript-level projection. Semi-join transcript rows to the filtered MatrixTable's variant keys and export them without expanding genotype rows; propagate the resulting cloud path through both WDL workflow layers.

**Tech Stack:** Python 3, Hail, WDL 1.0, miniwdl, standard-library `unittest`

## Global Constraints

- Preserve the existing VCF and `<OutputPrefix>.annotations.tsv.bgz` names and schemas.
- Emit `<OutputPrefix>.transcript_annotations.tsv.bgz` only when `AnnotateWithVAT=true`.
- Include `chrom`, `pos`, `ref`, `alt`, `rsid`, `gene_id`, `gene_symbol`, `transcript`, `is_canonical_transcript`, `consequence`, `aa_change`, `LoF`, `LoF_filter`, `LoF_flags`, `LoF_info`, `gvs_max_af`, and `gvs_max_subpop`.
- Retain intergenic rows with missing transcript or gene fields.
- Filter transcript annotations with a Hail semi-join; do not collect annotations in driver memory or expand the genotype MatrixTable.
- Fail early with an explicit list of missing required VAT fields when VAT annotation is enabled.
- Keep transcript consequence ranking and Ensembl version normalization downstream.
- Add no runtime dependency beyond the repository's existing Hail image.

---

### Task 1: Preserve and export filtered VAT transcript rows

**Files:**
- Create: `tests/test_filter_and_write_mt_contract.py`
- Modify: `scripts/filter_and_write_mt.py:24-197`
- Modify: `scripts/filter_and_write_mt.py:222-357`

**Interfaces:**
- Consumes: an AoU VAT Hail Table containing `REQUIRED_TRANSCRIPT_VAT_FIELDS` and a filtered MatrixTable keyed by `locus`, `alleles`.
- Produces: `_prepare_vat_tables(vat_hail_table) -> tuple[hl.Table, hl.Table]`, `_missing_transcript_vat_fields(available_fields) -> list[str]`, `<OutputPrefix>.transcript_annotations.tsv.bgz`, and `transcript_annotations_outpath.txt`.

- [ ] **Step 1: Add failing field-contract tests**

Create `tests/test_filter_and_write_mt_contract.py`:

```python
import importlib.util
from pathlib import Path
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.modules.setdefault("hail", types.ModuleType("hail"))
SPEC = importlib.util.spec_from_file_location(
    "filter_and_write_mt", ROOT / "scripts" / "filter_and_write_mt.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class TranscriptVatContractTests(unittest.TestCase):
    def test_transcript_output_fields_are_complete(self):
        self.assertEqual(
            MODULE.TRANSCRIPT_ANNOTATION_FIELDS,
            (
                "rsid", "gene_id", "gene_symbol", "transcript",
                "is_canonical_transcript", "consequence", "aa_change",
                "LoF", "LoF_filter", "LoF_flags", "LoF_info",
                "gvs_max_af", "gvs_max_subpop",
            ),
        )

    def test_missing_required_fields_are_sorted(self):
        available = set(MODULE.REQUIRED_TRANSCRIPT_VAT_FIELDS) - {
            "gene_id", "transcript"
        }
        self.assertEqual(
            MODULE._missing_transcript_vat_fields(available),
            ["gene_id", "transcript"],
        )

    def test_vid_and_dbsnp_source_fields_are_required(self):
        self.assertIn("vid", MODULE.REQUIRED_TRANSCRIPT_VAT_FIELDS)
        self.assertIn("dbsnp_rsid", MODULE.REQUIRED_TRANSCRIPT_VAT_FIELDS)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests and verify the contract is absent**

Run:

```bash
python3 -m unittest tests/test_filter_and_write_mt_contract.py -v
```

Expected: FAIL because the transcript constants do not exist.

- [ ] **Step 3: Add schema constants and validation**

Add to `scripts/filter_and_write_mt.py`:

```python
TRANSCRIPT_ANNOTATION_FIELDS = (
    "rsid", "gene_id", "gene_symbol", "transcript",
    "is_canonical_transcript", "consequence", "aa_change",
    "LoF", "LoF_filter", "LoF_flags", "LoF_info",
    "gvs_max_af", "gvs_max_subpop",
)

REQUIRED_TRANSCRIPT_VAT_FIELDS = (
    "vid", "dbsnp_rsid", "gene_id", "gene_symbol", "transcript",
    "is_canonical_transcript", "consequence", "aa_change",
    "LoF", "LoF_filter", "LoF_flags", "LoF_info",
    "gvs_max_af", "gvs_max_subpop",
)


def _missing_transcript_vat_fields(available_fields):
    return sorted(set(REQUIRED_TRANSCRIPT_VAT_FIELDS) - set(available_fields))
```

After reading the VAT, compute `available_fields = set(vat_ht.row.dtype)` so keyed
fields such as `vid` are included. If fields are missing, raise:

```python
raise ValueError(
    "VAT Hail Table is missing required transcript annotation fields: "
    + ", ".join(missing_fields)
)
```

- [ ] **Step 4: Split VAT preparation into two projections**

Rename `_prepare_vat_ht` to `_prepare_vat_tables`. Preserve the current variant projection exactly. From the parsed source, build:

```python
transcript_ht = vat_source_ht.select(
    "locus",
    "alleles",
    rsid=_cast_to_str(vat_source_ht.dbsnp_rsid),
    gene_id=_cast_to_str(vat_source_ht.gene_id),
    gene_symbol=_cast_to_str(vat_source_ht.gene_symbol),
    transcript=_cast_to_str(vat_source_ht.transcript),
    is_canonical_transcript=_cast_to_str(vat_source_ht.is_canonical_transcript),
    consequence=_cast_to_str(vat_source_ht.consequence),
    aa_change=_cast_to_str(vat_source_ht.aa_change),
    LoF=_cast_to_str(vat_source_ht.LoF),
    LoF_filter=sanitize_info(vat_source_ht.LoF_filter),
    LoF_flags=sanitize_info(vat_source_ht.LoF_flags),
    LoF_info=sanitize_info(vat_source_ht.LoF_info),
    gvs_max_af=_cast_to_float(vat_source_ht.gvs_max_af),
    gvs_max_subpop=_cast_to_str(vat_source_ht.gvs_max_subpop),
).key_by("locus", "alleles")
```

Return `(variant_ht.key_by("locus", "alleles"), transcript_ht)`. When VAT is disabled, set both variables to `None` without reading or validating VAT.

- [ ] **Step 5: Semi-join and export transcript annotations**

Inside `if annotate_with_vat:` after the current filters, add:

```python
filtered_variant_keys = mt_filtered.rows().select()
transcript_annotations_ht = transcript_vat_ht.semi_join(filtered_variant_keys)
transcript_annotations_ht = transcript_annotations_ht.annotate(
    chrom=transcript_annotations_ht.locus.contig,
    pos=transcript_annotations_ht.locus.position,
    ref=transcript_annotations_ht.alleles[0],
    alt=transcript_annotations_ht.alleles[1],
)
transcript_annotations_ht = transcript_annotations_ht.key_by().select(
    "chrom", "pos", "ref", "alt", *TRANSCRIPT_ANNOTATION_FIELDS
)
transcript_annotations_tsv = _join_cloud_path(
    args.OutputBucket,
    f"{args.OutputPrefix}.transcript_annotations.tsv.bgz",
)
transcript_annotations_ht.export(transcript_annotations_tsv)
with open("transcript_annotations_outpath.txt", "w") as output_path_file:
    output_path_file.write(transcript_annotations_tsv)
```

Do not alter the existing VCF or variant-level annotation selections.

- [ ] **Step 6: Run unit and syntax tests**

Run:

```bash
python3 -m unittest tests/test_filter_and_write_mt_contract.py -v
python3 -m py_compile scripts/filter_and_write_mt.py
```

Expected: three unit tests pass and Python compilation exits 0.

- [ ] **Step 7: Commit the Hail export change**

```bash
git add scripts/filter_and_write_mt.py tests/test_filter_and_write_mt_contract.py
git commit -m "feat: export transcript-level VAT annotations"
```

---

### Task 2: Propagate the optional WDL output

**Files:**
- Modify: `tests/test_filter_and_write_mt_contract.py`
- Modify: `workflow/FilterMT.wdl:47-49`
- Modify: `workflow/FilterMT.wdl:103-105`
- Modify: `main.wdl:111-119`

**Interfaces:**
- Consumes: `transcript_annotations_outpath.txt`, written only when `AnnotateWithVAT=true`.
- Produces: `File? TranscriptAnnotations` from `TaskFilterMT`, `FilterMT`, and `FilterMTAndExportToVCF`.

- [ ] **Step 1: Add failing WDL propagation tests**

Append to `TranscriptVatContractTests`:

```python
    def test_filter_workflow_exposes_optional_transcript_output(self):
        source = (ROOT / "workflow" / "FilterMT.wdl").read_text()
        self.assertIn(
            "File? TranscriptAnnotations = TaskFilterMT.TranscriptAnnotations",
            source,
        )
        self.assertIn(
            "File? TranscriptAnnotations = if AnnotateWithVAT then "
            "read_string('transcript_annotations_outpath.txt') else None",
            source,
        )

    def test_main_workflow_propagates_transcript_output(self):
        source = (ROOT / "main.wdl").read_text()
        self.assertIn(
            "File? TranscriptAnnotations = filter.TranscriptAnnotations",
            source,
        )
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run `python3 -m unittest tests/test_filter_and_write_mt_contract.py -v`.

Expected: the two new tests fail because the output is not exposed.

- [ ] **Step 3: Add the output to `workflow/FilterMT.wdl`**

Add to workflow outputs:

```wdl
File? TranscriptAnnotations = TaskFilterMT.TranscriptAnnotations
```

Add to task outputs:

```wdl
File? TranscriptAnnotations = if AnnotateWithVAT then read_string('transcript_annotations_outpath.txt') else None
```

- [ ] **Step 4: Propagate through `main.wdl`**

Add to `FilterMTAndExportToVCF.output`:

```wdl
File? TranscriptAnnotations = filter.TranscriptAnnotations
```

- [ ] **Step 5: Validate tests and WDL syntax**

Run:

```bash
python3 -m unittest tests/test_filter_and_write_mt_contract.py -v
miniwdl check main.wdl workflow/FilterMT.wdl
```

Expected: five tests pass and both WDL documents validate. If miniwdl rejects `None`, use its WDL 1.0-compatible optional expression and update the exact source assertion.

- [ ] **Step 6: Commit WDL propagation**

```bash
git add workflow/FilterMT.wdl main.wdl tests/test_filter_and_write_mt_contract.py
git commit -m "feat: expose transcript annotations output"
```

---

### Task 3: Document and fully verify the contract

**Files:**
- Modify: `README.md:12-85`

**Interfaces:**
- Consumes: output behavior from Tasks 1 and 2.
- Produces: user-facing documentation for naming, row grain, schema, optional behavior, and downstream consequence collapsing.

- [ ] **Step 1: Update README output documentation**

Document:

```markdown
- `<OutputPrefix>.annotations.tsv.bgz`: existing one-row-per-variant annotations.
- `<OutputPrefix>.transcript_annotations.tsv.bgz`: one row per retained
  variant-transcript combination, emitted when `AnnotateWithVAT=true` and
  exposed as the optional `TranscriptAnnotations` WDL output.
```

List every companion field from Global Constraints. State that intergenic rows may have missing `gene_id`/`transcript`, the MatrixTable is not expanded, and downstream analyses should choose the most severe consequence for the matched gene.

- [ ] **Step 2: Run complete verification**

Run:

```bash
python3 -m unittest discover -s tests -v
python3 -m py_compile scripts/filter_and_write_mt.py scripts/tsv_gz_to_hail_table.py scripts/ExportVCF.py
miniwdl check main.wdl workflow/FilterMT.wdl workflow/TSVtoHailTable.wdl workflow/MTtoVCF.wdl workflow/VCFPostProcess.wdl
git diff --check origin/main...HEAD
```

Expected: all tests pass, Python compilation exits 0, all five WDL files validate, and `git diff --check` emits no errors.

- [ ] **Step 3: Confirm existing outputs are unchanged**

Run:

```bash
git diff origin/main...HEAD -- scripts/filter_and_write_mt.py workflow/FilterMT.wdl main.wdl
```

Verify that `<OutputPrefix>.vcf.bgz` and `<OutputPrefix>.annotations.tsv.bgz` retain their names and existing fields, and the new export is guarded by `annotate_with_vat` / `AnnotateWithVAT`.

- [ ] **Step 4: Commit documentation**

```bash
git add README.md
git commit -m "docs: describe transcript VAT annotations"
```

- [ ] **Step 5: Prepare for review**

Run:

```bash
git status --short
git log --oneline origin/main..HEAD
```

Expected: clean worktree containing the design, plan, implementation, WDL propagation, and documentation commits.
