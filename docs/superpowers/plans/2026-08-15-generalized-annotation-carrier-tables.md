# Generalized Annotation Carrier Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve the existing LoF carrier outputs while adding configurable, separate carrier tables for VEP consequence groups such as splice acceptor and splice donor.

**Architecture:** Define internal annotation-group metadata once per extractor, normalize transcript/VAT rows into group-aware variant-gene maps, scan or aggregate carrier genotypes once, and export one table per group. Keep LoF’s HC/LC compatibility outputs as a special output policy over the shared group model.

**Tech Stack:** Python 3.11 standard library, Hail 0.2.134, WDL 1.0, `unittest`, `miniwdl`, existing `bcftools` container images.

## Global Constraints

- Preserve the existing LoF carrier table schemas and output names.
- Emit separate tables for non-LoF annotation groups.
- Initial non-LoF groups are `splice_acceptor_variant` and `splice_donor_variant` from the VEP `consequence` field.
- Use one VCF scan for all standalone annotation groups.
- Use one shared Hail carrier aggregation for all annotation groups.
- Keep LoF HC-only and HC-or-LC semantics unchanged.
- Empty annotation groups must emit valid gzip TSVs with stable headers.
- Keep new group configuration internal; do not add a user-facing configuration input in this iteration.
- Do not change existing VCF INFO fields or the existing filtered MatrixTable behavior.

---

### Task 1: Generalize the standalone parser and carrier aggregation

**Files:**
- Modify: `scripts/extract_lof_carriers.py`
- Modify: `tests/test_extract_lof_carriers.py`

**Interfaces:**
- Consumes: transcript annotation TSV/BGZ and VCF sample genotypes through the existing `write-sites` and `collect-carriers` CLI commands.
- Produces: the existing LoF files plus `<output_prefix>.splice_acceptor_carriers.tsv.gz` and `<output_prefix>.splice_donor_carriers.tsv.gz`.
- New internal interface: an immutable annotation-group definition containing `name`, `source_column`, `matching_values`, `annotation_header`, `has_header`, `count_header`, and `output_suffix`.
- New internal interface: `ANNOTATION_GROUPS` containing `splice_acceptor` and `splice_donor`; LoF remains represented by the same normalized map but retains its two legacy output policies.

- [ ] **Step 1: Add a failing parser contract test**

Extend `tests/test_extract_lof_carriers.py` with a fixture row set containing:

```text
chr1 100 A T ENSG1 GENE1 stop_gained HC
chr1 200 G C ENSG1 GENE1 splice_donor_variant .
chr1 300 C A ENSG2 GENE2 splice_acceptor_variant .
chr1 400 T G ENSG3 GENE3 missense_variant .
```

Invoke `write-sites` and assert that the generated variant map contains a
group column and rows for `lof`, `splice_acceptor`, and `splice_donor`, while
the missense row is absent. Assert that the regions file contains positions
100, 200, and 300 exactly once.

Run:

```bash
python3 -m unittest tests.test_extract_lof_carriers.ExtractLoFCarriersTests.test_writes_grouped_variant_map -v
```

Expected: FAIL because the current map has no group column and filters only
LoF rows.

- [ ] **Step 2: Add a failing multi-output carrier test**

Add a VCF with carriers at the LoF, splice-donor, and splice-acceptor sites
and invoke `collect-carriers`. Assert that:

- existing HC and HC-or-LC files remain present and unchanged;
- `cohort.splice_donor_carriers.tsv.gz` contains the expected `S2`/`ENSG1`
  row;
- `cohort.splice_acceptor_carriers.tsv.gz` contains the expected
  `S1`/`ENSG2` row;
- each non-LoF table has exactly the header
  `sample_id`, `gene_id`, `gene_symbol`,
  `has_<group>_variant`, `n_<group>_variants`, `variant_ids`, `consequences`;
- a group with no matching VEP rows still produces a header-only gzip TSV.

Run:

```bash
python3 -m unittest tests.test_extract_lof_carriers.ExtractLoFCarriersTests.test_writes_separate_group_carrier_tables -v
```

Expected: FAIL because the current collector writes only the two LoF files.

- [ ] **Step 3: Implement annotation-group metadata and normalization**

In `scripts/extract_lof_carriers.py`, add a frozen standard-library dataclass
for group metadata and define the two initial consequence groups. Replace the
LoF-only transcript-row filter with normalization that:

1. cleans the source column using `_clean_value`;
2. matches case-insensitively for `LoF` values and exactly for VEP consequence
   values after normalization;
3. records the stable group name, gene ID, gene symbols, and matched values;
4. skips rows with missing gene IDs or incomplete coordinates;
5. deduplicates values in sets.

Use one variant-map schema with these columns:

```text
chrom  pos  ref  alt  group  gene_id  gene_symbol  annotation
```

Represent LoF matches with `group=lof` and `annotation=HC` or `LC`. Keep
`parse_transcript_annotations` as a compatibility wrapper if existing tests or
callers use it, but have the CLI use the generalized parser.

- [ ] **Step 4: Run the parser tests and make them pass**

Run:

```bash
python3 -m unittest tests.test_extract_lof_carriers.ExtractLoFCarriersTests.test_writes_grouped_variant_map -v
```

Expected: PASS, with the grouped map and union regions matching the test
fixture.

- [ ] **Step 5: Implement one-scan group aggregation**

Change `read_variant_map` to return `group -> variant -> gene -> metadata`.
Change `collect_lof_carriers` into a generic collector that reads the VCF once
and updates a records dictionary per group. For every non-reference genotype,
update all matching groups for the variant. Keep the current `_is_non_ref_gt`
behavior and deterministic sorting.

Implement a generic writer for non-LoF groups with these exact fields:

```python
[
    "sample_id",
    "gene_id",
    "gene_symbol",
    group.has_header,
    group.count_header,
    "variant_ids",
    group.annotation_header,
]
```

Use `"true"` for the presence field, count distinct variant IDs, and join
sorted annotation values with commas. Keep the current LoF writer and its
`lof_classes` ordering for the HC and HC-or-LC outputs.

- [ ] **Step 6: Run all standalone extraction tests**

Run:

```bash
python3 -m unittest tests/test_extract_lof_carriers.py -v
```

Expected: PASS, including the pre-existing LoF regression test and the new
group-output tests.

- [ ] **Step 7: Refactor only after green and commit**

Remove duplicated map and record logic only if the tests remain green. Confirm
that `write-sites` emits the union regions and that `collect-carriers` emits
all four initial outputs. Then commit:

```bash
git add scripts/extract_lof_carriers.py tests/test_extract_lof_carriers.py
git commit -m "feat: generalize standalone annotation carrier extraction"
```

---

### Task 2: Generalize the Hail-native carrier aggregation

**Files:**
- Modify: `scripts/extract_lof_carriers_hail.py`
- Modify: `tests/test_filter_and_write_mt_contract.py`

**Interfaces:**
- Consumes: the existing MatrixTable, VAT Hail Table, filtering arguments, output bucket, output prefix, and temporary directory.
- Produces: unchanged LoF VCF and LoF tables plus Hail-native splice-acceptor and splice-donor carrier tables.
- New internal interface: a group-aware VAT table with `annotation_group`, `gene_id`, `gene_symbol`, and `annotation_value` fields.
- New internal interface: one shared sample-gene-group carrier checkpoint used to format every output.

- [ ] **Step 1: Add failing Hail source-contract assertions**

Extend `tests/test_filter_and_write_mt_contract.py` to assert that
`extract_lof_carriers_hail.py` contains:

- configured `splice_acceptor` and `splice_donor` groups;
- a generic VAT preparation function rather than only LoF filtering;
- a shared `annotation_group` carrier aggregation;
- output path strings for
  `splice_acceptor_carriers.tsv.bgz` and `splice_donor_carriers.tsv.bgz`;
- outpath files for both new outputs.

Also assert that the existing LoF VCF field names and HC/HC-or-LC output paths
remain present.

Run:

```bash
python3 -m unittest tests.test_filter_and_write_mt_contract.TranscriptVatContractTests -v
```

Expected: FAIL because the Hail script is still LoF-only.

- [ ] **Step 2: Implement generalized VAT normalization**

Replace `_prepare_lof_variant_gene_table` with a group-aware preparation
function that validates the existing required fields plus `consequence`,
filters VAT rows to LoF, splice acceptor, or splice donor matches, parses
`vid` as before, and emits a normalized annotation struct:

```text
annotation_group
gene_id
gene_symbol
annotation_value
lof_class
```

Group VAT rows by `(locus, alleles)` and collect distinct annotation structs.
Keep the current Hail set-membership style for LoF class filtering and retain
the existing contig normalization and coordinate handling.

- [ ] **Step 3: Run the focused contract and compilation checks**

Run:

```bash
python3 -m unittest tests.test_filter_and_write_mt_contract.TranscriptVatContractTests -v
python3 -m py_compile scripts/extract_lof_carriers_hail.py
```

Expected: the source contracts still fail only on the shared
aggregation/output assertions, and Python compilation passes.

- [ ] **Step 4: Implement one shared Hail carrier aggregation**

After filtering and materializing the shared annotation MatrixTable, explode
the normalized annotations and retain non-reference entries. Aggregate once
by `(sample_id, annotation_group, gene_id)` with collected gene symbols,
variant IDs, and annotation values. Preserve separate HC-specific collections
for LoF so HC-only formatting still excludes LC-only variants.

The aggregation must retain `annotation_group` as a key or field until all
outputs are formatted. Do not perform a second MatrixTable scan for each
group.

- [ ] **Step 5: Implement group-specific Hail output formatting**

Keep the existing LoF formatter output exactly:

```text
sample_id, gene_id, gene_symbol, has_lof_variant,
n_lof_variants, variant_ids, lof_classes
```

Add a generic formatter for non-LoF groups that emits:

```text
sample_id, gene_id, gene_symbol,
has_<group>_variant, n_<group>_variants,
variant_ids, consequences
```

Export the two new tables to the output bucket using `.tsv.bgz`, checkpoint
the shared carrier table once, and write:

```text
splice_acceptor_carriers_outpath.txt
splice_donor_carriers_outpath.txt
```

Continue writing the existing LoF VCF and LoF outpath files. If no rows match a
group, export an empty Hail table with the required schema rather than
omitting the output.

- [ ] **Step 6: Run Hail source contracts and compile checks to green**

Run:

```bash
python3 -m unittest tests.test_filter_and_write_mt_contract -v
python3 -m py_compile scripts/extract_lof_carriers_hail.py
```

Expected: PASS.

- [ ] **Step 7: Refactor only after green and commit**

Review that the filtered MatrixTable path, LoF VCF INFO fields, LoF output
schemas, HC semantics, and one-checkpoint aggregation are all preserved. Then
commit:

```bash
git add scripts/extract_lof_carriers_hail.py tests/test_filter_and_write_mt_contract.py
git commit -m "feat: generalize Hail annotation carrier extraction"
```

---

### Task 3: Propagate new outputs through WDL workflows

**Files:**
- Modify: `workflow/LoFCarrierTable.wdl`
- Modify: `workflow/HailLoFCarrierTable.wdl`
- Modify: `main.wdl`
- Modify: `tests/test_filter_and_write_mt_contract.py`

**Interfaces:**
- Consumes: the new standalone and Hail output path files.
- Produces: optional workflow outputs named `SpliceAcceptorCarriers` and `SpliceDonorCarriers` in both utility workflows and `main.wdl`.

- [ ] **Step 1: Add failing WDL contract assertions**

Assert that `workflow/LoFCarrierTable.wdl` invokes the generalized script with
the shared regions/map and outputs:

```text
<output_prefix>.splice_acceptor_carriers.tsv.gz
<output_prefix>.splice_donor_carriers.tsv.gz
```

Assert that `workflow/HailLoFCarrierTable.wdl` exposes the two `.tsv.bgz`
outputs from the Hail task, and that `main.wdl` propagates them as optional
outputs from `HailLoFCarriers`.

Run:

```bash
python3 -m unittest tests.test_filter_and_write_mt_contract -v
```

Expected: FAIL because the WDLs expose only LoF files.

- [ ] **Step 2: Update the standalone WDL command and outputs**

Rename only local temporary filenames as needed from `lof_regions.tsv` and
`lof_variant_gene_map.tsv` to generalized names. Keep the existing `bcftools
view -R` flow and the LoF output names. Add task outputs for the two new
gzip TSVs and workflow-level outputs with stable names.

- [ ] **Step 3: Update the Hail WDL task and workflow outputs**

Add `File` outputs that read the two new outpath files written by the Hail
script. Expose them from `HailLoFCarrierTable` alongside the existing LoF VCF,
index, HC, and HC-or-LC outputs. Do not add a second Hail task or alter the
post-hoc LoF VCF indexing task.

- [ ] **Step 4: Propagate optional outputs through `main.wdl`**

Add:

```wdl
File? SpliceAcceptorCarriers = HailLoFCarriers.SpliceAcceptorCarriers
File? SpliceDonorCarriers = HailLoFCarriers.SpliceDonorCarriers
```

Keep them behind the existing `if (MakeLoFCarriers)` conditional and preserve
the default `MakeLoFCarriers = false` behavior.

- [ ] **Step 5: Validate WDL contracts and syntax**

Run:

```bash
python3 -m unittest tests.test_filter_and_write_mt_contract -v
miniwdl check workflow/LoFCarrierTable.wdl
miniwdl check workflow/HailLoFCarrierTable.wdl
miniwdl check main.wdl
```

Expected: all tests and WDL checks pass.

- [ ] **Step 6: Commit the WDL integration**

```bash
git add workflow/LoFCarrierTable.wdl workflow/HailLoFCarrierTable.wdl main.wdl tests/test_filter_and_write_mt_contract.py
git commit -m "feat: expose generalized carrier table outputs"
```

---

### Task 4: Document the new annotation carrier outputs

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: the final standalone and Hail WDL output names and schemas.
- Produces: user-facing documentation for the initial internal groups and separate output tables.

- [ ] **Step 1: Review documentation against the final outputs**

Use `rg` to locate the existing LoF output documentation and verify the new
filenames will be added in the same sections. Do not add a brittle prose test
unless the repository has an established documentation-contract pattern.

- [ ] **Step 2: Update the main workflow documentation**

Document that `MakeLoFCarriers=true` emits:

```text
<FullPrefix>.splice_acceptor_carriers.tsv.bgz
<FullPrefix>.splice_donor_carriers.tsv.bgz
```

State that the initial groups match VEP `consequence` values
`splice_acceptor_variant` and `splice_donor_variant`, that each group is
written to its own sparse sample-gene table, and that the existing LoF HC and
HC-or-LC tables are unchanged.

- [ ] **Step 3: Update utility workflow documentation**

Add the new outputs to the `LoFCarrierTable.wdl` and
`HailLoFCarrierTable.wdl` sections. Explain that the extractor scans or
aggregates all configured groups in one pass and that empty groups still
produce header-only outputs.

- [ ] **Step 4: Review documentation and commit**

Run:

```bash
rg -n "splice_acceptor|splice_donor|HC_or_LC|LoFCarriers" README.md
git diff --check
```

Then commit:

```bash
git add README.md
git commit -m "docs: describe generalized annotation carrier tables"
```

---

### Task 5: Full verification and handoff

**Files:**
- No production files unless verification finds a defect.
- Review: all files changed by Tasks 1–4.

**Interfaces:**
- Consumes: all implementation and test changes from Tasks 1–4.
- Produces: verified generalized carrier-table support with unchanged LoF compatibility behavior.

- [ ] **Step 1: Run the full Python test suite**

```bash
python3 -m unittest discover -s tests -v
```

Expected: all runnable tests pass; the existing pinned-Hail integration test may
remain skipped when its image-specific condition is unavailable.

- [ ] **Step 2: Run final static and workflow validation**

```bash
python3 -m py_compile scripts/extract_lof_carriers.py scripts/extract_lof_carriers_hail.py
miniwdl check workflow/LoFCarrierTable.wdl
miniwdl check workflow/HailLoFCarrierTable.wdl
miniwdl check main.wdl
git diff --check
```

Expected: all commands succeed.

- [ ] **Step 3: Review behavior against the design**

Confirm that:

- LoF filenames, headers, sorting, and HC/HC-or-LC semantics are unchanged;
- splice acceptor and splice donor each have separate outputs;
- all standalone groups are handled by one VCF scan;
- all Hail groups derive from one shared carrier aggregation;
- empty groups have valid headers;
- no new user-facing configuration input was added;
- no unrelated files or worktree changes were included.

- [ ] **Step 4: Commit any final test-only or documentation corrections**

If verification found and fixed a defect, run the affected focused test again
and commit the correction with a specific message. Otherwise, leave the
implementation commits intact and report the verification commands and
results.
