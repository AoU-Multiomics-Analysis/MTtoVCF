# VAT Transcript Annotation Final Fix Report

## Status

COMPLETE_WITH_RESIDUAL_LIMITATION

The reviewed Hail defects are fixed in commit `adb28b0` (`fix: harden VAT transcript annotation export`). Real Hail and miniwdl regressions were added and executed successfully before the final status guard. Per the final instruction, no further Docker or integration command was run after that guard; only fast local checks were repeated before commit.

## Files changed

- `scripts/filter_and_write_mt.py`
- `tests/test_filter_and_write_mt_hail_integration.py`
- `tests/test_filtermt_optional_output_smoke.py`
- `tests/wdl/filtermt_optional_output_smoke.wdl`
- `README.md`
- `docs/superpowers/plans/2026-08-10-vat-transcript-annotations-final-fix.md`
- `.superpowers/sdd/2026-08-10-vat-transcript-annotations/final-fix-report.md` (this report)

## Finding resolutions

### 1. Composite-key VAT protected field

Resolved. `_prepare_vat_tables` validates the source row schema first, then calls `vat_source_ht.key_by()` before parsing or projecting fields. This preserves `vid` and `transcript` as ordinary row fields while removing their protected source-key status. The existing variant-level projection is still returned with key `(locus, alleles)`, and its downstream selection and VCF INFO contract were not changed.

The real Hail fixture is written with source key `(vid, transcript)`. Before the fix, Hail failed at the transcript projection with:

```text
hail.expr.expressions.base_expression.ExpressionException:
'Table.select': cannot overwrite key field 'transcript' with annotate, select or drop; use key_by to modify keys.
```

After source-key normalization, the test progressed through `_prepare_vat_tables` and failed only because the new transcript-preparation helper had not yet been added, establishing the isolated red/green transition.

### 2. Deterministic genomic/transcript ordering

Resolved. `_prepare_transcript_annotations` performs the existing Hail semi-join, annotates flat variant identity fields, then explicitly calls:

```python
order_by(locus, alleles, transcript)
```

Only afterward does it select the approved flat output fields. Hail `order_by` unkeys the table while retaining an explicit sorted execution plan, so export contains exactly the approved 17 columns rather than key-only helper columns.

The pinned regression asserts both exported row order and the executed Hail IR sort fields. During mutation testing, replacing the explicit sort with the reviewed empty `key_by()` caused the test to fail because the outer sort remained descending rather than the required ascending sort:

```text
[('locus', 'A'), ('alleles', 'A'), ('transcript', 'D')]
!=
[('locus', 'A'), ('alleles', 'A'), ('transcript', 'A')]
```

Restoring `order_by` returned the pinned test to green.

### 3. Real pinned-Hail regression coverage

Resolved. `tests/test_filter_and_write_mt_hail_integration.py` runs production helpers under `hailgenetics/hail:0.2.134-py3.11` and covers:

- a VAT Hail Table keyed by `(vid, transcript)`;
- two transcript rows for one retained variant, both preserved;
- an intergenic row with missing transcript/gene values, retained;
- one filtered-out variant, absent from output;
- the exact approved columns in exact order;
- explicit ascending `(locus, alleles, transcript)` Hail ordering;
- genomic order in the exported BGZF file.

The integration test skips under the host unit-test interpreter when real Hail is unavailable and runs when invoked in the pinned image.

Controller rerun command (requires Docker daemon approval):

```bash
docker run --rm --platform linux/amd64 \
  -v "$PWD:/workspace" -w /workspace \
  hailgenetics/hail:0.2.134-py3.11 \
  python3 -m unittest tests.test_filter_and_write_mt_hail_integration -v
```

The first unapproved sandbox attempt produced this exact daemon denial:

```text
docker: permission denied while trying to connect to the Docker daemon socket at unix:///Users/evinmpadhi/.docker/run/docker.sock: Post "http://%2FUsers%2Fevinmpadhi%2F.docker%2Frun%2Fdocker.sock/v1.24/containers/create?platform=linux%2Famd64": dial unix /Users/evinmpadhi/.docker/run/docker.sock: connect: operation not permitted.
```

After approval, the pinned image was pulled and the final green run completed:

```text
Ran 1 test in 33.288s
OK
```

Hail emitted its upstream Java illegal-reflective-access warnings, but the process exited 0.

### 4. `AnnotateWithVAT=false` optional output

Resolved to the smallest meaningful executable regression. The smoke fixture uses the production WDL 1.0 optional-output expression, and the Python test first verifies that the fixture declaration matches `workflow/FilterMT.wdl`. It then executes the fixture with miniwdl and Docker while creating no transcript path file, and asserts that `FilterMTOptionalOutputSmoke.TranscriptAnnotations` is JSON `null`.

Command and result:

```bash
python3 -m unittest tests.test_filtermt_optional_output_smoke -v
```

```text
Ran 1 test in 2.118s
OK
```

This tests the intended available WDL engine's optional-file behavior without fabricating a full production MatrixTable/cloud run.

### 5. README input-schema migration

Resolved. README now lists the required transcript source VAT fields (`vid`, `dbsnp_rsid`, `gene_id`, `gene_symbol`, `transcript`, `is_canonical_transcript`, `consequence`, `aa_change`, `LoF`, `LoF_filter`, `LoF_flags`, `LoF_info`, `gvs_max_af`, and `gvs_max_subpop`), documents validation before projection and the complete missing-field error, confirms composite source keys are accepted, and directs users to regenerate older precomputed VAT tables that lack these columns. It also documents the pinned Hail and miniwdl smoke commands.

## Red/green evidence

1. Pinned Hail RED: protected `transcript` key field exception.
2. Key-normalization intermediate RED: `_prepare_vat_tables` succeeded, then `_prepare_transcript_annotations` was absent.
3. Ordering mutation RED: old empty-key behavior failed the required ascending Hail IR assertion.
4. Pinned Hail GREEN: one integration test passed in 33.288 seconds.
5. miniwdl optional-output GREEN: one executable smoke test passed in 2.118 seconds and returned null.

## Validation

Baseline before edits:

```bash
python3 -m unittest discover -s tests -v
python3 -m py_compile scripts/filter_and_write_mt.py scripts/tsv_gz_to_hail_table.py scripts/ExportVCF.py
miniwdl check main.wdl workflow/FilterMT.wdl workflow/TSVtoHailTable.wdl workflow/MTtoVCF.wdl workflow/VCFPostProcess.wdl
git diff --check
```

Result: 5/5 existing tests passed, compilation exited 0, all five WDL documents checked successfully, and the diff check was clean.

Complete host suite after implementation (before the final status guard):

```bash
python3 -m unittest discover -s tests -v
```

Result: 7 tests discovered; 6 passed and the real Hail test was intentionally skipped outside the pinned image. The included miniwdl/Docker smoke passed.

Final fast local verification after the status guard:

```bash
python3 -m unittest tests.test_filter_and_write_mt_contract -v
python3 -m py_compile scripts/filter_and_write_mt.py scripts/tsv_gz_to_hail_table.py scripts/ExportVCF.py tests/test_filter_and_write_mt_contract.py tests/test_filter_and_write_mt_hail_integration.py tests/test_filtermt_optional_output_smoke.py
miniwdl check main.wdl workflow/FilterMT.wdl workflow/TSVtoHailTable.wdl workflow/MTtoVCF.wdl workflow/VCFPostProcess.wdl tests/wdl/filtermt_optional_output_smoke.wdl
git diff --check
```

Result: 5/5 focused tests passed, all listed Python files compiled, all five required WDL files plus the smoke fixture checked successfully, and `git diff --check` exited 0.

## Commits

- `adb28b0` — `fix: harden VAT transcript annotation export`
- This report is committed immediately after creation; its own immutable hash cannot be embedded in its contents and is included in the final task response.

## Self-review

- Reviewed the complete final-wave diff against `9d94bde` and the full feature diff against `b8efb6a`.
- Confirmed existing `<OutputPrefix>.vcf.bgz` and `<OutputPrefix>.annotations.tsv.bgz` names are unchanged.
- Confirmed existing variant-level annotation field selection and VCF INFO selection are unchanged by the final wave.
- Confirmed transcript filtering remains a Hail semi-join against retained MatrixTable row keys and does not expand the MatrixTable or collect annotations on the driver.
- Confirmed the transcript file exports exactly `chrom`, `pos`, `ref`, `alt`, `rsid`, `gene_id`, `gene_symbol`, `transcript`, `is_canonical_transcript`, `consequence`, `aa_change`, `LoF`, `LoF_filter`, `LoF_flags`, `LoF_info`, `gvs_max_af`, and `gvs_max_subpop` in that order.
- Confirmed intergenic rows remain and two same-variant transcript rows remain separate.
- Confirmed no consequence ranking or Ensembl ID normalization was added upstream.
- Confirmed VAT reading/schema validation and transcript export remain disabled when `AnnotateWithVAT=false`.
- Confirmed only requested final-wave files were staged in the implementation commit.

## Residual concerns

- No full production `TaskFilterMT` run was performed because no production MatrixTable, sample list, VAT cloud table, credentials, or cloud output paths were supplied. The real pinned-Hail helper integration and miniwdl optional-output execution cover the defects without inventing those inputs.
- The controller should rerun the exact pinned-Docker command above with daemon approval on the committed tree, per the final status guard.
- The production-pinned Hail image emits Java illegal-reflective-access warnings under its bundled Spark/JVM stack; these warnings did not affect the successful test exit.
