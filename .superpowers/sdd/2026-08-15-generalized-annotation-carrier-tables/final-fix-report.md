# Final Review Fix Report

Date: 2026-08-15

## Status

The final-review Hail VCF regression is fixed. The pinned Hail 0.2.134
integration test now exports a real bgzipped VCF and verifies the header and
record values for `LOF_GENE_ID`, `LOF_GENE_SYMBOL`, and `LOF_CLASS`.

## Root Cause

`_annotate_lof_vcf_fields()` derived the three `LOF_*` arrays correctly but
stored them as top-level MatrixTable row fields. The production path then
materialized that MatrixTable and passed it to `hl.export_vcf()`. Hail exports
VCF INFO annotations from the row-level `info` struct; it does not serialize
arbitrary top-level row fields as INFO values. The prior synthetic integration
assertions collected the top-level Hail rows directly and therefore stopped
before the failing MatrixTable-to-VCF serialization boundary.

The minimal fix constructs `info` with the three sorted arrays. No carrier
aggregation, filtering, WDL interface, output name, output schema, or HC/LC
logic changed.

## Changed Files

- `scripts/extract_lof_carriers_hail.py`
  - Moved `LOF_GENE_ID`, `LOF_GENE_SYMBOL`, and `LOF_CLASS` into the row
    `info` struct consumed by Hail VCF export.
- `tests/test_filter_and_write_mt_hail_integration.py`
  - Kept the existing synthetic carrier/HC-only assertions.
  - Made the synthetic rows VCF-exportable with `locus`/`alleles` keys.
  - Derived `lof_annotations` through the production-shaped filtering path.
  - Exported and read an actual bgzipped VCF, checking all three INFO headers
    and HC/LC record values.
- `README.md`
  - Added `consequence` to the Hail VAT required fields because splice-group
    matching reads it.
- `.superpowers/sdd/2026-08-15-generalized-annotation-carrier-tables/final-fix-report.md`
  - Added this report.

## Commits

- `6376ba972e84f51dd29aea2dc91150620c1b22f3` -
  `fix: export Hail LoF annotations in VCF info`
- The report is committed separately after this document is written; its hash
  is returned in the final task response.

## TDD Evidence

### Initial sandbox check

Command:

```bash
docker run --rm --platform linux/amd64 \
  -v /Users/evinmpadhi/Documents/MTtoVCF:/workspace -w /workspace \
  hailgenetics/hail:0.2.134-py3.11 \
  python3 -m unittest \
  tests.test_filter_and_write_mt_hail_integration.TranscriptVatHailIntegrationTests.test_hail_lof_outputs_preserve_hc_only_semantics -v
```

Result: exit 126 before test execution because the sandbox denied access to
the Docker daemon socket. The command was rerun with approved Docker access.

### RED fixture correction

The first approved pinned-image run exited 1 before reaching the regression:
Hail 0.2.134 could not infer the type of the existing `.default([])` synthetic
fixture. The fixture was changed to derive `lof_annotations` by filtering the
fully typed `annotations` array, matching production data flow. No production
code had been changed.

### RED root-cause failure

Command:

```bash
docker run --rm --platform linux/amd64 \
  -v /Users/evinmpadhi/Documents/MTtoVCF:/workspace -w /workspace \
  hailgenetics/hail:0.2.134-py3.11 \
  python3 -m unittest \
  tests.test_filter_and_write_mt_hail_integration.TranscriptVatHailIntegrationTests.test_hail_lof_outputs_preserve_hc_only_semantics -v
```

Result: exit 1 with the intended failure:

```text
LookupError: Table instance has no field 'info'
Ran 1 test in 18.401s
FAILED (errors=1)
```

This demonstrated that the regression reached the missing VCF INFO contract
before the production fix.

### GREEN focused pinned-Hail regression

The same command after the production change returned:

```text
Ran 1 test in 41.851s
OK
```

The passing test read the actual exported VCF and verified all three INFO
headers plus `ENSG0001`, `GENE1`, `HC`, and `LC` record values.

## Full Verification Commands And Results

Command:

```bash
docker run --rm --platform linux/amd64 \
  -v /Users/evinmpadhi/Documents/MTtoVCF:/workspace -w /workspace \
  hailgenetics/hail:0.2.134-py3.11 \
  python3 -m unittest tests.test_filter_and_write_mt_hail_integration -v
```

Result: exit 0; `Ran 2 tests in 58.562s`, `OK`.

Command:

```bash
python3 -m unittest discover -s tests -v
```

Initial sandbox result: exit 1. All code/contract tests passed, the two local
Hail tests skipped because Hail is not installed in the host Python, and the
miniwdl smoke errored because its Docker launch could not access the sandboxed
daemon socket.

The smoke was isolated and rerun with approved Docker access:

```bash
python3 -m unittest tests.test_filtermt_optional_output_smoke -v
```

Result: exit 0; `Ran 1 test in 1.656s`, `OK`.

The complete suite was then rerun with the same approved Docker access:

```bash
python3 -m unittest discover -s tests -v
```

Result: exit 0; `Ran 24 tests in 2.822s`, `OK (skipped=2)`.
The two host-Python skips are the Hail integration tests, both of which passed
in the pinned Hail image above.

Command:

```bash
python3 -m py_compile scripts/extract_lof_carriers_hail.py \
  tests/test_filter_and_write_mt_hail_integration.py
```

Result: exit 0 with no output.

Commands:

```bash
miniwdl check workflow/HailLoFCarrierTable.wdl
miniwdl check main.wdl
```

Result: both exited 0 and parsed/typechecked the workflows and imports.

Command:

```bash
git diff --check
```

Result: exit 0 with no output.

Command:

```bash
git diff --cached --check
```

Result before the implementation commit: exit 0 with no output.

## Skips And Environment Limits

- Host Python is 3.14.6 and does not have Hail installed, so the two Hail
  integration tests skip during the host suite. Both were run and passed in
  the production-pinned `hailgenetics/hail:0.2.134-py3.11` image.
- Docker daemon access is blocked in the default sandbox. Approved access was
  used for the pinned-Hail tests and miniwdl execution smoke.
- Pinned Hail emitted Spark illegal-reflective-access warnings during export;
  they did not affect test results.
- This session did not expose a reviewer-subagent interface. The final scoped
  diff review was performed read-only in the main session.

## Deferred Notes And Concerns

- The explicitly deferred compatibility parser deduplication and standalone
  header assertion hardening notes were left unchanged.
- No outstanding functional concerns were found in this fix wave.
