# Dynamic Tabix Indexing Design

## Goal

Make the MTtoVCF workflow reliably generate Tabix indexes for both the exported VCF and the exported annotation TSV, size each indexing task from its own input file, copy each index beside its source in `OutputBucket`, and return both cloud paths as workflow outputs.

## Confirmed Failure

`IndexVCF` currently declares the runtime image as `ghcr.io/aou-multiomics-analysis/mttovcf/utils` without a tag. Docker therefore requests `:latest`, but the repository publishes branch tags such as `:main`; `:latest` does not exist. The task fails before `bcftools` runs.

The published `:main` utility image contains bcftools 1.24 and tabix 1.24. A container-level reproduction confirmed that `bcftools index --tbi --force --output <task-local-path> <localized-vcf>` creates a valid `.tbi`. The task-local basename change already present on branch `codex/index-vcf-basename-output` remains appropriate.

## Architecture

The workflow will use two independent indexing tasks:

1. `IndexVCF` indexes the localized `<prefix>.vcf.bgz` with `bcftools index`.
2. `IndexAnnotations` indexes the localized `<prefix>.annotations.tsv.bgz` with `tabix` using chromosome column 1, position column 2, and one skipped header line.

Both tasks will run the explicitly tagged utility image, calculate disk independently from the localized input size, create the `.tbi` in the task working directory, and upload it directly to its final `gs://` destination with Google Cloud CLI. Keeping these tasks outside the Hail filtering task means an indexing failure can be retried without repeating the expensive MatrixTable filtering and export.

## Exported Annotation File

`filter_and_write_mt.py` already exports:

`<OutputBucket>/<FullPrefix>.annotations.tsv.bgz`

It will additionally write that URI to `annotations_outpath.txt`. `TaskFilterMT` and the `FilterMT` workflow will expose it as `File PathAnnotations`, matching the existing `PathVCF` pattern.

The annotation table remains ordered by the MatrixTable row key inherited from `mt_filtered.rows()`. Its first two columns are `chrom` and `pos`, and its first line is an un-commented header. The indexing command will therefore use:

```bash
tabix --force --sequence 1 --begin 2 --end 2 --skip-lines 1 <task-local-annotation-file>
```

Because tabix 1.24 does not support selecting a separate output path, the task will create a task-local symlink with the source basename and index that symlink. This creates the sidecar in the task working directory without duplicating the potentially large annotation file.

## Runtime Image

The utility Docker image will install Google Cloud CLI in addition to bcftools and tabix. Each index task will declare:

```wdl
docker: "ghcr.io/aou-multiomics-analysis/mttovcf/utils:" + UtilsImageTag
```

The workflow input `UtilsImageTag` will default to `"main"`. It remains independent from the existing Hail-image `Branch` input so a utility-image test tag does not require a matching Hail-image tag.

The utility-image GitHub Actions workflow will trigger when either `envs/utils/Dockerfile` or the utility workflow definition changes, in addition to its existing script triggers. This ensures a merged Dockerfile change actually rebuilds `utils:main`.

## Dynamic Disk Sizing

Each index task will receive these workflow-configurable inputs:

- `Float IndexDiskMultiplier = 2.0`
- `Int IndexDiskOverheadGiB = 10`
- `Int IndexMinDiskGiB = 20`

For each input file independently:

```wdl
Int CalculatedDiskGiB = ceil(size(InputFile, "GiB") * IndexDiskMultiplier) + IndexDiskOverheadGiB
Int IndexDiskGiB = if CalculatedDiskGiB > IndexMinDiskGiB then CalculatedDiskGiB else IndexMinDiskGiB
```

The runtime request will be:

```wdl
disks: "local-disk ~{IndexDiskGiB} SSD"
```

This implements `max(20 GiB, ceil(input GiB × 2) + 10 GiB)` by default. The multiplier accounts for input localization and working-space uncertainty; the fixed overhead covers the container, task metadata, and index output.

## Cloud Placement and Outputs

The workflow will remove trailing slashes with WDL's `sub(OutputBucket, "/+$", "")` before computing destination names from `OutputBucket` and `FullPrefix`:

- `<OutputBucket>/<FullPrefix>.vcf.bgz.tbi`
- `<OutputBucket>/<FullPrefix>.annotations.tsv.bgz.tbi`

Each task will upload its completed local index with `gcloud storage cp`. Upload occurs only after the indexing command succeeds. The task will then write the destination URI to a small path file and expose it as a WDL `File`, following the repository's existing remote-file output pattern.

The top-level workflow outputs will include:

- `File PathVCF`
- `File PathAnnotations`
- `File VCFIndex`
- `File AnnotationIndex`

Existing optional dosage and PLINK outputs remain unchanged.

## Validation and Error Handling

All index commands and uploads run with `set -euo pipefail`. A failed index or failed cloud upload fails only the corresponding index task.

Validation will include:

1. A static WDL contract test proving the image tag, dynamic disk formula, annotation tabix columns, and new workflow outputs are present.
2. A tiny bgzipped VCF runtime fixture that is indexed and queried successfully with bcftools/tabix.
3. A tiny bgzipped annotation TSV runtime fixture with an un-commented header that is indexed and queried successfully by genomic region.
4. A check that the index filenames exactly match the VCF and annotation cloud filenames plus `.tbi`.
5. `miniwdl check main.wdl` and `git diff --check` over the completed change.
6. A utility-image build proving bcftools, tabix, and `gcloud storage` are available through the image's normal entrypoint.

Cloud upload authentication will use the workload identity already provided to Cromwell tasks. Local tests will validate command construction and local indexing; they will not require production GCS credentials.

## Non-Goals

- Hail will not generate the Tabix indexes; Hail only produces the BGZF source files.
- The expensive Hail filter/export task will not be rerun merely to retry indexing.
- Dosage and PLINK disk sizing are not changed in this work.
- CSI indexes and non-GCS destination schemes are not added.
