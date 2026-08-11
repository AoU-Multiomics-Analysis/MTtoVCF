# Dynamic Tabix Indexing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reliably create, publish, and return Tabix indexes for the exported VCF and annotation TSV while sizing each indexing task from its own localized input.

**Architecture:** `FilterMT` will expose both cloud-exported source files. Separate `IndexVCF` and `IndexAnnotations` tasks will localize one source each, request dynamic disk, create a task-local `.tbi`, and upload it beside the source with Google Cloud CLI. The utility image will contain bcftools, tabix, and Google Cloud CLI and will always be referenced by an explicit tag.

**Tech Stack:** WDL 1.0, Cromwell/miniwdl, Hail, bcftools 1.24, tabix 1.24, Google Cloud CLI, Docker, Python `unittest`.

## Global Constraints

- Preserve the existing `codex/index-vcf-basename-output` behavior: indexes are created in the task working directory, not beside Cromwell's localized input.
- Default disk sizing is `max(20 GiB, ceil(input GiB × 2.0) + 10 GiB)` and is computed independently for the VCF and annotation TSV.
- Expose `IndexDiskMultiplier`, `IndexDiskOverheadGiB`, and `IndexMinDiskGiB` as workflow inputs with defaults `2.0`, `10`, and `20`.
- Expose `UtilsImageTag` independently from the Hail-image `Branch` input, with default `"main"`.
- Publish `<FullPrefix>.vcf.bgz.tbi` and `<FullPrefix>.annotations.tsv.bgz.tbi` directly under normalized `OutputBucket`.
- Index annotation columns as one-based `chrom=1`, `pos=2`, `end=2`, skipping the single un-commented header line.
- Do not move indexing into the Hail filter task and do not change dosage or PLINK runtime sizing.
- Use test-first changes: run each named test and observe its expected failure before editing production files.

---

### Task 1: Expose the annotation TSV as a workflow file

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/test_indexing_contract.py`
- Modify: `scripts/filter_and_write_mt.py:324-355`
- Modify: `workflow/FilterMT.wdl:47-49,103-105`

**Interfaces:**
- Consumes: the existing `annotations_tsv` cloud URI created in `filter_and_write_mt.py`.
- Produces: `TaskFilterMT.PathAnnotations: File` and `FilterMT.PathAnnotations: File`, read from `annotations_outpath.txt`.

- [ ] **Step 1: Create the failing annotation-output contract test**

Create `tests/__init__.py` as an empty file. Create `tests/test_indexing_contract.py` with:

```python
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class AnnotationOutputContractTests(unittest.TestCase):
    def test_filter_script_records_annotation_cloud_path(self):
        script = (ROOT / "scripts/filter_and_write_mt.py").read_text()
        self.assertIn("with open('annotations_outpath.txt', 'w') as file:", script)
        self.assertIn("file.write(annotations_tsv)", script)

    def test_filter_wdl_exposes_annotation_file(self):
        wdl = (ROOT / "workflow/FilterMT.wdl").read_text()
        self.assertIn("File PathAnnotations = read_string('annotations_outpath.txt')", wdl)
        self.assertIn("File PathAnnotations = TaskFilterMT.PathAnnotations", wdl)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the annotation-output tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_indexing_contract.AnnotationOutputContractTests -v
```

Expected: both tests fail because the path file and WDL outputs are absent.

- [ ] **Step 3: Record and expose the annotation URI**

Immediately after `annotations_ht.export(annotations_tsv)` in `scripts/filter_and_write_mt.py`, add:

```python
    with open('annotations_outpath.txt', 'w') as file:
        file.write(annotations_tsv)
```

Add to the `FilterMT` workflow output block:

```wdl
        File PathAnnotations = TaskFilterMT.PathAnnotations
```

Add to the `TaskFilterMT` output block:

```wdl
        File PathAnnotations = read_string('annotations_outpath.txt')
```

- [ ] **Step 4: Verify GREEN and validate the imported WDL**

Run:

```bash
python3 -m unittest tests.test_indexing_contract.AnnotationOutputContractTests -v
/opt/homebrew/bin/miniwdl check workflow/FilterMT.wdl
```

Expected: 2 tests pass and miniwdl exits 0.

- [ ] **Step 5: Commit the annotation-output contract**

```bash
git add tests/__init__.py tests/test_indexing_contract.py scripts/filter_and_write_mt.py workflow/FilterMT.wdl
git commit -m "feat: expose annotation export path"
```

---

### Task 2: Add Google Cloud CLI to the utility image

**Files:**
- Modify: `tests/test_indexing_contract.py`
- Modify: `envs/utils/Dockerfile:7-17`
- Modify: `.github/workflows/utils.yaml:3-14`

**Interfaces:**
- Consumes: the existing micromamba `base` environment and utility-image build workflow.
- Produces: `gcloud storage cp` in `ghcr.io/aou-multiomics-analysis/mttovcf/utils:<tag>` and CI rebuilds when the utility Dockerfile changes.

- [ ] **Step 1: Add failing image and CI contract tests**

Append this class to `tests/test_indexing_contract.py`:

```python
class UtilityImageContractTests(unittest.TestCase):
    def test_utility_image_installs_cloud_cli(self):
        dockerfile = (ROOT / "envs/utils/Dockerfile").read_text()
        self.assertIn("conda-forge::google-cloud-sdk=579.0.0", dockerfile)

    def test_utility_workflow_rebuilds_for_dockerfile_changes(self):
        workflow = (ROOT / ".github/workflows/utils.yaml").read_text()
        self.assertGreaterEqual(workflow.count("'envs/utils/Dockerfile'"), 2)
        self.assertGreaterEqual(workflow.count("'.github/workflows/utils.yaml'"), 2)
```

- [ ] **Step 2: Run the utility-image tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_indexing_contract.UtilityImageContractTests -v
```

Expected: both tests fail because Google Cloud SDK and Dockerfile workflow triggers are absent.

- [ ] **Step 3: Install the pinned cloud CLI and correct image triggers**

In `envs/utils/Dockerfile`, add this package to the existing micromamba install command before the bioconda packages:

```dockerfile
    conda-forge::google-cloud-sdk=579.0.0 \
```

In both `push.paths` and `pull_request.paths` in `.github/workflows/utils.yaml`, use:

```yaml
      - 'scripts/**'
      - 'envs/utils/Dockerfile'
      - '.github/workflows/utils.yaml'
```

- [ ] **Step 4: Verify GREEN**

Run:

```bash
python3 -m unittest tests.test_indexing_contract.UtilityImageContractTests -v
```

Expected: 2 tests pass.

- [ ] **Step 5: Build and smoke-test the utility image**

Run:

```bash
docker build -f envs/utils/Dockerfile -t mttovcf-utils:index-test .
docker run --rm mttovcf-utils:index-test bcftools --version
docker run --rm mttovcf-utils:index-test tabix --version
docker run --rm mttovcf-utils:index-test gcloud storage cp --help
```

Expected: the image builds and all three executable checks exit 0 through the normal micromamba entrypoint.

- [ ] **Step 6: Commit the utility-image change**

```bash
git add tests/test_indexing_contract.py envs/utils/Dockerfile .github/workflows/utils.yaml
git commit -m "build: add cloud CLI to utility image"
```

---

### Task 3: Make VCF indexing tagged, dynamic, and cloud-published

**Files:**
- Modify: `tests/test_indexing_contract.py`
- Modify: `main.wdl:6-28,36-70,94-117`

**Interfaces:**
- Consumes: `filter.PathVCF`, `VCFIndexDestination`, `UtilsImageTag`, and the three index disk parameters.
- Produces: `IndexVCF.Index: File`, whose value is the uploaded `gs://...vcf.bgz.tbi` URI.

- [ ] **Step 1: Add the failing VCF task contract test**

Append this class to `tests/test_indexing_contract.py`:

```python
class VcfIndexContractTests(unittest.TestCase):
    def test_vcf_index_is_tagged_dynamic_and_uploaded(self):
        wdl = (ROOT / "main.wdl").read_text()
        self.assertIn('String UtilsImageTag = "main"', wdl)
        self.assertIn("Float IndexDiskMultiplier = 2.0", wdl)
        self.assertIn("Int IndexDiskOverheadGiB = 10", wdl)
        self.assertIn("Int IndexMinDiskGiB = 20", wdl)
        self.assertIn('ceil(size(VCF, "GiB") * IndexDiskMultiplier)', wdl)
        self.assertIn("if CalculatedDiskGiB > IndexMinDiskGiB", wdl)
        self.assertIn('docker: "ghcr.io/aou-multiomics-analysis/mttovcf/utils:" + UtilsImageTag', wdl)
        self.assertIn('disks: "local-disk ~{IndexDiskGiB} SSD"', wdl)
        self.assertIn('gcloud storage cp "${index_name}" "~{IndexDestination}"', wdl)
        self.assertIn('File Index = read_string("index_outpath.txt")', wdl)
```

- [ ] **Step 2: Run the VCF contract test and verify RED**

Run:

```bash
python3 -m unittest tests.test_indexing_contract.VcfIndexContractTests -v
```

Expected: the test fails on the missing workflow inputs and dynamic task behavior.

- [ ] **Step 3: Replace `IndexVCF` with the dynamic publishing contract**

Give `IndexVCF` these inputs:

```wdl
        File VCF
        String IndexDestination
        String UtilsImageTag
        Float IndexDiskMultiplier
        Int IndexDiskOverheadGiB
        Int IndexMinDiskGiB
```

Add these task declarations after the input block:

```wdl
    Int CalculatedDiskGiB = ceil(size(VCF, "GiB") * IndexDiskMultiplier) + IndexDiskOverheadGiB
    Int IndexDiskGiB = if CalculatedDiskGiB > IndexMinDiskGiB then CalculatedDiskGiB else IndexMinDiskGiB
```

Use this command body:

```bash
        set -euo pipefail

        index_name="~{basename(VCF)}.tbi"
        bcftools index --tbi --force \
            --output "${index_name}" \
            "~{VCF}"
        test -s "${index_name}"
        gcloud storage cp "${index_name}" "~{IndexDestination}"
        printf '%s' "~{IndexDestination}" > index_outpath.txt
```

Use this runtime and output:

```wdl
    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/mttovcf/utils:" + UtilsImageTag
        memory: "256G"
        cpu: 64
        disks: "local-disk ~{IndexDiskGiB} SSD"
    }

    output {
        File Index = read_string("index_outpath.txt")
    }
```

- [ ] **Step 4: Wire the VCF index inputs and destination**

Add these top-level workflow inputs:

```wdl
        String UtilsImageTag = "main"
        Float IndexDiskMultiplier = 2.0
        Int IndexDiskOverheadGiB = 10
        Int IndexMinDiskGiB = 20
```

After `FullPrefix`, normalize the bucket and define the destination:

```wdl
    String NormalizedOutputBucket = sub(OutputBucket, "/+$", "")
    String VCFIndexDestination = NormalizedOutputBucket + "/" + FullPrefix + ".vcf.bgz.tbi"
```

Pass all six inputs to `IndexVCF`:

```wdl
            VCF = filter.PathVCF,
            IndexDestination = VCFIndexDestination,
            UtilsImageTag = UtilsImageTag,
            IndexDiskMultiplier = IndexDiskMultiplier,
            IndexDiskOverheadGiB = IndexDiskOverheadGiB,
            IndexMinDiskGiB = IndexMinDiskGiB
```

Rename the workflow output from generic `Index` to:

```wdl
        File VCFIndex = IndexVCF.Index
```

- [ ] **Step 5: Verify GREEN and WDL validity**

Run:

```bash
python3 -m unittest tests.test_indexing_contract.VcfIndexContractTests -v
/opt/homebrew/bin/miniwdl check main.wdl
```

Expected: the test passes and miniwdl exits 0.

- [ ] **Step 6: Commit VCF index publication**

```bash
git add tests/test_indexing_contract.py main.wdl
git commit -m "feat: publish dynamically sized VCF index"
```

---

### Task 4: Index and publish the annotation TSV

**Files:**
- Modify: `tests/test_indexing_contract.py`
- Modify: `main.wdl`

**Interfaces:**
- Consumes: `filter.PathAnnotations`, `AnnotationIndexDestination`, `UtilsImageTag`, and the same disk controls used by `IndexVCF`.
- Produces: `IndexAnnotations.Index: File`, whose value is the uploaded `gs://...annotations.tsv.bgz.tbi` URI.

- [ ] **Step 1: Add the failing annotation-index task contract test**

Append this class to `tests/test_indexing_contract.py`:

```python
class AnnotationIndexContractTests(unittest.TestCase):
    def test_annotation_index_uses_tabix_columns_and_is_published(self):
        wdl = (ROOT / "main.wdl").read_text()
        self.assertIn("task IndexAnnotations", wdl)
        self.assertIn('ceil(size(Annotations, "GiB") * IndexDiskMultiplier)', wdl)
        self.assertIn("tabix --force --sequence 1 --begin 2 --end 2 --skip-lines 1", wdl)
        self.assertIn('gcloud storage cp "${index_name}" "~{IndexDestination}"', wdl)
        self.assertIn("call IndexAnnotations", wdl)
        self.assertIn("Annotations = filter.PathAnnotations", wdl)
        self.assertIn("File PathAnnotations = filter.PathAnnotations", wdl)
        self.assertIn("File AnnotationIndex = IndexAnnotations.Index", wdl)
```

- [ ] **Step 2: Run the annotation-index test and verify RED**

Run:

```bash
python3 -m unittest tests.test_indexing_contract.AnnotationIndexContractTests -v
```

Expected: the test fails because `IndexAnnotations` and its outputs do not exist.

- [ ] **Step 3: Add `IndexAnnotations`**

Add a task with inputs matching `IndexVCF`, except the source is `File Annotations`. Compute `CalculatedDiskGiB` from `size(Annotations, "GiB")` and use the same minimum conditional.

Use this command body:

```bash
        set -euo pipefail

        annotation_name="~{basename(Annotations)}"
        ln -s "~{Annotations}" "${annotation_name}"
        tabix --force --sequence 1 --begin 2 --end 2 --skip-lines 1 "${annotation_name}"
        index_name="${annotation_name}.tbi"
        test -s "${index_name}"
        gcloud storage cp "${index_name}" "~{IndexDestination}"
        printf '%s' "~{IndexDestination}" > index_outpath.txt
```

Use the same tagged Docker image, memory, CPU, dynamic disk string, and `read_string("index_outpath.txt")` output as `IndexVCF`.

- [ ] **Step 4: Wire the annotation destination, call, and outputs**

After `VCFIndexDestination`, add:

```wdl
    String AnnotationIndexDestination = NormalizedOutputBucket + "/" + FullPrefix + ".annotations.tsv.bgz.tbi"
```

Call `IndexAnnotations` with:

```wdl
    call IndexAnnotations {
        input:
            Annotations = filter.PathAnnotations,
            IndexDestination = AnnotationIndexDestination,
            UtilsImageTag = UtilsImageTag,
            IndexDiskMultiplier = IndexDiskMultiplier,
            IndexDiskOverheadGiB = IndexDiskOverheadGiB,
            IndexMinDiskGiB = IndexMinDiskGiB
    }
```

Add these top-level outputs:

```wdl
        File PathAnnotations = filter.PathAnnotations
        File AnnotationIndex = IndexAnnotations.Index
```

- [ ] **Step 5: Verify GREEN and WDL validity**

Run:

```bash
python3 -m unittest tests.test_indexing_contract.AnnotationIndexContractTests -v
/opt/homebrew/bin/miniwdl check main.wdl
```

Expected: the test passes and miniwdl exits 0.

- [ ] **Step 6: Commit annotation indexing**

```bash
git add tests/test_indexing_contract.py main.wdl
git commit -m "feat: publish annotation tabix index"
```

---

### Task 5: Add container-level index smoke tests

**Files:**
- Create: `tests/fixtures/tiny.vcf`
- Create: `tests/fixtures/tiny.annotations.tsv`
- Create: `tests/smoke_test_indexes.sh`

**Interfaces:**
- Consumes: a built utility image supplied as argument 1, defaulting to `mttovcf-utils:index-test`.
- Produces: successful indexed region queries for both source formats; exits nonzero on any missing or invalid index.

- [ ] **Step 1: Create the VCF fixture**

Create `tests/fixtures/tiny.vcf`:

```text
##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	.	PASS	.
chr1	200	.	C	T	.	PASS	.
```

- [ ] **Step 2: Create the annotation fixture**

Create `tests/fixtures/tiny.annotations.tsv`:

```text
chrom	pos	ref	alt	AC	AN
chr1	100	A	G	1	20000
chr1	200	C	T	2	20000
```

- [ ] **Step 3: Create the smoke-test script**

Create executable `tests/smoke_test_indexes.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

image="${1:-mttovcf-utils:index-test}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT

cp "${repo_root}/tests/fixtures/tiny.vcf" "${work_dir}/tiny.vcf"
cp "${repo_root}/tests/fixtures/tiny.annotations.tsv" "${work_dir}/tiny.annotations.tsv"

docker run --rm -v "${work_dir}:/data" "${image}" bash -c '
    set -euo pipefail
    bgzip --stdout /data/tiny.vcf > /data/tiny.vcf.bgz
    cd /tmp
    ln -s /data/tiny.vcf.bgz tiny.vcf.bgz
    bcftools index --tbi --force --output tiny.vcf.bgz.tbi /data/tiny.vcf.bgz
    bcftools view --no-header --regions chr1:100-100 tiny.vcf.bgz | grep -q "^chr1[[:space:]]100[[:space:]]"

    bgzip --stdout /data/tiny.annotations.tsv > /data/tiny.annotations.tsv.bgz
    ln -s /data/tiny.annotations.tsv.bgz tiny.annotations.tsv.bgz
    tabix --force --sequence 1 --begin 2 --end 2 --skip-lines 1 tiny.annotations.tsv.bgz
    tabix tiny.annotations.tsv.bgz chr1:200-200 | grep -q "^chr1[[:space:]]200[[:space:]]"
'
```

- [ ] **Step 4: Run the smoke test against the pre-change image and verify the cloud-CLI check fails**

Run:

```bash
docker run --rm ghcr.io/aou-multiomics-analysis/mttovcf/utils:main gcloud storage cp --help
```

Expected: fail because `gcloud` is absent. This confirms the image change has a reproducible RED state.

- [ ] **Step 5: Run the complete smoke test against the rebuilt image**

Run:

```bash
chmod +x tests/smoke_test_indexes.sh
tests/smoke_test_indexes.sh mttovcf-utils:index-test
docker run --rm mttovcf-utils:index-test gcloud storage cp --help
```

Expected: both commands exit 0; each region query returns its expected row.

- [ ] **Step 6: Commit runtime fixtures**

```bash
git add tests/fixtures/tiny.vcf tests/fixtures/tiny.annotations.tsv tests/smoke_test_indexes.sh
git commit -m "test: smoke test tabix index commands"
```

---

### Task 6: Document inputs, outputs, and final verification

**Files:**
- Modify: `tests/test_indexing_contract.py`
- Modify: `README.md:15-44,77-81`

**Interfaces:**
- Consumes: final WDL input and output names from Tasks 1-4.
- Produces: user-facing documentation for utility image selection, disk sizing, annotation output, and both cloud index outputs.

- [ ] **Step 1: Add the failing README contract test**

Append this class to `tests/test_indexing_contract.py`:

```python
class ReadmeIndexContractTests(unittest.TestCase):
    def test_readme_documents_index_runtime_and_outputs(self):
        readme = (ROOT / "README.md").read_text()
        for name in (
            "UtilsImageTag",
            "IndexDiskMultiplier",
            "IndexDiskOverheadGiB",
            "IndexMinDiskGiB",
            "PathAnnotations",
            "VCFIndex",
            "AnnotationIndex",
        ):
            self.assertIn(f"`{name}`", readme)
```

- [ ] **Step 2: Run the README test and verify RED**

Run:

```bash
python3 -m unittest tests.test_indexing_contract.ReadmeIndexContractTests -v
```

Expected: fail on the first undocumented input.

- [ ] **Step 3: Update README inputs and outputs**

Add input-table rows documenting:

```markdown
| `UtilsImageTag` | Utility image tag used by VCF and annotation index tasks (default: `main`; `main.wdl` only) |
| `IndexDiskMultiplier` | Input-size multiplier for each index task's disk calculation (default: 2.0; `main.wdl` only) |
| `IndexDiskOverheadGiB` | GiB added after multiplying each index input size (default: 10; `main.wdl` only) |
| `IndexMinDiskGiB` | Minimum disk request in GiB for each index task (default: 20; `main.wdl` only) |
```

Replace the output paragraph with text stating that `main.wdl` emits `PathVCF`, `PathAnnotations`, `VCFIndex`, and `AnnotationIndex`, and that both `.tbi` files are uploaded beside their sources in `OutputBucket`.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
python3 -m unittest tests.test_indexing_contract.ReadmeIndexContractTests -v
```

Expected: 1 test passes.

- [ ] **Step 5: Run the complete verification suite**

Run fresh:

```bash
python3 -m unittest discover -s tests -v
/opt/homebrew/bin/miniwdl check main.wdl
docker build -f envs/utils/Dockerfile -t mttovcf-utils:index-test .
tests/smoke_test_indexes.sh mttovcf-utils:index-test
docker run --rm mttovcf-utils:index-test gcloud storage cp --help
git diff --check origin/codex/index-vcf-basename-output...HEAD
git status -sb
```

Expected: every unit test passes, miniwdl exits 0, the image builds, both index queries pass, Google Cloud CLI is available, the diff has no whitespace errors, and only intentional changes are present.

- [ ] **Step 6: Commit documentation**

```bash
git add tests/test_indexing_contract.py README.md
git commit -m "docs: describe dynamic index outputs"
```

- [ ] **Step 7: Review the complete branch diff**

Run:

```bash
git diff --stat origin/codex/index-vcf-basename-output...HEAD
git log --oneline origin/codex/index-vcf-basename-output..HEAD
```

Expected: the diff contains only the design/plan documents, index contracts and fixtures, annotation-path exposure, utility image/CI changes, `main.wdl` indexing changes, and README updates.
