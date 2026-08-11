# Dynamic Tabix Indexing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reliably create, publish, and return Tabix indexes for the exported VCF and annotation TSV while sizing each indexing task from its own localized input.

**Architecture:** `FilterMT` exposes both cloud-exported source files. Separate `IndexVCF` and `IndexAnnotations` tasks localize one source each, request dynamic disk, call tested utility scripts to create a task-local `.tbi`, and upload it beside the source with Google Cloud CLI. The utility image contains bcftools, tabix, Google Cloud CLI, and the two index scripts and is always referenced by an explicit tag.

**Tech Stack:** WDL 1.0, Cromwell/miniwdl, Hail, bcftools 1.24, tabix 1.24, Google Cloud CLI 579.0.0, Docker, Python `unittest`.

## Global Constraints

- Preserve the existing `codex/index-vcf-basename-output` behavior: indexes are created in the task working directory, not beside Cromwell's localized input.
- Default disk sizing is `max(20 GiB, ceil(input GiB × 2.0) + 10 GiB)` and is computed independently for the VCF and annotation TSV.
- Expose `IndexDiskMultiplier`, `IndexDiskOverheadGiB`, and `IndexMinDiskGiB` as workflow inputs with defaults `2.0`, `10`, and `20`.
- Expose `UtilsImageTag` independently from the Hail-image `Branch` input, with default `"main"`.
- Publish `<FullPrefix>.vcf.bgz.tbi` and `<FullPrefix>.annotations.tsv.bgz.tbi` directly under normalized `OutputBucket`.
- Index annotation columns as one-based `chrom=1`, `pos=2`, `end=2`, skipping the single un-commented header line.
- Do not move indexing into the Hail filter task and do not change dosage or PLINK runtime sizing.
- Tests must exercise observable behavior or parsed WDL interfaces. Do not assert raw source text or README wording.
- Use test-first changes: run each named test and observe its expected failure before editing production files.

---

### Task 1: Expose the annotation TSV through a tested output manifest

**Files:**
- Create: `scripts/output_manifest.py`
- Create: `tests/__init__.py`
- Create: `tests/test_output_manifest.py`
- Modify: `scripts/filter_and_write_mt.py:1-3,324-355`
- Modify: `workflow/FilterMT.wdl:47-49,103-105`

**Interfaces:**
- Consumes: a local manifest filename and a cloud URI.
- Produces: `write_output_manifest(filename: str, cloud_uri: str) -> None`, plus `TaskFilterMT.PathAnnotations: File` and `FilterMT.PathAnnotations: File` read from `annotations_outpath.txt`.

- [ ] **Step 1: Write the failing manifest behavior test**

Create `tests/__init__.py` as an empty file and create `tests/test_output_manifest.py`:

```python
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from output_manifest import write_output_manifest


class OutputManifestTests(unittest.TestCase):
    def test_write_output_manifest_records_exact_cloud_uri(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "annotations_outpath.txt"
            uri = "gs://bucket/results/sample.annotations.tsv.bgz"

            write_output_manifest(str(manifest), uri)

            self.assertEqual(manifest.read_text(), uri)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
python3 -m unittest tests.test_output_manifest -v
```

Expected: import failure because `scripts/output_manifest.py` does not exist.

- [ ] **Step 3: Implement the manifest helper and use it for both exports**

Create `scripts/output_manifest.py`:

```python
from pathlib import Path


def write_output_manifest(filename: str, cloud_uri: str) -> None:
    Path(filename).write_text(cloud_uri)
```

Import it in `scripts/filter_and_write_mt.py`:

```python
from output_manifest import write_output_manifest
```

After `annotations_ht.export(annotations_tsv)`, add:

```python
    write_output_manifest('annotations_outpath.txt', annotations_tsv)
```

Replace the existing manual `outpath.txt` write with:

```python
    write_output_manifest('outpath.txt', OutputFilePath)
```

Expose `PathAnnotations` from both output blocks in `workflow/FilterMT.wdl`:

```wdl
        File PathAnnotations = TaskFilterMT.PathAnnotations
```

```wdl
        File PathAnnotations = read_string('annotations_outpath.txt')
```

- [ ] **Step 4: Verify GREEN and WDL validity**

Run:

```bash
python3 -m unittest tests.test_output_manifest -v
/opt/homebrew/bin/miniwdl check workflow/FilterMT.wdl
```

Expected: 1 test passes and miniwdl exits 0.

- [ ] **Step 5: Commit**

```bash
git add scripts/output_manifest.py scripts/filter_and_write_mt.py tests/__init__.py tests/test_output_manifest.py workflow/FilterMT.wdl
git commit -m "feat: expose annotation export path"
```

---

### Task 2: Add Google Cloud CLI to the utility image

**Files:**
- Modify: `envs/utils/Dockerfile:7-17`
- Modify: `.github/workflows/utils.yaml:3-14`

**Interfaces:**
- Consumes: the existing micromamba `base` environment and utility-image build workflow.
- Produces: `gcloud storage cp` in `ghcr.io/aou-multiomics-analysis/mttovcf/utils:<tag>`, with CI rebuilds when the utility Dockerfile or its workflow changes.

- [ ] **Step 1: Verify the published image is RED**

Run:

```bash
docker run --rm ghcr.io/aou-multiomics-analysis/mttovcf/utils:main gcloud storage cp --help
```

Expected: nonzero exit because `gcloud` is absent.

- [ ] **Step 2: Install the pinned cloud CLI**

Add this package to the existing micromamba install command before the bioconda packages:

```dockerfile
    conda-forge::google-cloud-sdk=579.0.0 \
```

- [ ] **Step 3: Correct the utility-image workflow triggers**

In both `push.paths` and `pull_request.paths` in `.github/workflows/utils.yaml`, use:

```yaml
      - 'scripts/**'
      - 'envs/utils/Dockerfile'
      - '.github/workflows/utils.yaml'
```

- [ ] **Step 4: Build and verify GREEN behavior**

Run:

```bash
docker build -f envs/utils/Dockerfile -t mttovcf-utils:index-test .
docker run --rm mttovcf-utils:index-test bcftools --version
docker run --rm mttovcf-utils:index-test tabix --version
docker run --rm mttovcf-utils:index-test gcloud storage cp --help
```

Expected: image build succeeds and all executable checks exit 0 through the normal micromamba entrypoint.

- [ ] **Step 5: Commit**

```bash
git add envs/utils/Dockerfile .github/workflows/utils.yaml
git commit -m "build: add cloud CLI to utility image"
```

---

### Task 3: Implement and wire dynamically sized VCF indexing

**Files:**
- Create: `scripts/index_vcf.sh`
- Create: `tests/fixtures/tiny.vcf`
- Create: `tests/fake_gcloud.sh`
- Create: `tests/smoke_test_vcf_index.sh`
- Modify: `main.wdl:6-28,36-70,94-117`

**Interfaces:**
- Consumes: `/index_vcf.sh <localized-vcf> <index-destination>`, `filter.PathVCF`, `UtilsImageTag`, and the three disk controls.
- Produces: `index_outpath.txt` containing the destination URI and `IndexVCF.Index: File` resolving to `<OutputBucket>/<FullPrefix>.vcf.bgz.tbi`.

- [ ] **Step 1: Create the VCF fixture and fake cloud-copy boundary**

Create `tests/fixtures/tiny.vcf`:

```text
##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	G	.	PASS	.
chr1	200	.	C	T	.	PASS	.
```

Create executable `tests/fake_gcloud.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
test "$1" = "storage"
test "$2" = "cp"
cp "$3" "$4"
```

- [ ] **Step 2: Write the failing VCF behavior smoke test**

Create executable `tests/smoke_test_vcf_index.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

image="${1:-mttovcf-utils:index-test}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT

mkdir -p "${work_dir}/published" "${work_dir}/task"
cp "${repo_root}/tests/fixtures/tiny.vcf" "${work_dir}/tiny.vcf"
cp "${repo_root}/tests/fake_gcloud.sh" "${work_dir}/fake_gcloud"
chmod +x "${work_dir}/fake_gcloud"

docker run --rm \
    -v "${work_dir}:/data" \
    -v "${work_dir}/fake_gcloud:/opt/conda/bin/gcloud:ro" \
    "${image}" bash -c '
        set -euo pipefail
        bgzip --stdout /data/tiny.vcf > /data/tiny.vcf.bgz
        cd /data/task
        /index_vcf.sh /data/tiny.vcf.bgz /data/published/tiny.vcf.bgz.tbi
        test "$(cat index_outpath.txt)" = "/data/published/tiny.vcf.bgz.tbi"
        ln -s /data/tiny.vcf.bgz tiny.vcf.bgz
        ln -s /data/published/tiny.vcf.bgz.tbi tiny.vcf.bgz.tbi
        bcftools view --no-header --regions chr1:100-100 tiny.vcf.bgz |
            grep -q "^chr1[[:space:]]100[[:space:]]"
    '
```

- [ ] **Step 3: Run the smoke test and verify RED**

Run:

```bash
chmod +x tests/fake_gcloud.sh tests/smoke_test_vcf_index.sh
tests/smoke_test_vcf_index.sh mttovcf-utils:index-test
```

Expected: fail because `/index_vcf.sh` is absent from the image.

- [ ] **Step 4: Implement the exact VCF index script**

Create executable `scripts/index_vcf.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

vcf="$1"
index_destination="$2"
index_name="$(basename "${vcf}").tbi"

bcftools index --tbi --force --output "${index_name}" "${vcf}"
test -s "${index_name}"
gcloud storage cp "${index_name}" "${index_destination}"
printf '%s' "${index_destination}" > index_outpath.txt
```

Rebuild `mttovcf-utils:index-test` so the Dockerfile's existing `COPY scripts/* .` includes the script.

- [ ] **Step 5: Replace `IndexVCF` with the tagged dynamic task**

Give `IndexVCF` these inputs:

```wdl
        File VCF
        String IndexDestination
        String UtilsImageTag
        Float IndexDiskMultiplier
        Int IndexDiskOverheadGiB
        Int IndexMinDiskGiB
```

Add:

```wdl
    Int CalculatedDiskGiB = ceil(size(VCF, "GiB") * IndexDiskMultiplier) + IndexDiskOverheadGiB
    Int IndexDiskGiB = if CalculatedDiskGiB > IndexMinDiskGiB then CalculatedDiskGiB else IndexMinDiskGiB
```

The command calls the tested script:

```wdl
    command <<<
        /index_vcf.sh "~{VCF}" "~{IndexDestination}"
    >>>
```

Use:

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

- [ ] **Step 6: Wire workflow inputs, normalized destination, call, and output**

Add workflow inputs:

```wdl
        String UtilsImageTag = "main"
        Float IndexDiskMultiplier = 2.0
        Int IndexDiskOverheadGiB = 10
        Int IndexMinDiskGiB = 20
```

After `FullPrefix`, add:

```wdl
    String NormalizedOutputBucket = sub(OutputBucket, "/+$", "")
    String VCFIndexDestination = NormalizedOutputBucket + "/" + FullPrefix + ".vcf.bgz.tbi"
```

Pass all task inputs in the `IndexVCF` call and expose:

```wdl
        File VCFIndex = IndexVCF.Index
```

- [ ] **Step 7: Verify GREEN**

Run:

```bash
docker build -f envs/utils/Dockerfile -t mttovcf-utils:index-test .
tests/smoke_test_vcf_index.sh mttovcf-utils:index-test
/opt/homebrew/bin/miniwdl check main.wdl
```

Expected: the real script publishes a valid queryable index through the fake cloud-copy boundary, and miniwdl exits 0.

- [ ] **Step 8: Commit**

```bash
git add scripts/index_vcf.sh tests/fixtures/tiny.vcf tests/fake_gcloud.sh tests/smoke_test_vcf_index.sh main.wdl
git commit -m "feat: publish dynamically sized VCF index"
```

---

### Task 4: Implement and wire annotation TSV indexing

**Files:**
- Create: `scripts/index_annotations.sh`
- Create: `tests/fixtures/tiny.annotations.tsv`
- Create: `tests/smoke_test_annotation_index.sh`
- Modify: `main.wdl`

**Interfaces:**
- Consumes: `/index_annotations.sh <localized-bgzip-tsv> <index-destination>`, `filter.PathAnnotations`, and the same tag/disk controls as `IndexVCF`.
- Produces: `IndexAnnotations.Index: File` resolving to `<OutputBucket>/<FullPrefix>.annotations.tsv.bgz.tbi`.

- [ ] **Step 1: Create the annotation fixture and failing smoke test**

Create `tests/fixtures/tiny.annotations.tsv`:

```text
chrom	pos	ref	alt	AC	AN
chr1	100	A	G	1	20000
chr1	200	C	T	2	20000
```

Create executable `tests/smoke_test_annotation_index.sh` using the same image, temporary directory, and fake-gcloud mount as the VCF smoke test. Its container command must:

```bash
bgzip --stdout /data/tiny.annotations.tsv > /data/tiny.annotations.tsv.bgz
cd /data/task
/index_annotations.sh /data/tiny.annotations.tsv.bgz /data/published/tiny.annotations.tsv.bgz.tbi
test "$(cat index_outpath.txt)" = "/data/published/tiny.annotations.tsv.bgz.tbi"
ln -s /data/tiny.annotations.tsv.bgz tiny.annotations.tsv.bgz
ln -s /data/published/tiny.annotations.tsv.bgz.tbi tiny.annotations.tsv.bgz.tbi
tabix tiny.annotations.tsv.bgz chr1:200-200 |
    grep -q "^chr1[[:space:]]200[[:space:]]"
```

- [ ] **Step 2: Run the annotation smoke test and verify RED**

Run:

```bash
chmod +x tests/smoke_test_annotation_index.sh
tests/smoke_test_annotation_index.sh mttovcf-utils:index-test
```

Expected: fail because `/index_annotations.sh` is absent.

- [ ] **Step 3: Implement the exact annotation index script**

Create executable `scripts/index_annotations.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

annotations="$1"
index_destination="$2"
annotation_name="$(basename "${annotations}")"

ln -s "${annotations}" "${annotation_name}"
tabix --force --sequence 1 --begin 2 --end 2 --skip-lines 1 "${annotation_name}"
index_name="${annotation_name}.tbi"
test -s "${index_name}"
gcloud storage cp "${index_name}" "${index_destination}"
printf '%s' "${index_destination}" > index_outpath.txt
```

- [ ] **Step 4: Add `IndexAnnotations` and wire it**

Add a task with the same tag/disk inputs and runtime as `IndexVCF`, but compute size from `File Annotations` and call:

```wdl
    command <<<
        /index_annotations.sh "~{Annotations}" "~{IndexDestination}"
    >>>
```

Add:

```wdl
    String AnnotationIndexDestination = NormalizedOutputBucket + "/" + FullPrefix + ".annotations.tsv.bgz.tbi"
```

Call the task with `filter.PathAnnotations` and expose:

```wdl
        File PathAnnotations = filter.PathAnnotations
        File AnnotationIndex = IndexAnnotations.Index
```

- [ ] **Step 5: Verify GREEN**

Run:

```bash
docker build -f envs/utils/Dockerfile -t mttovcf-utils:index-test .
tests/smoke_test_annotation_index.sh mttovcf-utils:index-test
/opt/homebrew/bin/miniwdl check main.wdl
```

Expected: the exact annotation script produces a valid region-queryable index, and miniwdl exits 0.

- [ ] **Step 6: Commit**

```bash
git add scripts/index_annotations.sh tests/fixtures/tiny.annotations.tsv tests/smoke_test_annotation_index.sh main.wdl
git commit -m "feat: publish annotation tabix index"
```

---

### Task 5: Validate the compiled WDL interface

**Files:**
- Create: `tests/test_wdl_interface.py`

**Interfaces:**
- Consumes: miniwdl's parsed `WDL.Document` for `main.wdl`.
- Produces: behavior-level checks that the compiler sees the required workflow inputs/defaults, tasks, task inputs, runtime attributes, and workflow outputs.

- [ ] **Step 1: Write parsed-interface tests**

Create `tests/test_wdl_interface.py`:

```python
from pathlib import Path
import unittest

import WDL


ROOT = Path(__file__).resolve().parents[1]


class WdlInterfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = WDL.load(str(ROOT / "main.wdl"))
        cls.workflow = cls.document.workflow
        cls.tasks = {task.name: task for task in cls.document.tasks}

    def test_workflow_index_inputs_have_expected_defaults(self):
        inputs = {decl.name: decl for decl in self.workflow.inputs}
        expected = {
            "UtilsImageTag": "main",
            "IndexDiskMultiplier": 2.0,
            "IndexDiskOverheadGiB": 10,
            "IndexMinDiskGiB": 20,
        }
        for name, value in expected.items():
            self.assertIn(name, inputs)
            self.assertEqual(inputs[name].expr.literal.value, value)

    def test_compiler_sees_both_index_task_interfaces(self):
        expected_inputs = {
            "IndexDestination",
            "UtilsImageTag",
            "IndexDiskMultiplier",
            "IndexDiskOverheadGiB",
            "IndexMinDiskGiB",
        }
        for task_name, source_name in (("IndexVCF", "VCF"), ("IndexAnnotations", "Annotations")):
            task = self.tasks[task_name]
            self.assertEqual({decl.name for decl in task.inputs}, expected_inputs | {source_name})
            self.assertEqual({decl.name for decl in task.outputs}, {"Index"})
            self.assertEqual(set(task.runtime), {"docker", "memory", "cpu", "disks"})

    def test_workflow_exposes_sources_and_indexes(self):
        outputs = {decl.name: str(decl.type) for decl in self.workflow.outputs}
        self.assertEqual(outputs["PathVCF"], "File")
        self.assertEqual(outputs["PathAnnotations"], "File")
        self.assertEqual(outputs["VCFIndex"], "File")
        self.assertEqual(outputs["AnnotationIndex"], "File")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Mutation-check the tests**

On a temporary uncommitted edit, rename `AnnotationIndex` to `AnnotationIndexBroken`, then run the test with miniwdl's Python interpreter:

```bash
/opt/homebrew/opt/python@3.11/bin/python3.11 -m unittest tests.test_wdl_interface -v
```

Expected: `test_workflow_exposes_sources_and_indexes` fails. Revert only the temporary rename using `apply_patch`, then rerun and expect all 3 tests to pass.

- [ ] **Step 3: Validate all behavior tests together**

Run:

```bash
python3 -m unittest tests.test_output_manifest -v
/opt/homebrew/opt/python@3.11/bin/python3.11 -m unittest tests.test_wdl_interface -v
tests/smoke_test_vcf_index.sh mttovcf-utils:index-test
tests/smoke_test_annotation_index.sh mttovcf-utils:index-test
/opt/homebrew/bin/miniwdl check main.wdl
```

Expected: all unit tests and smoke tests pass and miniwdl exits 0.

- [ ] **Step 4: Commit**

```bash
git add tests/test_wdl_interface.py
git commit -m "test: validate index workflow interface"
```

---

### Task 6: Document and perform final verification

**Files:**
- Modify: `README.md:15-44,77-81`

**Interfaces:**
- Consumes: final WDL input/output names from Tasks 1-4.
- Produces: user-facing documentation for utility image selection, dynamic disk sizing, annotation output, and both cloud indexes.

- [ ] **Step 1: Update README inputs and outputs**

Add input-table rows:

```markdown
| `UtilsImageTag` | Utility image tag used by VCF and annotation index tasks (default: `main`; `main.wdl` only) |
| `IndexDiskMultiplier` | Input-size multiplier for each index task's disk calculation (default: 2.0; `main.wdl` only) |
| `IndexDiskOverheadGiB` | GiB added after multiplying each index input size (default: 10; `main.wdl` only) |
| `IndexMinDiskGiB` | Minimum disk request in GiB for each index task (default: 20; `main.wdl` only) |
```

Replace the output paragraph with text stating that `main.wdl` emits `PathVCF`, `PathAnnotations`, `VCFIndex`, and `AnnotationIndex`; both `.tbi` files are uploaded beside their sources in `OutputBucket`; and each indexing task requests `max(IndexMinDiskGiB, ceil(input GiB × IndexDiskMultiplier) + IndexDiskOverheadGiB)`.

- [ ] **Step 2: Run fresh complete verification**

Run:

```bash
python3 -m unittest tests.test_output_manifest -v
/opt/homebrew/opt/python@3.11/bin/python3.11 -m unittest tests.test_wdl_interface -v
/opt/homebrew/bin/miniwdl check main.wdl
docker build -f envs/utils/Dockerfile -t mttovcf-utils:index-test .
tests/smoke_test_vcf_index.sh mttovcf-utils:index-test
tests/smoke_test_annotation_index.sh mttovcf-utils:index-test
docker run --rm mttovcf-utils:index-test gcloud storage cp --help
git diff --check origin/codex/index-vcf-basename-output...HEAD
git status -sb
```

Expected: all tests pass, miniwdl exits 0, the image builds, both exact index scripts produce valid region-queryable indexes, Google Cloud CLI is available, the diff has no whitespace errors, and only intentional changes are present.

- [ ] **Step 3: Commit documentation**

```bash
git add README.md
git commit -m "docs: describe dynamic index outputs"
```

- [ ] **Step 4: Review the complete branch diff**

Run:

```bash
git diff --stat origin/codex/index-vcf-basename-output...HEAD
git log --oneline origin/codex/index-vcf-basename-output..HEAD
```

Expected: only design/plan documentation, manifest and index scripts, behavior tests and fixtures, annotation-path exposure, utility image/CI changes, `main.wdl` indexing changes, and README updates are present.
