# MTtoVCF

The MTtoVCF pipelines take a Hail matrix table, apply quality and cohort filters, annotate variants using the AoU Variant Annotation Table (VAT), and export a VCF.

The specific matrix table is referred to as the superset matrix table, consisting of 10,750 multiomic samples.

---

## Workflows

### 1. FilterMT.wdl *(primary workflow)*

This is the main, all-in-one workflow. It filters the matrix table, annotates variants from the VAT Hail table, and exports a VCF — all in a single step. The export step from the legacy `MTtoVCF.wdl` has been rolled into this workflow.

**Inputs:**

| Parameter | Description |
|---|---|
| `UriMatrixTable` | Path to the input Hail matrix table |
| `SampleList` | TSV file of samples to retain (keyed on `research_id`) |
| `MinAlleleCountThreshold` | Minimum allele count to retain a variant (default: 5) |
| `MaxAlleleCountThreshold` | Maximum allele count to retain a variant (default: unbounded) |
| `AlleleNumberPercentage` | Minimum AN as a percentage of the maximum possible AN (default: 95) |
| `VATHailTable` | Path to the pre-computed AoU VAT Hail table (see TSVtoHailTable.wdl); required when `AnnotateWithVAT` is `true` |
| `AnnotateWithVAT` | If `true`, add VAT annotations to the annotations TSV and VCF INFO fields (default: `true`) |
| `OutputBucket` | Cloud bucket path for the output VCF |
| `OutputPrefix` | Filename prefix for the output VCF |
| `CloudTmpdir` | Temporary cloud directory for Spark/Hail intermediate data |
| `BedFile` | *(optional)* BED file of genomic regions to restrict variants to |
| `Branch` | Docker image branch tag (default: `main`) |
| `TaskCpu` | CPU count for the Hail filter task (default: 64) |
| `TaskMemory` | Memory for the Hail filter task (default: `256G`) |
| `TaskDisk` | Local disk request for the Hail filter task (default: `local-disk 1000 SSD`) |
| `SparkDriverMemory` | Spark/Hail driver memory inside the task (default: `64g`) |
| `SparkParallelism` | Spark default parallelism (default: 100) |
| `SparkShufflePartitions` | Spark SQL shuffle partitions (default: 100) |
| `MakeDosage` | If `true`, convert the exported VCF to a genotype dosage TSV (default: `false`; `main.wdl` only) |
| `MakePlink` | If `true`, convert the exported VCF to PLINK 2 pgen/pvar/psam files (default: `false`; `main.wdl` only) |
| `MakeLoFCarriers` | If `true`, emit long-format sample-gene LoF carrier tables from the filtered rare-variant MatrixTable and VAT Hail Table (default: `false`; `main.wdl` only; requires `VATHailTable`) |
| `DosageThreads` | Threads for the optional bcftools dosage task (default: 4; `main.wdl` only) |
| `PlinkNewIdMaxAlleleLen` | Value passed to PLINK 2 `--new-id-max-allele-len` (default: 200; `main.wdl` only) |
| `LoFCarrierTaskCpu` | CPU count for the optional Hail-native LoF carrier task (default: 64; `main.wdl` only) |
| `LoFCarrierTaskMemory` | Memory for the optional Hail-native LoF carrier task (default: `256G`; `main.wdl` only) |
| `LoFCarrierTaskDisk` | Local disk request for the optional Hail-native LoF carrier task (default: `local-disk 1000 SSD`; `main.wdl` only) |
| `LoFCarrierSparkDriverMemory` | Spark/Hail driver memory inside the optional LoF carrier task (default: `64g`; `main.wdl` only) |
| `LoFCarrierSparkParallelism` | Spark default parallelism for the optional LoF carrier task (default: 100; `main.wdl` only) |
| `LoFCarrierSparkShufflePartitions` | Spark SQL shuffle partitions for the optional LoF carrier task (default: 100; `main.wdl` only) |

When running through `main.wdl`, these filter-task runtime inputs are exposed with a `Filter` prefix: `FilterTaskCpu`, `FilterTaskMemory`, `FilterTaskDisk`, `FilterSparkDriverMemory`, `FilterSparkParallelism`, and `FilterSparkShufflePartitions`.

Override these values upward when a larger runtime configuration is available.

**Filtering steps performed by `filter_and_write_mt.py`:**

1. **Region filter** *(optional)*: If a BED file is supplied, only variants overlapping those intervals are retained.
2. **Sample filter**: Columns (samples) are filtered to those present in the provided sample list.
3. **Biallelic filter**: Only biallelic sites (`len(alleles) == 2`) are kept.
4. **Non-empty genotype filter**: Rows where every genotype call is missing are dropped.
5. **Non-missing AC filter**: Rows with a missing `info.AC` are dropped.
6. **Quality filter**: Rows with any `FILTER` flag set are removed (only `PASS` variants are retained).
7. **Variant QC**: Hail `variant_qc` is run to compute Hardy–Weinberg equilibrium p-values and excess heterozygosity p-values, which are stored in the output INFO field (`ALL_p_value_hwe`, `ALL_p_value_excess_het`).
8. **AC/AN/AF recalculation**: Allele count, allele number, and allele frequency from `variant_qc` are reused for the post-filter cohort without a second genotype aggregation.
9. **Allele number percentage cutoff**: Variants are removed if `AN < AlleleNumberPercentage% × 2 × n_samples`.
10. **Allele count range filter**: Only variants with `MinAlleleCount ≤ AC ≤ MaxAlleleCount` are retained.

**VAT annotations added to the INFO field:**

| Field | Description |
|---|---|
| `gvs_all_ac` / `gvs_all_an` / `gvs_all_af` | AoU cohort-wide AC/AN/AF |
| `gvs_max_ac` / `gvs_max_an` / `gvs_max_af` / `gvs_max_subpop` | AoU max-subpopulation AC/AN/AF and subpop label |
| `gnomad_all_ac` / `gnomad_all_an` / `gnomad_all_af` | gnomAD cohort-wide AC/AN/AF |
| `gnomad_max_ac` / `gnomad_max_an` / `gnomad_max_af` / `gnomad_max_subpop` | gnomAD max-subpopulation AC/AN/AF and subpop label |
| `clinvar_classification` | ClinVar clinical significance |
| `clinvar_phenotype` | ClinVar associated phenotype |
| `omim_phenotypes_id` | OMIM phenotype identifiers |
| `consequence` | Variant consequence |
| `revel` | REVEL pathogenicity score |
| `splice_ai_acceptor_gain_score` / `_loss_score` | SpliceAI acceptor gain/loss scores |
| `splice_ai_donor_gain_score` / `_loss_score` | SpliceAI donor gain/loss scores |
| `splice_ai_acceptor_gain_distance` / `_loss_distance` | SpliceAI acceptor gain/loss distances |
| `splice_ai_donor_gain_distance` / `_loss_distance` | SpliceAI donor gain/loss distances |

**Output:** A bgzipped VCF (`<OutputPrefix>.vcf.bgz`) written to `<OutputBucket>`.

The final filtered MatrixTable is materialized once under `CloudTmpdir` using
the run-specific name
`<OutputPrefix>.AC<min>-<max>.filtered_rare_variants.mt`. It contains the
filtered samples, genotypes, and final row annotations and is exposed as the
`FilteredMatrixTable` output. When `MakeLoFCarriers=true`, the Hail LoF task
consumes this MatrixTable and skips the sample, BED, and variant QC filters.

The workflow also writes the following annotation outputs:

- `<OutputPrefix>.annotations.tsv.bgz`: existing one-row-per-variant annotations.
- `<OutputPrefix>.transcript_annotations.tsv.bgz`: one row per retained
  variant-transcript combination, emitted when `AnnotateWithVAT=true` and
  exposed as the optional `TranscriptAnnotations` WDL output.

The transcript annotations TSV contains `chrom`, `pos`, `ref`, `alt`, `rsid`,
`gene_id`, `gene_symbol`, `transcript`, `is_canonical_transcript`,
`consequence`, `aa_change`, `LoF`, `LoF_filter`, `LoF_flags`, `LoF_info`,
`gvs_max_af`, and `gvs_max_subpop`. Intergenic rows may have missing `gene_id`
and/or `transcript`. Transcript annotations are filtered to retained variants
without expanding the MatrixTable. Downstream analyses should choose the most
severe consequence for the matched gene when collapsing transcript-level rows.

The source VAT Hail Table must include `vid`, `dbsnp_rsid`, `gene_id`,
`gene_symbol`, `transcript`, `is_canonical_transcript`, `consequence`,
`aa_change`, `LoF`, `LoF_filter`, `LoF_flags`, `LoF_info`, `gvs_max_af`, and
`gvs_max_subpop` in addition to the fields already used by the variant-level
annotations. The table may be keyed by `(vid, transcript)`. When
`AnnotateWithVAT=true`, these transcript fields are validated before any Hail
projection; a missing-field error lists every absent field. Older precomputed
VAT Hail Tables that lack any of these columns must be regenerated from a VAT
export containing them. No VAT schema validation occurs when annotation is
disabled.

When running through `main.wdl`, the exported VCF is always indexed. If `MakeDosage` is enabled, the workflow also emits `<FullPrefix>.dose.tsv.gz` and its `.tbi` index. If `MakePlink` is enabled, it emits `<FullPrefix>.pgen`, `<FullPrefix>.pvar`, and `<FullPrefix>.psam`. If `MakeLoFCarriers` is enabled, `VATHailTable` must be supplied and the workflow emits `<FullPrefix>.lof_variants.vcf.bgz`, its `.tbi` index, `<FullPrefix>.splice_acceptor_carriers.tsv.bgz`, `<FullPrefix>.splice_donor_carriers.tsv.bgz`, `<FullPrefix>.lof_carriers.HC.tsv.bgz`, and `<FullPrefix>.lof_carriers.HC_or_LC.tsv.bgz`.

The carrier outputs are sparse long-format sample-gene tables with one row per
sample and gene where the sample carries at least one qualifying non-reference
variant. The initial internal groups are based on VEP `consequence` values:
`splice_acceptor_variant` writes the splice-acceptor carrier table and
`splice_donor_variant` writes the splice-donor carrier table. Each group is
written to its own sparse sample-gene table, and the existing LoF HC and
HC-or-LC tables remain unchanged. HC-only uses VAT rows with `LoF == "HC"`;
HC-or-LC uses `LoF` in `{"HC", "LC"}`. Genotypes with any non-reference allele
count as carriers. In `main.wdl`, this optional output is generated by a
Hail-native subworkflow that reads the filtered rare-variant MatrixTable
produced by `FilterMT` and the VAT Hail Table, joins annotations by parsed VAT
`vid`, and then aggregates carriers by sample and gene. The standalone Hail
workflow retains its original filtering behavior by default;
`MatrixTableAlreadyFiltered=true` enables the reuse path. Output columns are
`sample_id`, `gene_id`, `gene_symbol`, `has_lof_variant`, `n_lof_variants`,
`variant_ids`, and `lof_classes` for the LoF tables, and the group-specific
presence/count fields plus `variant_ids` and the annotation-value field for the
splice tables. The LoF VCF is exported from the same materialized filtered Hail
data and contains flat `LOF_GENE_ID`, `LOF_GENE_SYMBOL`, and `LOF_CLASS` INFO
arrays. Its tabix index is created afterward by a separate `bcftools index`
task; carrier tables are derived from the Hail intermediate rather than by
parsing the VCF.

For pipeline testing without VAT, set `AnnotateWithVAT = false` and omit `VATHailTable`. The annotations TSV and VCF still include filtered cohort statistics and variant QC fields, but VAT-derived fields are omitted.

Run the real transcript-export regression in the production-pinned Hail image:

```bash
docker run --rm --platform linux/amd64 \
  -v "$PWD:/workspace" -w /workspace \
  hailgenetics/hail:0.2.134-py3.11 \
  python3 -m unittest tests.test_filter_and_write_mt_hail_integration -v
```

With miniwdl and Docker available, smoke-test the disabled-VAT optional output
with `python3 -m unittest tests.test_filtermt_optional_output_smoke -v`.

---

### 2. LoFCarrierTable.wdl

This utility workflow creates sparse long-format carrier tables from an
exported VCF and the corresponding transcript annotations TSV. Use this for
post hoc runs where a VCF and transcript annotations already exist. The
extractor scans the configured groups in one pass, and groups with no matches
still write header-only outputs.

**Inputs:**

| Parameter | Description |
|---|---|
| `vcf_file` | Exported bgzipped VCF containing sample genotype fields |
| `vcf_index` | Tabix index for `vcf_file` |
| `transcript_annotations_tsv` | Transcript annotations TSV/BGZ emitted by the filter workflow |
| `output_prefix` | Filename prefix for the carrier outputs |
| `threads` | Threads for `bcftools view -R` (default: 4) |
| `task_memory` | Memory for the extraction task (default: `32G`) |
| `task_disk` | Local disk request for the extraction task (default: `local-disk 500 SSD`) |

**Outputs:**

- `<output_prefix>.lof_carriers.HC.tsv.gz`
- `<output_prefix>.lof_carriers.HC_or_LC.tsv.gz`
- `<output_prefix>.splice_acceptor_carriers.tsv.gz`
- `<output_prefix>.splice_donor_carriers.tsv.gz`

---

### 3. HailLoFCarrierTable.wdl

This utility workflow creates the same sparse long-format carrier tables
directly from a MatrixTable plus a VAT Hail Table. It does not require the
transcript annotations TSV or an exported VCF, so it is the preferred route for
rerunning carrier extraction across new sample sets when VAT is already
available in Hail format. The extractor aggregates the configured groups in one
pass, and groups with no matches still write header-only outputs.

**Inputs:**

| Parameter | Description |
|---|---|
| `UriMatrixTable` | Path to the input Hail matrix table |
| `SampleList` | TSV file of samples to retain (keyed on `research_id`) |
| `VATHailTable` | VAT Hail Table containing `vid`, `gene_id`, `gene_symbol`, `LoF`, and `consequence` |
| `OutputBucket` | Cloud bucket path for the output carrier tables |
| `OutputPrefix` | Filename prefix for the carrier outputs |
| `CloudTmpdir` | Temporary cloud directory for Spark/Hail intermediate data |
| `BedFile` | *(optional)* BED file of genomic regions to restrict variants to |
| `MinAlleleCountThreshold` | Minimum allele count to retain a variant (default: 5) |
| `MaxAlleleCountThreshold` | Maximum allele count to retain a variant (default: unbounded) |
| `AlleleNumberPercentage` | Minimum AN as a percentage of the maximum possible AN (default: 95) |
| `TaskCpu` | CPU count for the Hail extraction task (default: 64) |
| `TaskMemory` | Memory for the Hail extraction task (default: `256G`) |
| `TaskDisk` | Local disk request for the Hail extraction task (default: `local-disk 1000 SSD`) |
| `SparkDriverMemory` | Spark/Hail driver memory inside the task (default: `64g`) |
| `SparkParallelism` | Spark default parallelism (default: 100) |
| `SparkShufflePartitions` | Spark SQL shuffle partitions (default: 100) |
| `IndexCpu` | CPU count for the post hoc VCF indexing task (default: 4) |
| `IndexMemory` | Memory for the post hoc VCF indexing task (default: `32G`) |
| `IndexDisk` | Local disk request for the post hoc VCF indexing task (default: `local-disk 100 SSD`) |

**Outputs:**

- `<output_prefix>.lof_variants.vcf.bgz`
- `<output_prefix>.lof_variants.vcf.bgz.tbi`
- `<output_prefix>.lof_carriers.HC.tsv.bgz`
- `<output_prefix>.lof_carriers.HC_or_LC.tsv.bgz`
- `<output_prefix>.splice_acceptor_carriers.tsv.bgz`
- `<output_prefix>.splice_donor_carriers.tsv.bgz`

---

### 4. TSVtoHailTable.wdl

This utility workflow converts an AoU Variant Annotation Table (VAT) TSV (or TSV.gz / BGZ) into a Hail table stored in cloud storage. The resulting Hail table is used by `FilterMT.wdl` to efficiently annotate variants at scale, avoiding the overhead of re-importing the flat file on every run.

**Inputs:**

| Parameter | Description |
|---|---|
| `InputTSVPath` | Cloud path to the input TSV or TSV.gz (e.g. `gs://bucket/vat.tsv.gz`) |
| `OutputTablePath` | Cloud path where the output Hail table will be written (e.g. `gs://bucket/vat.ht`) |
| `CloudTmpdir` | Temporary cloud directory for Spark/Hail intermediate data |
| `KeyField` | *(optional)* Field name to use as the Hail table key |
| `ForceBGZ` | Set to `true` when the `.gz` file is block-gzipped to enable parallel reads (default: `false`) |
| `Branch` | Docker image branch tag (default: `main`) |

**Output:** A Hail table written to `OutputTablePath`.

---

### 5. MTtoVCF.wdl *(legacy — export only)*

> **This workflow is legacy.** The export step has been rolled into `FilterMT.wdl`. Use `FilterMT.wdl` for all new work.

This workflow takes a pre-existing matrix table and exports it directly to a VCF without any filtering or annotation. It is retained for compatibility with existing pipelines that produce a filtered matrix table in a separate step.

**Inputs:**

| Parameter | Description |
|---|---|
| `UriMatrixTable` | Path to the input Hail matrix table |
| `OutputBucket` | Cloud bucket path for the output VCF |
| `OutputPrefix` | Filename prefix for the output VCF |
| `CloudTmpdir` | Temporary cloud directory for Spark/Hail intermediate data |
| `Branch` | Docker image branch tag (default: `main`) |

**Output:** A bgzipped VCF (`<OutputPrefix>.vcf.bgz`) written to `<OutputBucket>`.

---
