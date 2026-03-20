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
| `VATHailTable` | Path to the pre-computed AoU VAT Hail table (see TSVtoHailTable.wdl) |
| `OutputBucket` | Cloud bucket path for the output VCF |
| `OutputPrefix` | Filename prefix for the output VCF |
| `CloudTmpdir` | Temporary cloud directory for Spark/Hail intermediate data |
| `BedFile` | *(optional)* BED file of genomic regions to restrict variants to |
| `Branch` | Docker image branch tag (default: `main`) |

**Filtering steps performed by `filter_and_write_mt.py`:**

1. **Region filter** *(optional)*: If a BED file is supplied, only variants overlapping those intervals are retained.
2. **Sample filter**: Columns (samples) are filtered to those present in the provided sample list.
3. **Biallelic filter**: Only biallelic sites (`len(alleles) == 2`) are kept.
4. **Non-empty genotype filter**: Rows where every genotype call is missing are dropped.
5. **Non-missing AC filter**: Rows with a missing `info.AC` are dropped.
6. **Quality filter**: Rows with any `FILTER` flag set are removed (only `PASS` variants are retained).
7. **Variant QC**: Hail `variant_qc` is run to compute Hardy–Weinberg equilibrium p-values and excess heterozygosity p-values, which are stored in the output INFO field (`ALL_p_value_hwe`, `ALL_p_value_excess_het`).
8. **AC/AN/AF recalculation**: Allele count, allele number, and allele frequency are recalculated on the post-filter cohort using `hl.agg.call_stats`.
9. **Allele number percentage cutoff**: Variants are removed if `AN < AlleleNumberPercentage% × 2 × n_samples`.
10. **Allele count range filter**: Only variants with `MinAlleleCount ≤ AC ≤ MaxAlleleCount` are retained.

**VAT annotations added to the INFO field:**

| Field | Description |
|---|---|
| `gvs_all_ac` / `gvs_all_an` / `gvs_all_af` | GVS cohort-wide AC/AN/AF |
| `gvs_max_ac` / `gvs_max_an` / `gvs_max_af` / `gvs_max_subpop` | GVS max-subpopulation AC/AN/AF and subpop label |
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

---

### 2. TSVtoHailTable.wdl

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

### 3. MTtoVCF.wdl *(legacy — export only)*

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

## ComputeGeneticPCs.wdl

Takes a VCF and computes genetic principal components (PCs) using plink2. This workflow is a standalone utility and is not part of the main filter/export pipeline. It is not currently included in this repository.
