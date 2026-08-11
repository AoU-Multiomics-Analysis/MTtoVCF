# VAT Transcript Annotation Export Design

## Context

The All of Us Variant Annotation Table (VAT) contains one row per
variant-transcript combination, keyed by `vid` and `transcript`. The current
MTtoVCF filtering workflow rekeys VAT rows by variant (`locus`, `alleles`) for
annotation of a variant-level MatrixTable. Consequently, the exported
`<prefix>.annotations.tsv.bgz` does not retain the gene and transcript identity
needed for gene-matched rare-variant enrichment.

The existing VCF and annotations TSV are consumed as one-row-per-variant
artifacts. Changing either artifact to transcript-level rows would be a breaking
change.

## Decision

Preserve all existing outputs and add a companion transcript-level annotation
file:

`<OutputPrefix>.transcript_annotations.tsv.bgz`

The companion file will contain every VAT variant-transcript row whose variant
survives the existing MTtoVCF filters. The genotype MatrixTable will remain at
one row per variant.

## Output contract

Each row represents one retained variant-transcript combination. The output
will include:

- Variant identity: `chrom`, `pos`, `ref`, `alt`, `rsid`
- Gene/transcript identity: `gene_id`, `gene_symbol`, `transcript`,
  `is_canonical_transcript`
- Functional annotation: `consequence`, `aa_change`, `LoF`, `LoF_filter`,
  `LoF_flags`, `LoF_info`
- Population rarity fields: `gvs_max_af`, `gvs_max_subpop`

Rows with no overlapping transcript, such as intergenic variants, may contain
missing gene and transcript values and will be retained. Downstream gene-matched
analyses can discard those rows.

The transcript annotations output is optional at the WDL interface. It is
present when `AnnotateWithVAT=true` and absent when VAT annotation is disabled.

## Processing design

1. Read the VAT Hail Table once and parse `vid` into GRCh38 `locus` and
   biallelic `alleles` using the current contig-normalization behavior.
2. Retain the current variant-level VAT projection for the existing VCF and
   annotations TSV without changing their schemas or names.
3. Build a second transcript-level projection containing the output fields
   above and key it by variant for filtering.
4. After all current MatrixTable filters have run, create a key-only Hail Table
   from the retained MatrixTable rows.
5. Semi-join the transcript-level VAT projection to those retained variant keys.
   This filters annotations without expanding the genotype MatrixTable and
   avoids collecting chromosome-scale annotations in driver memory.
6. Export the companion BGZF TSV to `OutputBucket` and expose its path through
   `workflow/FilterMT.wdl` and `main.wdl`.

The export will remain ordered by genomic variant key so it can be indexed or
streamed efficiently by downstream workflows.

## Validation and failures

When `AnnotateWithVAT=true`, the script will validate that the input VAT Hail
Table contains all fields required by the companion output. Missing fields will
cause an early error listing the absent columns. When VAT annotation is disabled,
no transcript schema validation or transcript export will occur.

The implementation will verify:

- Existing WDL files continue to pass `miniwdl check`.
- Python source compiles.
- Field-contract tests cover required VAT columns, optional VAT behavior, and
  propagation of the new optional output through both WDL workflow layers.
- Existing VCF and variant-level annotation names and schemas remain unchanged.

## Downstream responsibility

RareVariantEnrichment will match version-normalized Ensembl gene IDs from this
file to the versioned Ensembl IDs in the expression BED. It will collapse
multiple transcript rows to the most severe consequence for the matched gene.
That analysis policy remains downstream so no transcript information is lost in
MTtoVCF.

## Alternatives rejected

- **Replace the current annotations TSV with transcript rows:** rejected because
  it would duplicate variants and break existing one-row-per-variant consumers.
- **Collapse to one consequence per gene in MTtoVCF:** rejected because it would
  discard transcript detail and hard-code consequence-ranking policy into the
  extraction workflow.
- **Encode all transcript annotations in VCF INFO:** rejected because nested
  transcript records are awkward to represent, inflate the VCF, and complicate
  downstream parsing.
