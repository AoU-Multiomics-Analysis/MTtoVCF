# Generalized Annotation Carrier Tables Design

## Context

MTtoVCF currently extracts carrier tables only for VEP LoF annotations. The
standalone VCF/transcript-annotation extractor and the Hail-native extractor
both hard-code LoF classes (`HC` and `LC`), LoF-specific aggregation fields,
and LoF-specific output names. The transcript annotation output already
contains VEP `consequence` values, including values such as
`splice_acceptor_variant` and `splice_donor_variant`.

The goal is to preserve the existing LoF outputs while making the extraction
machinery reusable for additional VEP annotation groups. Each additional
group should produce its own sample-gene carrier table rather than widening the
LoF table with unrelated columns.

## Decision

Use a configuration-driven annotation-group extractor shared conceptually by
the standalone and Hail-native workflows.

The initial groups are:

| Group | Source field | Matching values | Output |
|---|---|---|---|
| LoF | `LoF` | `HC`, `LC` | Existing HC and HC-or-LC tables |
| splice acceptor | `consequence` | `splice_acceptor_variant` | Separate splice-acceptor table |
| splice donor | `consequence` | `splice_donor_variant` | Separate splice-donor table |

The implementation will define groups by stable names, source annotation
column, accepted values, and output annotation column. Adding a future group
should require adding configuration rather than writing another extraction
algorithm.

## Output contract

Existing LoF outputs remain unchanged:

- `<output_prefix>.lof_carriers.HC.tsv.gz`
- `<output_prefix>.lof_carriers.HC_or_LC.tsv.gz`

They retain their current columns and semantics:

```text
sample_id
gene_id
gene_symbol
has_lof_variant
n_lof_variants
variant_ids
lof_classes
```

Each non-LoF group produces a separate file with a group-specific presence
and count field and a generic annotation-value field. For example:

```text
sample_id
gene_id
gene_symbol
has_splice_acceptor_variant
n_splice_acceptor_variants
variant_ids
consequences
```

The corresponding initial filenames are:

- `<output_prefix>.splice_acceptor_carriers.tsv.gz`
- `<output_prefix>.splice_donor_carriers.tsv.gz`

Rows are sparse: a row exists only when the sample has at least one
non-reference genotype for the group and gene. Variant IDs, gene symbols, and
annotation values are deduplicated and emitted in deterministic sorted order.

## Processing design

### Standalone VCF workflow

The transcript annotation TSV is read once and normalized into one map per
annotation group:

```text
variant -> gene -> {
    gene symbols,
    matching annotation values
}
```

The region file and variant map are built from the union of all configured
groups. The VCF is restricted to that union and scanned once. For each
non-reference sample genotype, the extractor updates the sample-gene record
for every matching group.

LoF records retain their existing HC-only and HC-or-LC dual aggregation. Other
groups use one aggregation per group and are exported independently.

An annotation group with no matching variants still writes an empty gzip TSV
with its required header.

### Hail-native workflow

The Hail extractor will normalize VAT rows once, retain rows matching any
configured group, and attach group-specific annotation arrays to filtered
MatrixTable rows. It will aggregate non-reference entries once into a shared
sample-gene-group carrier table. Each output table will be derived from that
shared aggregation rather than rescanning the MatrixTable.

The existing LoF VCF export and its flat LoF INFO fields remain unchanged.
Non-LoF group tables are carrier-table outputs only in this iteration; they do
not add new VCF INFO fields.

### WDL integration

`workflow/HailLoFCarrierTable.wdl` will expose the new group tables alongside
the existing LoF tables. `main.wdl` will propagate them as optional outputs
under the existing `MakeLoFCarriers` conditional. The existing
`LoFCarrierTable.wdl` utility will be updated to run the generalized
standalone extractor and expose the same additional tables.

The initial group configuration remains internal and version-controlled. The
data model will leave room for a future WDL input that selects or extends
groups, but this iteration will not add a user-facing configuration file or
input unless required by the existing workflow interface.

## Validation and failure behavior

- Every configured group must name a supported transcript annotation column.
- Missing required transcript columns fail early and list all missing columns.
- Missing or malformed variant coordinates are skipped consistently with the
  current LoF parser.
- A transcript row may match multiple groups and contributes to each matching
  output, but duplicate values within one output are removed.
- HC-only LoF output excludes LC-only variants; HC-or-LC includes both classes.
- Genotypes with any non-reference allele count are carriers, matching the
  current behavior.
- Empty groups produce valid empty tables with stable headers.

## Testing

Add regression coverage for:

1. Generic parsing of LoF, splice acceptor, and splice donor groups.
2. Separate output filenames and schemas for each non-LoF group.
3. One VCF scan producing carrier rows in multiple group outputs.
4. Empty-group output.
5. Existing LoF HC and HC-or-LC behavior, including HC-only filtering.
6. Hail source contracts for shared group aggregation and output paths.
7. WDL propagation through `HailLoFCarrierTable.wdl`, `LoFCarrierTable.wdl`,
   and `main.wdl`.
8. Python compilation and `miniwdl check` for changed workflows.

## Alternatives rejected

- **Separate hard-coded extractors per consequence:** rejected because every
  new VEP consequence would require another algorithm and another set of
  tests.
- **One wide table with columns for every consequence:** rejected because it
  couples unrelated annotation types and makes sparse downstream analyses
  harder to consume.
- **Replace the LoF tables with a generic schema:** rejected because it would
  break existing downstream consumers and is unnecessary for additive support.
