import argparse
from dataclasses import dataclass

import hail as hl


LOF_CLASSES = ("HC", "LC")
REQUIRED_LOF_VAT_FIELDS = (
    "vid",
    "gene_id",
    "gene_symbol",
    "LoF",
    "consequence",
)


@dataclass(frozen=True)
class AnnotationGroup:
    name: str
    source_field: str
    matching_values: tuple[str, ...]
    annotation_header: str
    has_header: str
    count_header: str
    output_filename: str


LOF_GROUP = AnnotationGroup(
    name="lof",
    source_field="LoF",
    matching_values=LOF_CLASSES,
    annotation_header="lof_classes",
    has_header="has_lof_variant",
    count_header="n_lof_variants",
    output_filename="lof_carriers",
)
ANNOTATION_GROUPS = (
    AnnotationGroup(
        name="splice_acceptor",
        source_field="consequence",
        matching_values=("splice_acceptor_variant",),
        annotation_header="consequences",
        has_header="has_splice_acceptor_variant",
        count_header="n_splice_acceptor_variants",
        output_filename="splice_acceptor_carriers.tsv.bgz",
    ),
    AnnotationGroup(
        name="splice_donor",
        source_field="consequence",
        matching_values=("splice_donor_variant",),
        annotation_header="consequences",
        has_header="has_splice_donor_variant",
        count_header="n_splice_donor_variants",
        output_filename="splice_donor_carriers.tsv.bgz",
    ),
)
ALL_GROUPS = (LOF_GROUP,) + ANNOTATION_GROUPS


def _positive_int(value):
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer.")
    return parsed


def _spark_local_threads(value):
    if value == "*":
        return value
    return str(_positive_int(value))


def _join_cloud_path(parent, child):
    return f"{parent.rstrip('/')}/{child.lstrip('/')}"


def _missing_lof_vat_fields(available_fields):
    return sorted(set(REQUIRED_LOF_VAT_FIELDS) - set(available_fields))


def _text_is_defined(expr):
    return (
        hl.is_defined(expr)
        & (expr != "")
        & (expr != ".")
        & (expr != "NA")
    )


def _clean_str(expr, default="."):
    return hl.or_else(hl.or_missing(_text_is_defined(expr), expr), default)


def _group_matches(vat_ht, group):
    if group.name == LOF_GROUP.name:
        return _text_is_defined(vat_ht.LoF) & hl.literal(set(LOF_CLASSES)).contains(vat_ht.LoF)

    return _text_is_defined(vat_ht[group.source_field]) & hl.literal(
        set(group.matching_values)
    ).contains(vat_ht[group.source_field])


def _prepare_variant_annotation_table(vat_hail_table):
    vat_ht = hl.read_table(vat_hail_table)
    missing_fields = _missing_lof_vat_fields(set(vat_ht.row.dtype))
    if missing_fields:
        raise ValueError(
            "VAT Hail Table is missing required LoF carrier fields: "
            + ", ".join(missing_fields)
        )

    vat_ht = vat_ht.key_by()
    vat_ht = vat_ht.filter(
        _text_is_defined(vat_ht.vid)
        & _text_is_defined(vat_ht.gene_id)
    )

    vat_ht = vat_ht.annotate(_parts=vat_ht.vid.split("-", 4))
    vat_ht = vat_ht.filter(hl.len(vat_ht._parts) >= 4)
    vat_ht = vat_ht.annotate(
        locus=hl.locus(
            hl.if_else(
                vat_ht._parts[0].startswith("chr"),
                vat_ht._parts[0],
                "chr" + vat_ht._parts[0],
            ),
            hl.int32(vat_ht._parts[1]),
            reference_genome="GRCh38",
        ),
        alleles=[vat_ht._parts[2], vat_ht._parts[3]],
        gene_symbol=_clean_str(vat_ht.gene_symbol),
    )

    group_tables = []
    for group in ALL_GROUPS:
        source_expr = vat_ht[group.source_field]
        group_ht = vat_ht.filter(_group_matches(vat_ht, group)).select(
            "locus",
            "alleles",
            annotation=hl.struct(
                annotation_group=group.name,
                gene_id=vat_ht.gene_id,
                gene_symbol=vat_ht.gene_symbol,
                annotation_value=source_expr,
                lof_class=vat_ht.LoF if group.name == LOF_GROUP.name else hl.missing(hl.tstr),
            ),
        )
        group_tables.append(group_ht)

    annotation_ht = group_tables[0]
    if len(group_tables) > 1:
        annotation_ht = annotation_ht.union(*group_tables[1:])

    return annotation_ht.group_by("locus", "alleles").aggregate(
        annotations=hl.array(hl.agg.collect_as_set(annotation_ht.annotation))
    )


def _apply_bed_filter(mt, bed_file):
    bed = hl.import_table(
        bed_file,
        delimiter="\t",
        no_header=True,
        types={"f1": hl.tint32, "f2": hl.tint32},
    )
    bed = bed.rename({"f0": "contig", "f1": "start", "f2": "end"})

    rg = hl.get_reference("GRCh38")
    lengths = hl.literal(rg.lengths)

    bed = bed.annotate(
        start=hl.max(0, bed.start),
        end=hl.min(bed.end, lengths.get(bed.contig)),
    )
    bed = bed.filter(hl.is_defined(lengths.get(bed.contig)) & (bed.start < bed.end))

    regions = bed.annotate(
        interval=hl.interval(
            hl.locus(bed.contig, bed.start + 1, reference_genome="GRCh38"),
            hl.locus(bed.contig, bed.end, reference_genome="GRCh38"),
            includes_start=True,
            includes_end=False,
        )
    ).key_by("interval")

    return mt.filter_rows(hl.is_defined(regions[mt.locus]))


def _filter_matrix_table(mt, samples_ht, args):
    if args.BedFile:
        mt = _apply_bed_filter(mt, args.BedFile)

    mt = mt.filter_cols(hl.is_defined(samples_ht[mt.s]))
    mt = mt.filter_rows(hl.len(mt.alleles) == 2)
    mt = mt.filter_rows(hl.agg.any(hl.is_defined(mt.GT)))
    mt = mt.filter_rows(~hl.is_missing(mt.info.AC))
    mt = mt.filter_rows(hl.is_missing(mt.filters))
    mt = mt.annotate_rows(info=hl.agg.call_stats(mt.GT, mt.alleles))
    mt = mt.filter_rows(
        mt.info.AN
        >= int(args.AlleleNumberPercentage) / 100 * mt.count_cols() * 2
    )
    mt = mt.filter_rows(
        (hl.min(mt.info.AC) >= int(args.MinAlleleCount))
        & (hl.min(mt.info.AC) <= int(args.MaxAlleleCount))
    )
    return mt


def _variant_id(ht):
    return (
        ht.locus.contig
        + ":"
        + hl.str(ht.locus.position)
        + ":"
        + ht.alleles[0]
        + ":"
        + ht.alleles[1]
    )


def _sorted_distinct(expr):
    return hl.sorted(hl.array(hl.set(expr)))


def _annotate_lof_vcf_fields(mt):
    return mt.annotate_rows(
        info=hl.struct(
            LOF_GENE_ID=_sorted_distinct(
                mt.lof_annotations.map(lambda annotation: annotation.gene_id)
            ),
            LOF_GENE_SYMBOL=_sorted_distinct(
                mt.lof_annotations.map(lambda annotation: annotation.gene_symbol)
            ),
            LOF_CLASS=_sorted_distinct(
                mt.lof_annotations.map(lambda annotation: annotation.lof_class)
            ),
        )
    )


def _aggregate_carrier_table(entries_ht):
    carrier_ht = entries_ht.group_by(
        sample_id=entries_ht.s,
        annotation_group=entries_ht.annotations.annotation_group,
        gene_id=entries_ht.annotations.gene_id,
    ).aggregate(
        all_gene_symbols=hl.agg.collect_as_set(entries_ht.annotations.gene_symbol),
        all_variant_ids=hl.agg.collect_as_set(entries_ht.variant_id),
        all_annotation_values=hl.agg.collect_as_set(
            entries_ht.annotations.annotation_value
        ),
        hc_gene_symbols=hl.agg.filter(
            hl.is_defined(entries_ht.annotations.lof_class)
            & (entries_ht.annotations.lof_class == "HC"),
            hl.agg.collect_as_set(entries_ht.annotations.gene_symbol),
        ),
        hc_variant_ids=hl.agg.filter(
            hl.is_defined(entries_ht.annotations.lof_class)
            & (entries_ht.annotations.lof_class == "HC"),
            hl.agg.collect_as_set(entries_ht.variant_id),
        ),
        hc_annotation_values=hl.agg.filter(
            hl.is_defined(entries_ht.annotations.lof_class)
            & (entries_ht.annotations.lof_class == "HC"),
            hl.agg.collect_as_set(entries_ht.annotations.annotation_value),
        ),
    )

    return carrier_ht


def _format_lof_carrier_table(carrier_ht, hc_only):
    carrier_ht = carrier_ht.filter(carrier_ht.annotation_group == LOF_GROUP.name)
    if hc_only:
        carrier_ht = carrier_ht.filter(hl.len(carrier_ht.hc_variant_ids) > 0)
        carrier_ht = carrier_ht.annotate(
            gene_symbol=hl.delimit(
                _sorted_distinct(carrier_ht.hc_gene_symbols), ","
            ),
            has_lof_variant="true",
            n_lof_variants=hl.len(carrier_ht.hc_variant_ids),
            variant_ids=hl.delimit(
                _sorted_distinct(carrier_ht.hc_variant_ids), ","
            ),
            lof_classes=hl.delimit(
                _sorted_distinct(carrier_ht.hc_annotation_values), ","
            ),
        )
    else:
        carrier_ht = carrier_ht.annotate(
            gene_symbol=hl.delimit(
                _sorted_distinct(carrier_ht.all_gene_symbols), ","
            ),
            has_lof_variant="true",
            n_lof_variants=hl.len(carrier_ht.all_variant_ids),
            variant_ids=hl.delimit(
                _sorted_distinct(carrier_ht.all_variant_ids), ","
            ),
            lof_classes=hl.delimit(
                _sorted_distinct(carrier_ht.all_annotation_values), ","
            ),
        )
    carrier_ht = carrier_ht.key_by()
    return carrier_ht.select(
        "sample_id",
        "gene_id",
        "gene_symbol",
        "has_lof_variant",
        "n_lof_variants",
        "variant_ids",
        "lof_classes",
    )


def _format_group_carrier_table(carrier_ht, group):
    carrier_ht = carrier_ht.filter(carrier_ht.annotation_group == group.name)
    carrier_ht = carrier_ht.annotate(
        gene_symbol=hl.delimit(_sorted_distinct(carrier_ht.all_gene_symbols), ","),
        variant_ids=hl.delimit(_sorted_distinct(carrier_ht.all_variant_ids), ","),
        **{
            group.has_header: "true",
            group.count_header: hl.len(carrier_ht.all_variant_ids),
            group.annotation_header: hl.delimit(
                _sorted_distinct(carrier_ht.all_annotation_values), ","
            ),
        },
    )
    carrier_ht = carrier_ht.key_by()
    return carrier_ht.select(
        "sample_id",
        "gene_id",
        "gene_symbol",
        group.has_header,
        group.count_header,
        "variant_ids",
        group.annotation_header,
    )


def main(args):
    hl.init(
        app_name="hail_lof_carrier_table",
        master=f"local[{args.SparkLocalThreads}]",
        tmp_dir=args.CloudTmpdir,
        spark_conf={
            "spark.local.dir": "/cromwell_root",
            "spark.driver.memory": args.SparkDriverMemory,
            "spark.sql.shuffle.partitions": str(args.SparkShufflePartitions),
            "spark.default.parallelism": str(args.SparkParallelism),
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.2",
        },
    )
    hl.default_reference("GRCh38")

    mt = hl.read_matrix_table(args.MatrixTable)
    annotation_ht = _prepare_variant_annotation_table(args.VATHailTable)

    if not args.MatrixTableAlreadyFiltered:
        samples_ht = hl.import_table(args.SampleList, key="research_id")
        mt = _filter_matrix_table(mt, samples_ht, args)

    mt = mt.annotate_rows(annotations=annotation_ht[mt.row_key].annotations)
    mt = mt.filter_rows(hl.is_defined(mt.annotations))
    mt = mt.annotate_rows(
        lof_annotations=mt.annotations.filter(
            lambda annotation: annotation.annotation_group == LOF_GROUP.name
        )
    )
    mt = mt.select_rows("annotations", "lof_annotations").select_cols().select_entries("GT")

    mt = _annotate_lof_vcf_fields(mt)
    lof_intermediate_path = _join_cloud_path(
        args.CloudTmpdir, f"{args.OutputPrefix}.lof_intermediate.mt"
    )
    mt.write(lof_intermediate_path, overwrite=True)
    lof_mt = hl.read_matrix_table(lof_intermediate_path)
    annotation_mt = lof_mt
    lof_mt = lof_mt.filter_rows(hl.len(lof_mt.lof_annotations) > 0)

    lof_vcf_output = _join_cloud_path(
        args.OutputBucket, f"{args.OutputPrefix}.lof_variants.vcf.bgz"
    )
    hl.export_vcf(lof_mt, lof_vcf_output)

    carrier_mt = annotation_mt.explode_rows(annotation_mt.annotations)
    carrier_mt = carrier_mt.annotate_rows(variant_id=_variant_id(carrier_mt))
    entries_ht = carrier_mt.entries()
    entries_ht = entries_ht.filter(
        hl.is_defined(entries_ht.GT) & entries_ht.GT.is_non_ref()
    )

    hc_output = _join_cloud_path(
        args.OutputBucket, f"{args.OutputPrefix}.lof_carriers.HC.tsv.bgz"
    )
    hc_or_lc_output = _join_cloud_path(
        args.OutputBucket, f"{args.OutputPrefix}.lof_carriers.HC_or_LC.tsv.bgz"
    )
    splice_acceptor_output = _join_cloud_path(
        args.OutputBucket, f"{args.OutputPrefix}.splice_acceptor_carriers.tsv.bgz"
    )
    splice_donor_output = _join_cloud_path(
        args.OutputBucket, f"{args.OutputPrefix}.splice_donor_carriers.tsv.bgz"
    )

    carrier_intermediate_path = _join_cloud_path(
        args.CloudTmpdir, f"{args.OutputPrefix}.lof_carriers_intermediate.ht"
    )
    carrier_ht = _aggregate_carrier_table(entries_ht)
    carrier_ht = carrier_ht.checkpoint(carrier_intermediate_path, overwrite=True)
    _format_lof_carrier_table(carrier_ht, hc_only=True).export(hc_output)
    _format_lof_carrier_table(carrier_ht, hc_only=False).export(hc_or_lc_output)
    _format_group_carrier_table(
        carrier_ht, ANNOTATION_GROUPS[0]
    ).export(splice_acceptor_output)
    _format_group_carrier_table(
        carrier_ht, ANNOTATION_GROUPS[1]
    ).export(splice_donor_output)

    with open("lof_variants_vcf_outpath.txt", "w") as output_path_file:
        output_path_file.write(lof_vcf_output)
    with open("lof_carriers_hc_outpath.txt", "w") as output_path_file:
        output_path_file.write(hc_output)
    with open("lof_carriers_hc_or_lc_outpath.txt", "w") as output_path_file:
        output_path_file.write(hc_or_lc_output)
    with open("splice_acceptor_carriers_outpath.txt", "w") as output_path_file:
        output_path_file.write(splice_acceptor_output)
    with open("splice_donor_carriers_outpath.txt", "w") as output_path_file:
        output_path_file.write(splice_donor_output)

    hl.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create Hail-native long-format sample-gene annotation carrier tables."
    )
    parser.add_argument("--MatrixTable", required=True, help="Path to input MatrixTable.")
    parser.add_argument("--SampleList", required=True, help="Path to samples TSV file.")
    parser.add_argument(
        "--VATHailTable",
        required=True,
        help="Path to VAT Hail table containing transcript-level annotation groups.",
    )
    parser.add_argument(
        "--BedFile",
        required=False,
        help="BED file containing regions of interest, typically cis windows for genes.",
    )
    parser.add_argument(
        "--MinAlleleCount", required=True, help="Min allele count threshold."
    )
    parser.add_argument(
        "--MaxAlleleCount", required=True, help="Max allele count threshold."
    )
    parser.add_argument(
        "--AlleleNumberPercentage",
        required=True,
        help="Allele number percentage cutoff.",
    )
    parser.add_argument("--OutputBucket", required=True, help="Path to output bucket.")
    parser.add_argument("--OutputPrefix", required=True, help="Output prefix.")
    parser.add_argument("--CloudTmpdir", required=True, help="Temporary cloud directory for Spark/Hail.")
    parser.add_argument(
        "--MatrixTableAlreadyFiltered",
        action="store_true",
        help="Skip sample, BED, and variant QC filters because the input is already filtered.",
    )
    parser.add_argument(
        "--SparkLocalThreads",
        type=_spark_local_threads,
        default="*",
        help="Spark local worker threads, or '*' for all visible CPUs.",
    )
    parser.add_argument(
        "--SparkDriverMemory", default="64g", help="Spark driver memory, for example 64g."
    )
    parser.add_argument(
        "--SparkParallelism",
        type=_positive_int,
        default=100,
        help="Spark default parallelism.",
    )
    parser.add_argument(
        "--SparkShufflePartitions",
        type=_positive_int,
        default=100,
        help="Spark SQL shuffle partitions.",
    )
    main(parser.parse_args())
