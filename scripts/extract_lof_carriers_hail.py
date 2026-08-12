import argparse

import hail as hl


LOF_CLASSES = ("HC", "LC")
REQUIRED_LOF_VAT_FIELDS = (
    "vid",
    "gene_id",
    "gene_symbol",
    "LoF",
)


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


def _prepare_lof_variant_gene_table(vat_hail_table):
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
        & _text_is_defined(vat_ht.LoF)
        & hl.literal(set(LOF_CLASSES)).contains(vat_ht.LoF)
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
        lof_gene=hl.struct(
            gene_id=vat_ht.gene_id,
            gene_symbol=_clean_str(vat_ht.gene_symbol),
            lof_class=vat_ht.LoF,
        ),
    )

    return vat_ht.group_by("locus", "alleles").aggregate(
        lof_genes=hl.agg.collect_as_set(vat_ht.lof_gene)
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


def _build_carrier_table(entries_ht, lof_classes):
    lof_class_literal = hl.literal(set(lof_classes))
    carrier_ht = entries_ht.filter(
        lof_class_literal.contains(entries_ht.lof_genes.lof_class)
    )
    carrier_ht = carrier_ht.group_by(
        sample_id=carrier_ht.s,
        gene_id=carrier_ht.lof_genes.gene_id,
    ).aggregate(
        gene_symbols=hl.agg.collect_as_set(carrier_ht.lof_genes.gene_symbol),
        variant_ids=hl.agg.collect_as_set(carrier_ht.variant_id),
        lof_classes=hl.agg.collect_as_set(carrier_ht.lof_genes.lof_class),
    )
    carrier_ht = carrier_ht.annotate(
        gene_symbol=hl.delimit(hl.sorted(hl.array(carrier_ht.gene_symbols)), ","),
        has_lof_variant="true",
        n_lof_variants=hl.len(carrier_ht.variant_ids),
        variant_ids=hl.delimit(hl.sorted(hl.array(carrier_ht.variant_ids)), ","),
        lof_classes=hl.delimit(hl.sorted(hl.array(carrier_ht.lof_classes)), ","),
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
    samples_ht = hl.import_table(args.SampleList, key="research_id")
    lof_ht = _prepare_lof_variant_gene_table(args.VATHailTable)

    mt = _filter_matrix_table(mt, samples_ht, args)
    mt = mt.annotate_rows(lof_genes=lof_ht[mt.row_key].lof_genes)
    mt = mt.filter_rows(hl.is_defined(mt.lof_genes))
    mt = mt.select_rows("lof_genes").select_cols().select_entries("GT")
    mt = mt.explode_rows(mt.lof_genes)
    mt = mt.annotate_rows(variant_id=_variant_id(mt))

    entries_ht = mt.entries()
    entries_ht = entries_ht.filter(
        hl.is_defined(entries_ht.GT) & entries_ht.GT.is_non_ref()
    )

    hc_output = _join_cloud_path(
        args.OutputBucket, f"{args.OutputPrefix}.lof_carriers.HC.tsv.bgz"
    )
    hc_or_lc_output = _join_cloud_path(
        args.OutputBucket, f"{args.OutputPrefix}.lof_carriers.HC_or_LC.tsv.bgz"
    )

    _build_carrier_table(entries_ht, ("HC",)).export(hc_output)
    _build_carrier_table(entries_ht, LOF_CLASSES).export(hc_or_lc_output)

    with open("lof_carriers_hc_outpath.txt", "w") as output_path_file:
        output_path_file.write(hc_output)
    with open("lof_carriers_hc_or_lc_outpath.txt", "w") as output_path_file:
        output_path_file.write(hc_or_lc_output)

    hl.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create Hail-native long-format sample-gene LoF carrier tables."
    )
    parser.add_argument("--MatrixTable", required=True, help="Path to input MatrixTable.")
    parser.add_argument("--SampleList", required=True, help="Path to samples TSV file.")
    parser.add_argument(
        "--VATHailTable",
        required=True,
        help="Path to VAT Hail table containing transcript-level LoF annotations.",
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
