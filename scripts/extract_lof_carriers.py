import argparse
import csv
import gzip
import re
from collections import defaultdict
from dataclasses import dataclass


MISSING_VALUES = {"", ".", "NA", "NaN", "nan", "None"}
LOF_CLASSES = ("HC", "LC")


@dataclass(frozen=True)
class AnnotationGroup:
    name: str
    source_column: str
    matching_values: tuple[str, ...]
    annotation_header: str
    has_header: str
    count_header: str
    output_suffix: str


LOF_GROUP = AnnotationGroup(
    name="lof",
    source_column="LoF",
    matching_values=("HC", "LC"),
    annotation_header="lof_classes",
    has_header="has_lof_variant",
    count_header="n_lof_variants",
    output_suffix="lof_carriers",
)
ANNOTATION_GROUPS = (
    AnnotationGroup(
        name="splice_acceptor",
        source_column="consequence",
        matching_values=("splice_acceptor_variant",),
        annotation_header="consequences",
        has_header="has_splice_acceptor_variant",
        count_header="n_splice_acceptor_variants",
        output_suffix="splice_acceptor_carriers.tsv.gz",
    ),
    AnnotationGroup(
        name="splice_donor",
        source_column="consequence",
        matching_values=("splice_donor_variant",),
        annotation_header="consequences",
        has_header="has_splice_donor_variant",
        count_header="n_splice_donor_variants",
        output_suffix="splice_donor_carriers.tsv.gz",
    ),
)
ALL_GROUPS = (LOF_GROUP,) + ANNOTATION_GROUPS
OUTPUT_HEADER = (
    "sample_id",
    "gene_id",
    "gene_symbol",
    LOF_GROUP.has_header,
    LOF_GROUP.count_header,
    "variant_ids",
    LOF_GROUP.annotation_header,
)
VARIANT_MAP_HEADER = (
    "chrom",
    "pos",
    "ref",
    "alt",
    "group",
    "gene_id",
    "gene_symbol",
    "annotation",
)


def _open_text(path):
    if path.endswith((".gz", ".bgz")):
        return gzip.open(path, "rt", newline="")
    return open(path, "rt", newline="")


def _clean_value(value):
    value = "" if value is None else value.strip()
    return "" if value in MISSING_VALUES else value


def _variant_id(chrom, pos, ref, alt):
    return f"{chrom}:{pos}:{ref}:{alt}"


def _sort_lof_classes(classes):
    rank = {lof_class: index for index, lof_class in enumerate(LOF_CLASSES)}
    return sorted(classes, key=lambda lof_class: rank.get(lof_class, len(rank)))


def _empty_group_entry():
    return {"gene_symbols": set(), "annotations": set()}


def _matches_group_value(group, source_value):
    if group.source_column == "LoF":
        normalized = source_value.upper()
        return {
            matching_value.upper()
            for matching_value in group.matching_values
            if matching_value.upper() == normalized
        }

    return {
        matching_value
        for matching_value in group.matching_values
        if source_value == matching_value
    }


def parse_grouped_transcript_annotations(transcript_annotations_tsv):
    required_columns = {"chrom", "pos", "ref", "alt", "gene_id", "gene_symbol"} | {
        group.source_column for group in ALL_GROUPS
    }
    variant_groups = defaultdict(
        lambda: defaultdict(lambda: defaultdict(_empty_group_entry))
    )

    with _open_text(transcript_annotations_tsv) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        missing_columns = required_columns - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                "Transcript annotation TSV is missing required columns: "
                + ", ".join(sorted(missing_columns))
            )

        for row in reader:
            gene_id = _clean_value(row.get("gene_id"))
            if not gene_id:
                continue

            chrom = _clean_value(row.get("chrom"))
            pos = _clean_value(row.get("pos"))
            ref = _clean_value(row.get("ref"))
            alt = _clean_value(row.get("alt"))
            if not all((chrom, pos, ref, alt)):
                continue

            gene_symbol = _clean_value(row.get("gene_symbol")) or "."
            variant_key = (chrom, pos, ref, alt)
            for group in ALL_GROUPS:
                source_value = _clean_value(row.get(group.source_column))
                if not source_value:
                    continue

                matching_values = _matches_group_value(group, source_value)
                if not matching_values:
                    continue

                gene_entry = variant_groups[variant_key][group.name][gene_id]
                gene_entry["gene_symbols"].add(gene_symbol)
                gene_entry["annotations"].update(matching_values)

    return variant_groups


def parse_transcript_annotations(transcript_annotations_tsv):
    required_columns = {"chrom", "pos", "ref", "alt", "gene_id", "gene_symbol", "LoF"}
    variant_genes = defaultdict(
        lambda: defaultdict(lambda: {"gene_symbols": set(), "lof_classes": set()})
    )

    with _open_text(transcript_annotations_tsv) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        missing_columns = required_columns - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                "Transcript annotation TSV is missing required columns: "
                + ", ".join(sorted(missing_columns))
            )

        for row in reader:
            lof_class = _clean_value(row.get("LoF")).upper()
            if lof_class not in LOF_CLASSES:
                continue

            gene_id = _clean_value(row.get("gene_id"))
            if not gene_id:
                continue

            chrom = _clean_value(row.get("chrom"))
            pos = _clean_value(row.get("pos"))
            ref = _clean_value(row.get("ref"))
            alt = _clean_value(row.get("alt"))
            if not all((chrom, pos, ref, alt)):
                continue

            gene_symbol = _clean_value(row.get("gene_symbol")) or "."
            gene_entry = variant_genes[(chrom, pos, ref, alt)][gene_id]
            gene_entry["gene_symbols"].add(gene_symbol)
            gene_entry["lof_classes"].add(lof_class)

    return variant_genes


def _iter_variant_map_rows(variant_groups):
    for (chrom, pos, ref, alt), groups in sorted(variant_groups.items()):
        for group_name, genes in sorted(groups.items()):
            for gene_id, gene_entry in sorted(genes.items()):
                yield {
                    "chrom": chrom,
                    "pos": pos,
                    "ref": ref,
                    "alt": alt,
                    "group": group_name,
                    "gene_id": gene_id,
                    "gene_symbol": ",".join(sorted(gene_entry["gene_symbols"])),
                    "annotation": ",".join(sorted(gene_entry["annotations"])),
                }


def write_variant_map(variant_groups, variant_map_path):
    with open(variant_map_path, "wt", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            delimiter="\t",
            fieldnames=VARIANT_MAP_HEADER,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(_iter_variant_map_rows(variant_groups))


def write_regions(variant_groups, regions_path):
    seen_regions = set()
    with open(regions_path, "wt", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        for chrom, pos, _, _ in sorted(variant_groups):
            region = (chrom, pos, pos)
            if region in seen_regions:
                continue
            seen_regions.add(region)
            writer.writerow(region)


def read_variant_map(variant_map_path):
    variant_groups = defaultdict(
        lambda: defaultdict(lambda: defaultdict(_empty_group_entry))
    )
    with _open_text(variant_map_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        missing_columns = set(VARIANT_MAP_HEADER) - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                "Variant map is missing required columns: "
                + ", ".join(sorted(missing_columns))
            )

        for row in reader:
            chrom = _clean_value(row.get("chrom"))
            pos = _clean_value(row.get("pos"))
            ref = _clean_value(row.get("ref"))
            alt = _clean_value(row.get("alt"))
            group = _clean_value(row.get("group"))
            gene_id = _clean_value(row.get("gene_id"))
            annotation = _clean_value(row.get("annotation"))
            if not all((chrom, pos, ref, alt, group, gene_id, annotation)):
                continue

            gene_symbol = _clean_value(row.get("gene_symbol")) or "."
            gene_entry = variant_groups[group][(chrom, pos, ref, alt)][gene_id]
            gene_entry["gene_symbols"].update(
                symbol for symbol in gene_symbol.split(",") if symbol
            )
            gene_entry["annotations"].update(
                value for value in annotation.split(",") if value
            )

    return variant_groups


def _is_non_ref_gt(gt):
    gt = _clean_value(gt)
    if not gt:
        return False

    for allele in re.split(r"[/|]", gt):
        if allele in MISSING_VALUES or allele == "0":
            continue
        try:
            if int(allele) > 0:
                return True
        except ValueError:
            return True
    return False


def _add_lof_record(records, sample_id, gene_id, gene_entry, variant_id, lof_classes):
    record = records[(sample_id, gene_id)]
    record["gene_symbols"].update(gene_entry["gene_symbols"])
    record["variant_ids"].add(variant_id)
    record["lof_classes"].update(lof_classes)


def _add_group_record(records, sample_id, gene_id, gene_entry, variant_id, annotations):
    record = records[(sample_id, gene_id)]
    record["gene_symbols"].update(gene_entry["gene_symbols"])
    record["variant_ids"].add(variant_id)
    record["annotations"].update(annotations)


def collect_carriers(vcf_file, variant_groups):
    hc_records = defaultdict(
        lambda: {"gene_symbols": set(), "variant_ids": set(), "lof_classes": set()}
    )
    hc_or_lc_records = defaultdict(
        lambda: {"gene_symbols": set(), "variant_ids": set(), "lof_classes": set()}
    )
    group_records = {
        group.name: defaultdict(
            lambda: {"gene_symbols": set(), "variant_ids": set(), "annotations": set()}
        )
        for group in ANNOTATION_GROUPS
    }
    sample_ids = []

    with _open_text(vcf_file) as handle:
        for raw_line in handle:
            if raw_line.startswith("##"):
                continue
            if raw_line.startswith("#CHROM"):
                fields = raw_line.rstrip("\n").split("\t")
                sample_ids = fields[9:]
                continue
            if raw_line.startswith("#"):
                continue

            fields = raw_line.rstrip("\n").split("\t")
            if len(fields) < 10:
                continue

            chrom, pos, _, ref, alt = fields[:5]
            key = (chrom, pos, ref, alt)
            genes_for_variant_by_group = {
                group_name: variant_groups.get(group_name, {}).get(key)
                for group_name in variant_groups
            }
            if not any(genes_for_variant_by_group.values()):
                continue

            format_fields = fields[8].split(":")
            try:
                gt_index = format_fields.index("GT")
            except ValueError:
                continue

            variant_id = _variant_id(chrom, pos, ref, alt)
            for sample_id, sample_value in zip(sample_ids, fields[9:]):
                sample_fields = sample_value.split(":")
                if gt_index >= len(sample_fields) or not _is_non_ref_gt(sample_fields[gt_index]):
                    continue

                lof_genes_for_variant = genes_for_variant_by_group.get(LOF_GROUP.name) or {}
                for gene_id, gene_entry in lof_genes_for_variant.items():
                    lof_classes = gene_entry["annotations"]
                    if "HC" in lof_classes:
                        _add_lof_record(
                            hc_records, sample_id, gene_id, gene_entry, variant_id, {"HC"}
                        )
                    hc_or_lc_classes = set(LOF_CLASSES) & lof_classes
                    if hc_or_lc_classes:
                        _add_lof_record(
                            hc_or_lc_records,
                            sample_id,
                            gene_id,
                            gene_entry,
                            variant_id,
                            hc_or_lc_classes,
                        )

                for group in ANNOTATION_GROUPS:
                    genes_for_variant = genes_for_variant_by_group.get(group.name) or {}
                    for gene_id, gene_entry in genes_for_variant.items():
                        _add_group_record(
                            group_records[group.name],
                            sample_id,
                            gene_id,
                            gene_entry,
                            variant_id,
                            gene_entry["annotations"],
                        )

    return hc_records, hc_or_lc_records, group_records


def write_lof_records(records, output_path):
    with gzip.open(output_path, "wt", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(OUTPUT_HEADER)
        for (sample_id, gene_id), record in sorted(records.items()):
            variant_ids = sorted(record["variant_ids"])
            writer.writerow(
                [
                    sample_id,
                    gene_id,
                    ",".join(sorted(record["gene_symbols"])),
                    "true",
                    len(variant_ids),
                    ",".join(variant_ids),
                    ",".join(_sort_lof_classes(record["lof_classes"])),
                ]
            )


def write_group_records(records, group, output_path):
    with gzip.open(output_path, "wt", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "sample_id",
                "gene_id",
                "gene_symbol",
                group.has_header,
                group.count_header,
                "variant_ids",
                group.annotation_header,
            ]
        )
        for (sample_id, gene_id), record in sorted(records.items()):
            variant_ids = sorted(record["variant_ids"])
            writer.writerow(
                [
                    sample_id,
                    gene_id,
                    ",".join(sorted(record["gene_symbols"])),
                    "true",
                    len(variant_ids),
                    ",".join(variant_ids),
                    ",".join(sorted(record["annotations"])),
                ]
            )


def write_sites_main(args):
    variant_groups = parse_grouped_transcript_annotations(args.TranscriptAnnotations)
    write_regions(variant_groups, args.Regions)
    write_variant_map(variant_groups, args.VariantMap)

    print(f"Annotated variants: {len(variant_groups)}")
    print(
        f"Annotated variant-gene mappings: {sum(len(genes) for groups in variant_groups.values() for genes in groups.values())}"
    )


def collect_carriers_main(args):
    variant_groups = read_variant_map(args.VariantMap)
    hc_records, hc_or_lc_records, group_records = collect_carriers(args.VCF, variant_groups)

    hc_output = f"{args.OutputPrefix}.lof_carriers.HC.tsv.gz"
    hc_or_lc_output = f"{args.OutputPrefix}.lof_carriers.HC_or_LC.tsv.gz"
    write_lof_records(hc_records, hc_output)
    write_lof_records(hc_or_lc_records, hc_or_lc_output)
    for group in ANNOTATION_GROUPS:
        write_group_records(
            group_records[group.name],
            group,
            f"{args.OutputPrefix}.{group.output_suffix}",
        )

    print(
        f"Annotated variant-gene mappings: {sum(len(genes) for groups in variant_groups.values() for genes in groups.values())}"
    )
    print(f"HC carrier gene rows: {len(hc_records)}")
    print(f"HC_or_LC carrier gene rows: {len(hc_or_lc_records)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create long-format sample-gene LoF carrier tables from a VCF and transcript annotations TSV."
    )
    subparsers = parser.add_subparsers(dest="command")

    write_sites_parser = subparsers.add_parser(
        "write-sites",
        help="Write a LoF region file and variant-gene map from transcript annotations.",
    )
    write_sites_parser.add_argument(
        "--TranscriptAnnotations",
        required=True,
        help="Transcript annotation TSV/BGZ emitted by filter_and_write_mt.py.",
    )
    write_sites_parser.add_argument(
        "--Regions",
        required=True,
        help="Output tab-delimited CHROM/POS/END regions file for bcftools -R.",
    )
    write_sites_parser.add_argument(
        "--VariantMap",
        required=True,
        help="Output LoF variant-gene map TSV.",
    )
    write_sites_parser.set_defaults(func=write_sites_main)

    collect_parser = subparsers.add_parser(
        "collect-carriers",
        help="Collect sample-gene carriers from a VCF subset.",
    )
    collect_parser.add_argument(
        "--VCF", required=True, help="LoF-only VCF/VCF.GZ/VCF.BGZ with sample GT fields."
    )
    collect_parser.add_argument(
        "--VariantMap",
        required=True,
        help="LoF variant-gene map TSV emitted by write-sites.",
    )
    collect_parser.add_argument(
        "--OutputPrefix", required=True, help="Output filename prefix."
    )
    collect_parser.set_defaults(func=collect_carriers_main)

    parsed_args = parser.parse_args()
    if not hasattr(parsed_args, "func"):
        parser.error("a command is required: write-sites or collect-carriers")
    parsed_args.func(parsed_args)
