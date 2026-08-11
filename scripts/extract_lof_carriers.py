import argparse
import csv
import gzip
import re
from collections import defaultdict


MISSING_VALUES = {"", ".", "NA", "NaN", "nan", "None"}
LOF_CLASSES = ("HC", "LC")
OUTPUT_HEADER = (
    "sample_id",
    "gene_id",
    "gene_symbol",
    "has_lof_variant",
    "n_lof_variants",
    "variant_ids",
    "lof_classes",
)
VARIANT_MAP_HEADER = (
    "chrom",
    "pos",
    "ref",
    "alt",
    "gene_id",
    "gene_symbol",
    "lof_class",
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


def _iter_variant_map_rows(variant_genes):
    for (chrom, pos, ref, alt), genes in sorted(variant_genes.items()):
        for gene_id, gene_entry in sorted(genes.items()):
            gene_symbols = sorted(gene_entry["gene_symbols"])
            for lof_class in _sort_lof_classes(gene_entry["lof_classes"]):
                yield {
                    "chrom": chrom,
                    "pos": pos,
                    "ref": ref,
                    "alt": alt,
                    "gene_id": gene_id,
                    "gene_symbol": ",".join(gene_symbols),
                    "lof_class": lof_class,
                }


def write_variant_map(variant_genes, variant_map_path):
    with open(variant_map_path, "wt", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            delimiter="\t",
            fieldnames=VARIANT_MAP_HEADER,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(_iter_variant_map_rows(variant_genes))


def write_regions(variant_genes, regions_path):
    seen_regions = set()
    with open(regions_path, "wt", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        for chrom, pos, _, _ in sorted(variant_genes):
            region = (chrom, pos, pos)
            if region in seen_regions:
                continue
            seen_regions.add(region)
            writer.writerow(region)


def read_variant_map(variant_map_path):
    variant_genes = defaultdict(
        lambda: defaultdict(lambda: {"gene_symbols": set(), "lof_classes": set()})
    )
    with _open_text(variant_map_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        missing_columns = set(VARIANT_MAP_HEADER) - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                "LoF variant map is missing required columns: "
                + ", ".join(sorted(missing_columns))
            )

        for row in reader:
            chrom = _clean_value(row.get("chrom"))
            pos = _clean_value(row.get("pos"))
            ref = _clean_value(row.get("ref"))
            alt = _clean_value(row.get("alt"))
            gene_id = _clean_value(row.get("gene_id"))
            lof_class = _clean_value(row.get("lof_class")).upper()
            if not all((chrom, pos, ref, alt, gene_id)) or lof_class not in LOF_CLASSES:
                continue

            gene_symbol = _clean_value(row.get("gene_symbol")) or "."
            gene_entry = variant_genes[(chrom, pos, ref, alt)][gene_id]
            gene_entry["gene_symbols"].update(
                symbol for symbol in gene_symbol.split(",") if symbol
            )
            gene_entry["lof_classes"].add(lof_class)

    return variant_genes


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


def _add_record(records, sample_id, gene_id, gene_entry, variant_id, lof_classes):
    record = records[(sample_id, gene_id)]
    record["gene_symbols"].update(gene_entry["gene_symbols"])
    record["variant_ids"].add(variant_id)
    record["lof_classes"].update(lof_classes)


def collect_lof_carriers(vcf_file, variant_genes):
    hc_records = defaultdict(
        lambda: {"gene_symbols": set(), "variant_ids": set(), "lof_classes": set()}
    )
    hc_or_lc_records = defaultdict(
        lambda: {"gene_symbols": set(), "variant_ids": set(), "lof_classes": set()}
    )
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
            genes_for_variant = variant_genes.get(key)
            if not genes_for_variant:
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

                for gene_id, gene_entry in genes_for_variant.items():
                    lof_classes = gene_entry["lof_classes"]
                    if "HC" in lof_classes:
                        _add_record(
                            hc_records, sample_id, gene_id, gene_entry, variant_id, {"HC"}
                        )
                    hc_or_lc_classes = set(LOF_CLASSES) & lof_classes
                    if hc_or_lc_classes:
                        _add_record(
                            hc_or_lc_records,
                            sample_id,
                            gene_id,
                            gene_entry,
                            variant_id,
                            hc_or_lc_classes,
                        )

    return hc_records, hc_or_lc_records


def write_records(records, output_path):
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


def write_sites_main(args):
    variant_genes = parse_transcript_annotations(args.TranscriptAnnotations)
    write_regions(variant_genes, args.Regions)
    write_variant_map(variant_genes, args.VariantMap)

    print(f"LoF variants: {len(variant_genes)}")
    print(f"LoF variant-gene mappings: {sum(len(v) for v in variant_genes.values())}")


def collect_carriers_main(args):
    variant_genes = read_variant_map(args.VariantMap)
    hc_records, hc_or_lc_records = collect_lof_carriers(args.VCF, variant_genes)

    hc_output = f"{args.OutputPrefix}.lof_carriers.HC.tsv.gz"
    hc_or_lc_output = f"{args.OutputPrefix}.lof_carriers.HC_or_LC.tsv.gz"
    write_records(hc_records, hc_output)
    write_records(hc_or_lc_records, hc_or_lc_output)

    print(f"LoF variant-gene mappings: {sum(len(v) for v in variant_genes.values())}")
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
        help="Collect sample-gene LoF carriers from a LoF-only VCF subset.",
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
