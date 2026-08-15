import csv
import gzip
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "extract_lof_carriers.py"


def _read_gzip_tsv(path):
    with gzip.open(path, "rt", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


class ExtractLoFCarriersTests(unittest.TestCase):
    def test_writes_grouped_variant_map(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            transcript_path = temp_path / "transcript_annotations.tsv"
            regions_path = temp_path / "lof_regions.tsv"
            variant_map_path = temp_path / "lof_variant_gene_map.tsv"

            transcript_path.write_text(
                "\n".join(
                    [
                        "chrom\tpos\tref\talt\tgene_id\tgene_symbol\tLoF\tconsequence",
                        "chr1\t100\tA\tT\tENSG1\tGENE1\tHC\tstop_gained",
                        "chr1\t200\tG\tC\tENSG1\tGENE1\t.\tsplice_donor_variant",
                        "chr1\t300\tC\tA\tENSG2\tGENE2\t.\tsplice_acceptor_variant",
                        "chr1\t400\tT\tG\tENSG3\tGENE3\t.\tmissense_variant",
                    ]
                )
                + "\n"
            )

            subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "write-sites",
                    "--TranscriptAnnotations",
                    str(transcript_path),
                    "--Regions",
                    str(regions_path),
                    "--VariantMap",
                    str(variant_map_path),
                ],
                check=True,
                text=True,
                capture_output=True,
            )

            self.assertEqual(
                regions_path.read_text().splitlines(),
                ["chr1\t100\t100", "chr1\t200\t200", "chr1\t300\t300"],
            )

            with open(variant_map_path, "rt", newline="") as handle:
                rows = list(csv.DictReader(handle, delimiter="\t"))

        self.assertEqual(
            [row["group"] for row in rows],
            ["lof", "splice_donor", "splice_acceptor"],
        )
        self.assertEqual(
            rows,
            [
                {
                    "chrom": "chr1",
                    "pos": "100",
                    "ref": "A",
                    "alt": "T",
                    "group": "lof",
                    "gene_id": "ENSG1",
                    "gene_symbol": "GENE1",
                    "annotation": "HC",
                },
                {
                    "chrom": "chr1",
                    "pos": "200",
                    "ref": "G",
                    "alt": "C",
                    "group": "splice_donor",
                    "gene_id": "ENSG1",
                    "gene_symbol": "GENE1",
                    "annotation": "splice_donor_variant",
                },
                {
                    "chrom": "chr1",
                    "pos": "300",
                    "ref": "C",
                    "alt": "A",
                    "group": "splice_acceptor",
                    "gene_id": "ENSG2",
                    "gene_symbol": "GENE2",
                    "annotation": "splice_acceptor_variant",
                },
            ],
        )

    def test_writes_separate_group_carrier_tables(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            vcf_path = temp_path / "input.vcf"
            variant_map_path = temp_path / "lof_variant_gene_map.tsv"
            output_prefix = temp_path / "cohort"

            vcf_path.write_text(
                "\n".join(
                    [
                        "##fileformat=VCFv4.2",
                        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\tS3",
                        "chr1\t100\t.\tA\tT\t.\tPASS\t.\tGT\t0/1\t0/0\t0/0",
                        "chr1\t200\t.\tG\tC\t.\tPASS\t.\tGT\t0/0\t0/1\t0/0",
                        "chr1\t300\t.\tC\tA\t.\tPASS\t.\tGT\t0/1\t0/0\t0/0",
                    ]
                )
                + "\n"
            )
            variant_map_path.write_text(
                "\n".join(
                    [
                        "chrom\tpos\tref\talt\tgroup\tgene_id\tgene_symbol\tannotation",
                        "chr1\t100\tA\tT\tlof\tENSG1\tGENE1\tHC",
                        "chr1\t200\tG\tC\tsplice_donor\tENSG1\tGENE1\tsplice_donor_variant",
                        "chr1\t300\tC\tA\tsplice_acceptor\tENSG2\tGENE2\tsplice_acceptor_variant",
                    ]
                )
                + "\n"
            )

            subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "collect-carriers",
                    "--VCF",
                    str(vcf_path),
                    "--VariantMap",
                    str(variant_map_path),
                    "--OutputPrefix",
                    str(output_prefix),
                ],
                check=True,
                text=True,
                capture_output=True,
            )

            hc_rows = _read_gzip_tsv(temp_path / "cohort.lof_carriers.HC.tsv.gz")
            hc_or_lc_rows = _read_gzip_tsv(
                temp_path / "cohort.lof_carriers.HC_or_LC.tsv.gz"
            )
            splice_donor_rows = _read_gzip_tsv(
                temp_path / "cohort.splice_donor_carriers.tsv.gz"
            )
            splice_acceptor_rows = _read_gzip_tsv(
                temp_path / "cohort.splice_acceptor_carriers.tsv.gz"
            )

        self.assertEqual(
            hc_rows,
            [
                {
                    "sample_id": "S1",
                    "gene_id": "ENSG1",
                    "gene_symbol": "GENE1",
                    "has_lof_variant": "true",
                    "n_lof_variants": "1",
                    "variant_ids": "chr1:100:A:T",
                    "lof_classes": "HC",
                }
            ],
        )
        self.assertEqual(
            hc_or_lc_rows,
            [
                {
                    "sample_id": "S1",
                    "gene_id": "ENSG1",
                    "gene_symbol": "GENE1",
                    "has_lof_variant": "true",
                    "n_lof_variants": "1",
                    "variant_ids": "chr1:100:A:T",
                    "lof_classes": "HC",
                }
            ],
        )
        self.assertEqual(
            splice_donor_rows,
            [
                {
                    "sample_id": "S2",
                    "gene_id": "ENSG1",
                    "gene_symbol": "GENE1",
                    "has_splice_donor_variant": "true",
                    "n_splice_donor_variants": "1",
                    "variant_ids": "chr1:200:G:C",
                    "consequences": "splice_donor_variant",
                }
            ],
        )
        self.assertEqual(
            splice_acceptor_rows,
            [
                {
                    "sample_id": "S1",
                    "gene_id": "ENSG2",
                    "gene_symbol": "GENE2",
                    "has_splice_acceptor_variant": "true",
                    "n_splice_acceptor_variants": "1",
                    "variant_ids": "chr1:300:C:A",
                    "consequences": "splice_acceptor_variant",
                }
            ],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            vcf_path = temp_path / "input.vcf"
            variant_map_path = temp_path / "lof_variant_gene_map.tsv"
            output_prefix = temp_path / "cohort"

            vcf_path.write_text(
                "\n".join(
                    [
                        "##fileformat=VCFv4.2",
                        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2",
                        "chr1\t100\t.\tA\tT\t.\tPASS\t.\tGT\t0/1\t0/0",
                        "chr1\t200\t.\tG\tC\t.\tPASS\t.\tGT\t0/0\t0/1",
                    ]
                )
                + "\n"
            )
            variant_map_path.write_text(
                "\n".join(
                    [
                        "chrom\tpos\tref\talt\tgroup\tgene_id\tgene_symbol\tannotation",
                        "chr1\t100\tA\tT\tlof\tENSG1\tGENE1\tHC",
                        "chr1\t200\tG\tC\tsplice_donor\tENSG1\tGENE1\tsplice_donor_variant",
                    ]
                )
                + "\n"
            )

            subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "collect-carriers",
                    "--VCF",
                    str(vcf_path),
                    "--VariantMap",
                    str(variant_map_path),
                    "--OutputPrefix",
                    str(output_prefix),
                ],
                check=True,
                text=True,
                capture_output=True,
            )

            empty_acceptor_rows = _read_gzip_tsv(
                temp_path / "cohort.splice_acceptor_carriers.tsv.gz"
            )

        self.assertEqual(empty_acceptor_rows, [])

    def test_writes_hc_and_hc_or_lc_sample_gene_tables(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            vcf_path = temp_path / "input.vcf"
            transcript_path = temp_path / "transcript_annotations.tsv"
            output_prefix = temp_path / "cohort"

            vcf_path.write_text(
                "\n".join(
                    [
                        "##fileformat=VCFv4.2",
                        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\tS3",
                        "chr1\t100\t.\tA\tT\t.\tPASS\t.\tGT:DP\t0/1:10\t0/0:9\t./.:.",
                        "chr1\t200\t.\tG\tC\t.\tPASS\t.\tGT\t0/0\t1/1\t0/1",
                        "chr1\t300\t.\tC\tA\t.\tPASS\t.\tGT\t0/1\t0/1\t0/0",
                        "chr1\t400\t.\tT\tG\t.\tPASS\t.\tGT\t0/1\t0/0\t0/0",
                    ]
                )
                + "\n"
            )
            transcript_path.write_text(
                "\n".join(
                    [
                        "chrom\tpos\tref\talt\trsid\tgene_id\tgene_symbol\ttranscript\tis_canonical_transcript\tconsequence\taa_change\tLoF\tLoF_filter\tLoF_flags\tLoF_info\tgvs_max_af\tgvs_max_subpop",
                        "chr1\t100\tA\tT\t.\tENSG1\tGENE1\tENST1\ttrue\tstop_gained\tp.X\tHC\t.\t.\t.\t.\t.",
                        "chr1\t100\tA\tT\t.\tENSG1\tGENE1\tENST2\tfalse\tstop_gained\tp.X\tHC\t.\t.\t.\t.\t.",
                        "chr1\t200\tG\tC\t.\tENSG1\tGENE1\tENST3\ttrue\tsplice_donor_variant\t.\tLC\t.\t.\t.\t.\t.",
                        "chr1\t300\tC\tA\t.\tENSG2\tGENE2\tENST4\ttrue\tframeshift_variant\t.\tHC\t.\t.\t.\t.\t.",
                        "chr1\t400\tT\tG\t.\tENSG3\tGENE3\tENST5\ttrue\tmissense_variant\t.\tNA\t.\t.\t.\t.\t.",
                    ]
                )
                + "\n"
            )

            subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "write-sites",
                    "--TranscriptAnnotations",
                    str(transcript_path),
                    "--Regions",
                    str(temp_path / "lof_regions.tsv"),
                    "--VariantMap",
                    str(temp_path / "lof_variant_gene_map.tsv"),
                ],
                check=True,
                text=True,
                capture_output=True,
            )
            self.assertEqual(
                (temp_path / "lof_regions.tsv").read_text().splitlines(),
                ["chr1\t100\t100", "chr1\t200\t200", "chr1\t300\t300"],
            )

            subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "collect-carriers",
                    "--VCF",
                    str(vcf_path),
                    "--VariantMap",
                    str(temp_path / "lof_variant_gene_map.tsv"),
                    "--OutputPrefix",
                    str(output_prefix),
                ],
                check=True,
                text=True,
                capture_output=True,
            )

            hc_rows = _read_gzip_tsv(
                temp_path / "cohort.lof_carriers.HC.tsv.gz"
            )
            hc_or_lc_rows = _read_gzip_tsv(
                temp_path / "cohort.lof_carriers.HC_or_LC.tsv.gz"
            )

        self.assertEqual(
            hc_rows,
            [
                {
                    "sample_id": "S1",
                    "gene_id": "ENSG1",
                    "gene_symbol": "GENE1",
                    "has_lof_variant": "true",
                    "n_lof_variants": "1",
                    "variant_ids": "chr1:100:A:T",
                    "lof_classes": "HC",
                },
                {
                    "sample_id": "S1",
                    "gene_id": "ENSG2",
                    "gene_symbol": "GENE2",
                    "has_lof_variant": "true",
                    "n_lof_variants": "1",
                    "variant_ids": "chr1:300:C:A",
                    "lof_classes": "HC",
                },
                {
                    "sample_id": "S2",
                    "gene_id": "ENSG2",
                    "gene_symbol": "GENE2",
                    "has_lof_variant": "true",
                    "n_lof_variants": "1",
                    "variant_ids": "chr1:300:C:A",
                    "lof_classes": "HC",
                },
            ],
        )
        self.assertEqual(
            hc_or_lc_rows,
            [
                {
                    "sample_id": "S1",
                    "gene_id": "ENSG1",
                    "gene_symbol": "GENE1",
                    "has_lof_variant": "true",
                    "n_lof_variants": "1",
                    "variant_ids": "chr1:100:A:T",
                    "lof_classes": "HC",
                },
                {
                    "sample_id": "S1",
                    "gene_id": "ENSG2",
                    "gene_symbol": "GENE2",
                    "has_lof_variant": "true",
                    "n_lof_variants": "1",
                    "variant_ids": "chr1:300:C:A",
                    "lof_classes": "HC",
                },
                {
                    "sample_id": "S2",
                    "gene_id": "ENSG1",
                    "gene_symbol": "GENE1",
                    "has_lof_variant": "true",
                    "n_lof_variants": "1",
                    "variant_ids": "chr1:200:G:C",
                    "lof_classes": "LC",
                },
                {
                    "sample_id": "S2",
                    "gene_id": "ENSG2",
                    "gene_symbol": "GENE2",
                    "has_lof_variant": "true",
                    "n_lof_variants": "1",
                    "variant_ids": "chr1:300:C:A",
                    "lof_classes": "HC",
                },
                {
                    "sample_id": "S3",
                    "gene_id": "ENSG1",
                    "gene_symbol": "GENE1",
                    "has_lof_variant": "true",
                    "n_lof_variants": "1",
                    "variant_ids": "chr1:200:G:C",
                    "lof_classes": "LC",
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
