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
