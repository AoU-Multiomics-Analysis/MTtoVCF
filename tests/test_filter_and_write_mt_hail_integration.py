import csv
import gzip
import importlib.util
from pathlib import Path
import tempfile
import unittest

try:
    import hail as hl
except ModuleNotFoundError:  # The real integration test runs in the pinned Hail image.
    hl = None
if hl is not None and not hasattr(hl, "init"):
    hl = None


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_COLUMNS = [
    "chrom",
    "pos",
    "ref",
    "alt",
    "rsid",
    "gene_id",
    "gene_symbol",
    "transcript",
    "is_canonical_transcript",
    "consequence",
    "aa_change",
    "LoF",
    "LoF_filter",
    "LoF_flags",
    "LoF_info",
    "gvs_max_af",
    "gvs_max_subpop",
]


def _table_order_by_nodes(ir):
    nodes = []
    if type(ir).__name__ == "TableOrderBy":
        nodes.append(ir)
    for child in getattr(ir, "children", ()):
        nodes.extend(_table_order_by_nodes(child))
    return nodes


@unittest.skipIf(hl is None, "requires the pinned Hail integration image")
class TranscriptVatHailIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.temp_path = Path(cls.temp_dir.name)
        hl.init(
            app_name="vat_transcript_integration_test",
            master="local[2]",
            tmp_dir=str(cls.temp_path / "hail-tmp"),
            log=str(cls.temp_path / "hail.log"),
            quiet=True,
        )
        hl.default_reference("GRCh38")

        spec = importlib.util.spec_from_file_location(
            "filter_and_write_mt_hail_integration",
            ROOT / "scripts" / "filter_and_write_mt.py",
        )
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)

    @classmethod
    def tearDownClass(cls):
        hl.stop()
        cls.temp_dir.cleanup()

    def _vat_row(self, vid, transcript, **values):
        source_fields = set(self.module.VAT_ANNOTATION_FIELDS)
        source_fields.update(self.module.REQUIRED_TRANSCRIPT_VAT_FIELDS)
        row = {field: "" for field in source_fields}
        row.update(
            {
                "vid": vid,
                "transcript": transcript,
                "dbsnp_rsid": values.pop("rsid", ""),
            }
        )
        row.update(values)
        return row

    def test_composite_key_transcript_export_contract(self):
        vat_rows = [
            self._vat_row(
                "2-200-G-T",
                "ENST0003",
                rsid="rs200",
                gene_id="ENSG0003",
                gene_symbol="GENE3",
                is_canonical_transcript="true",
                consequence="missense_variant",
                aa_change="p.Gly2Val",
                gvs_max_af="0.003",
                gvs_max_subpop="eur",
            ),
            self._vat_row(
                "1-100-A-C",
                "ENST0002",
                rsid="rs100",
                gene_id="ENSG0002",
                gene_symbol="GENE2",
                is_canonical_transcript="false",
                consequence="synonymous_variant",
                aa_change="p.Ala1Ala",
                gvs_max_af="0.002",
                gvs_max_subpop="afr",
            ),
            self._vat_row(
                "1-150-C-G",
                "",
                rsid="rs150",
                consequence="intergenic_variant",
                gvs_max_af="0.004",
                gvs_max_subpop="amr",
            ),
            self._vat_row(
                "1-100-A-C",
                "ENST0001",
                rsid="rs100",
                gene_id="ENSG0001",
                gene_symbol="GENE1",
                is_canonical_transcript="true",
                consequence="missense_variant",
                aa_change="p.Ala1Pro",
                LoF_filter="one;two=three",
                gvs_max_af="0.001",
                gvs_max_subpop="sas",
            ),
            self._vat_row(
                "1-50-G-A",
                "ENST_FILTERED",
                rsid="rs50",
                gene_id="ENSG_FILTERED",
                gene_symbol="FILTERED",
                is_canonical_transcript="true",
                consequence="stop_gained",
                gvs_max_af="0.5",
                gvs_max_subpop="oth",
            ),
        ]
        vat_path = self.temp_path / "composite-key-vat.ht"
        hl.Table.parallelize(vat_rows, n_partitions=2).key_by(
            "vid", "transcript"
        ).write(str(vat_path), overwrite=True)

        variant_ht, transcript_ht = self.module._prepare_vat_tables(str(vat_path))
        self.assertEqual(list(variant_ht.key), ["locus", "alleles"])
        self.assertEqual(list(transcript_ht.key), ["locus", "alleles"])

        # Feed same-variant transcripts through a descending upstream sort; the
        # IR assertion below requires production to add the approved ascending sort.
        transcript_ht = transcript_ht.order_by(
            transcript_ht.locus,
            transcript_ht.alleles,
            hl.desc(transcript_ht.transcript),
        ).key_by("locus", "alleles")
        retained_keys = hl.Table.parallelize(
            [
                {
                    "locus": hl.Locus("chr2", 200, "GRCh38"),
                    "alleles": ["G", "T"],
                },
                {
                    "locus": hl.Locus("chr1", 150, "GRCh38"),
                    "alleles": ["C", "G"],
                },
                {
                    "locus": hl.Locus("chr1", 100, "GRCh38"),
                    "alleles": ["A", "C"],
                },
            ],
            key=["locus", "alleles"],
            n_partitions=2,
        )

        exported_ht = self.module._prepare_transcript_annotations(
            transcript_ht, retained_keys
        )
        order_by_nodes = _table_order_by_nodes(exported_ht._tir)
        self.assertGreaterEqual(len(order_by_nodes), 1)
        self.assertEqual(
            order_by_nodes[0].sort_fields,
            [("locus", "A"), ("alleles", "A"), ("transcript", "A")],
        )
        output_path = self.temp_path / "transcript_annotations.tsv.bgz"
        exported_ht.export(str(output_path))

        with gzip.open(output_path, "rt", newline="") as input_file:
            reader = csv.DictReader(input_file, delimiter="\t")
            rows = list(reader)

        self.assertEqual(reader.fieldnames, EXPECTED_COLUMNS)
        self.assertEqual(
            [(row["chrom"], row["pos"]) for row in rows],
            [("chr1", "100"), ("chr1", "100"), ("chr1", "150"), ("chr2", "200")],
        )
        self.assertEqual(
            [row["transcript"] for row in rows[:2]],
            ["ENST0001", "ENST0002"],
        )
        self.assertEqual(
            sum(row["chrom"] == "chr1" and row["pos"] == "100" for row in rows),
            2,
        )
        self.assertEqual(
            next(row for row in rows if row["pos"] == "150")["consequence"],
            "intergenic_variant",
        )
        self.assertNotIn("50", {row["pos"] for row in rows})


if __name__ == "__main__":
    unittest.main()
