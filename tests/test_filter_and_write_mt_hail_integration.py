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
        lof_spec = importlib.util.spec_from_file_location(
            "extract_lof_carriers_hail_integration",
            ROOT / "scripts" / "extract_lof_carriers_hail.py",
        )
        cls.lof_module = importlib.util.module_from_spec(lof_spec)
        lof_spec.loader.exec_module(cls.lof_module)

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

    def test_hail_lof_outputs_preserve_hc_only_semantics(self):
        mt = hl.utils.range_matrix_table(4, 2)
        mt = mt.annotate_cols(s=hl.str(mt.col_idx)).key_cols_by("s")
        mt = mt.annotate_rows(
            annotations=hl.case()
            .when(
                mt.row_idx == 0,
                [
                    hl.struct(
                        annotation_group="lof",
                        gene_id="ENSG0001",
                        gene_symbol="GENE1",
                        annotation_value="HC",
                        lof_class="HC",
                    )
                ],
            )
            .when(
                mt.row_idx == 1,
                [
                    hl.struct(
                        annotation_group="lof",
                        gene_id="ENSG0001",
                        gene_symbol="GENE1",
                        annotation_value="LC",
                        lof_class="LC",
                    )
                ],
            )
            .when(
                mt.row_idx == 2,
                [
                    hl.struct(
                        annotation_group="splice_donor",
                        gene_id="ENSG0001",
                        gene_symbol="GENE1",
                        annotation_value="splice_donor_variant",
                        lof_class=hl.missing(hl.tstr),
                    )
                ],
            )
            .default(
                [
                    hl.struct(
                        annotation_group="splice_acceptor",
                        gene_id="ENSG0002",
                        gene_symbol="GENE2",
                        annotation_value="splice_acceptor_variant",
                        lof_class=hl.missing(hl.tstr),
                    )
                ]
            ),
            lof_annotations=hl.case()
            .when(
                mt.row_idx == 0,
                [
                    hl.struct(
                        annotation_group="lof",
                        gene_id="ENSG0001",
                        gene_symbol="GENE1",
                        annotation_value="HC",
                        lof_class="HC",
                    )
                ],
            )
            .when(
                mt.row_idx == 1,
                [
                    hl.struct(
                        annotation_group="lof",
                        gene_id="ENSG0001",
                        gene_symbol="GENE1",
                        annotation_value="LC",
                        lof_class="LC",
                    )
                ],
            )
            .default([]),
            variant_id=hl.str(mt.row_idx),
        )
        mt = mt.annotate_entries(GT=hl.call(0, 1))

        annotated_mt = self.lof_module._annotate_lof_vcf_fields(
            mt.filter_rows(hl.len(mt.lof_annotations) > 0)
        )
        rows = annotated_mt.rows().select(
            "variant_id",
            "LOF_GENE_ID", "LOF_GENE_SYMBOL", "LOF_CLASS"
        ).collect()
        rows_by_variant = {row.variant_id: row for row in rows}
        self.assertEqual(rows_by_variant["0"].LOF_GENE_ID, ["ENSG0001"])
        self.assertEqual(rows_by_variant["0"].LOF_GENE_SYMBOL, ["GENE1"])
        self.assertEqual(rows_by_variant["0"].LOF_CLASS, ["HC"])
        self.assertEqual(rows_by_variant["1"].LOF_CLASS, ["LC"])

        entries_ht = mt.explode_rows(mt.annotations).entries()
        entries_ht = entries_ht.filter(
            hl.is_defined(entries_ht.GT) & entries_ht.GT.is_non_ref()
        )
        carrier_ht = self.lof_module._aggregate_carrier_table(entries_ht)
        hc_rows = self.lof_module._format_lof_carrier_table(
            carrier_ht, hc_only=True
        ).collect()
        all_rows = self.lof_module._format_lof_carrier_table(
            carrier_ht, hc_only=False
        ).collect()
        group_by_name = {
            group.name: group for group in self.lof_module.ANNOTATION_GROUPS
        }
        splice_donor_rows = self.lof_module._format_group_carrier_table(
            carrier_ht, group_by_name["splice_donor"]
        ).collect()
        splice_acceptor_rows = self.lof_module._format_group_carrier_table(
            carrier_ht, group_by_name["splice_acceptor"]
        ).collect()

        self.assertEqual(len(hc_rows), 2)
        self.assertEqual(len(all_rows), 2)
        self.assertEqual({row.n_lof_variants for row in hc_rows}, {1})
        self.assertEqual({row.n_lof_variants for row in all_rows}, {2})
        self.assertEqual({row.variant_ids for row in hc_rows}, {"0"})
        self.assertEqual({row.variant_ids for row in all_rows}, {"0,1"})
        self.assertEqual(len(splice_donor_rows), 2)
        self.assertEqual(len(splice_acceptor_rows), 2)
        self.assertEqual(
            {row.has_splice_donor_variant for row in splice_donor_rows},
            {"true"},
        )
        self.assertEqual(
            {row.n_splice_donor_variants for row in splice_donor_rows},
            {1},
        )
        self.assertEqual({row.variant_ids for row in splice_donor_rows}, {"2"})
        self.assertEqual(
            {row.consequences for row in splice_donor_rows},
            {"splice_donor_variant"},
        )
        self.assertEqual(
            {row.has_splice_acceptor_variant for row in splice_acceptor_rows},
            {"true"},
        )
        self.assertEqual(
            {row.n_splice_acceptor_variants for row in splice_acceptor_rows},
            {1},
        )
        self.assertEqual(
            {row.variant_ids for row in splice_acceptor_rows},
            {"3"},
        )
        self.assertEqual(
            {row.consequences for row in splice_acceptor_rows},
            {"splice_acceptor_variant"},
        )

        empty_group = self.lof_module.AnnotationGroup(
            name="missense",
            source_field="consequence",
            matching_values=("missense_variant",),
            annotation_header="consequences",
            has_header="has_missense_variant",
            count_header="n_missense_variants",
            output_filename="missense_carriers.tsv.bgz",
        )
        empty_output = self.temp_path / "empty_group.tsv.bgz"
        self.lof_module._format_group_carrier_table(
            carrier_ht, empty_group
        ).export(str(empty_output))
        with gzip.open(empty_output, "rt", newline="") as input_file:
            self.assertEqual(
                input_file.read().strip(),
                "\t".join(
                    [
                        "sample_id",
                        "gene_id",
                        "gene_symbol",
                        "has_missense_variant",
                        "n_missense_variants",
                        "variant_ids",
                        "consequences",
                    ]
                ),
            )


if __name__ == "__main__":
    unittest.main()
