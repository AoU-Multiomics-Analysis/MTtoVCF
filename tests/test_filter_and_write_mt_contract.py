import importlib.util
from pathlib import Path
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.modules.setdefault("hail", types.ModuleType("hail"))
SPEC = importlib.util.spec_from_file_location(
    "filter_and_write_mt", ROOT / "scripts" / "filter_and_write_mt.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
HAIL_LOF_SPEC = importlib.util.spec_from_file_location(
    "extract_lof_carriers_hail",
    ROOT / "scripts" / "extract_lof_carriers_hail.py",
)
HAIL_LOF_MODULE = importlib.util.module_from_spec(HAIL_LOF_SPEC)
HAIL_LOF_SPEC.loader.exec_module(HAIL_LOF_MODULE)


class TranscriptVatContractTests(unittest.TestCase):
    def test_transcript_output_fields_are_complete(self):
        self.assertEqual(
            MODULE.TRANSCRIPT_ANNOTATION_FIELDS,
            (
                "rsid", "gene_id", "gene_symbol", "transcript",
                "is_canonical_transcript", "consequence", "aa_change",
                "LoF", "LoF_filter", "LoF_flags", "LoF_info",
                "gvs_max_af", "gvs_max_subpop",
            ),
        )

    def test_missing_required_fields_are_sorted(self):
        available = set(MODULE.REQUIRED_TRANSCRIPT_VAT_FIELDS) - {
            "gene_id", "transcript"
        }
        self.assertEqual(
            MODULE._missing_transcript_vat_fields(available),
            ["gene_id", "transcript"],
        )

    def test_vid_and_dbsnp_source_fields_are_required(self):
        self.assertIn("vid", MODULE.REQUIRED_TRANSCRIPT_VAT_FIELDS)
        self.assertIn("dbsnp_rsid", MODULE.REQUIRED_TRANSCRIPT_VAT_FIELDS)

    def test_filter_workflow_exposes_optional_transcript_output(self):
        source = (ROOT / "workflow" / "FilterMT.wdl").read_text()
        self.assertIn(
            "File? TranscriptAnnotations = TaskFilterMT.TranscriptAnnotations",
            source,
        )
        self.assertIn(
            "File? TranscriptAnnotations = if AnnotateWithVAT then "
            "read_string('transcript_annotations_outpath.txt') else "
            "'transcript_annotations_outpath.txt'",
            source,
        )

    def test_filter_workflow_exposes_reusable_filtered_matrix_table(self):
        source = (ROOT / "workflow" / "FilterMT.wdl").read_text()
        self.assertIn(
            "String FilteredMatrixTable = TaskFilterMT.FilteredMatrixTable",
            source,
        )
        self.assertIn(
            "String FilteredMatrixTable = read_string("
            "'filtered_matrix_table_outpath.txt'"
            ")",
            source,
        )

    def test_main_workflow_propagates_transcript_output(self):
        source = (ROOT / "main.wdl").read_text()
        self.assertIn(
            "File? TranscriptAnnotations = filter.TranscriptAnnotations",
            source,
        )
        self.assertIn(
            "String FilteredRareVariantMatrixTable = filter.FilteredMatrixTable",
            source,
        )

    def test_main_workflow_exposes_lof_carrier_outputs(self):
        source = (ROOT / "main.wdl").read_text()
        self.assertIn("Boolean MakeLoFCarriers = false", source)
        self.assertIn("Int LoFCarrierTaskCpu = 64", source)
        self.assertIn("String LoFCarrierTaskMemory = \"256G\"", source)
        self.assertIn("vcf_index = IndexVCF.Index", source)
        self.assertIn("import \"workflow/HailLoFCarrierTable.wdl\"", source)
        self.assertIn("if (MakeLoFCarriers)", source)
        self.assertIn(
            "call HailLoFCarrierTable.HailLoFCarrierTable as HailLoFCarriers",
            source,
        )
        self.assertIn("make_lof_carriers = false", source)
        self.assertIn(
            "File? LoFCarriersHC = HailLoFCarriers.LoFCarriersHC",
            source,
        )
        self.assertIn(
            "File? LoFCarriersHCOrLC = HailLoFCarriers.LoFCarriersHCOrLC",
            source,
        )
        self.assertIn(
            "UriMatrixTable = filter.FilteredMatrixTable",
            source,
        )
        self.assertIn("MatrixTableAlreadyFiltered = true", source)

    def test_filter_task_materializes_final_matrix_table_once(self):
        source = (ROOT / "scripts" / "filter_and_write_mt.py").read_text()
        self.assertIn(
            "filtered_matrix_table_path = _join_cloud_path(\n"
            "        args.CloudTmpdir,",
            source,
        )
        self.assertIn(
            "filtered_matrix_table.write(filtered_matrix_table_path, overwrite=True)",
            source,
        )
        self.assertIn("filtered_matrix_table_outpath.txt", source)
        self.assertNotIn("hl.agg.call_stats", source)

    def test_filtered_matrix_table_checkpoint_is_lean_and_pre_vat(self):
        source = (ROOT / "scripts" / "filter_and_write_mt.py").read_text()
        self.assertIn("filtered_matrix_table = (", source)
        self.assertIn("mt_filtered.select_rows()", source)
        self.assertIn(".select_cols()", source)
        self.assertIn('.select_entries("GT")', source)
        self.assertIn(
            "filtered_matrix_table.write(filtered_matrix_table_path, overwrite=True)",
            source,
        )
        checkpoint_write = source.index(
            "filtered_matrix_table.write(filtered_matrix_table_path, overwrite=True)"
        )
        vat_join = source.index(
            "mt_filtered = mt_filtered.annotate_rows(_vat=vat_ht[mt_filtered.row_key])"
        )
        self.assertLess(checkpoint_write, vat_join)
        self.assertNotIn(
            "mt_filtered = hl.read_matrix_table(filtered_matrix_table_path)",
            source,
        )

    def test_wdl_has_explicit_lean_checkpoint_update_marker(self):
        marker = (
            "# MTtoVCF update tag: lean-filtered-matrix-table-20260816"
        )
        for path in (ROOT / "main.wdl", ROOT / "workflow" / "FilterMT.wdl"):
            source = path.read_text()
            self.assertIn(f"version 1.0\n{marker}", source)
        self.assertIn(marker, (ROOT / "workflow" / "FilterMT.wdl").read_text())

    def test_lof_carrier_workflow_uses_dedicated_image(self):
        source = (ROOT / "workflow" / "LoFCarrierTable.wdl").read_text()
        self.assertIn(
            'docker: "ghcr.io/aou-multiomics-analysis/mttovcf/lof-carriers:main"',
            source,
        )

    def test_hail_lof_workflow_contract(self):
        self.assertEqual(
            HAIL_LOF_MODULE.REQUIRED_LOF_VAT_FIELDS,
            ("vid", "gene_id", "gene_symbol", "LoF"),
        )
        self.assertEqual(
            HAIL_LOF_MODULE._missing_lof_vat_fields({"vid", "LoF"}),
            ["gene_id", "gene_symbol"],
        )

        workflow_source = (
            ROOT / "workflow" / "HailLoFCarrierTable.wdl"
        ).read_text()
        self.assertIn("--VATHailTable ~{VATHailTable}", workflow_source)
        self.assertIn("/extract_lof_carriers_hail.py", workflow_source)
        self.assertIn("--MatrixTableAlreadyFiltered", workflow_source)
        self.assertIn(
            'docker: "ghcr.io/aou-multiomics-analysis/mttovcf:" + Branch',
            workflow_source,
        )

        hail_lof_source = (
            ROOT / "scripts" / "extract_lof_carriers_hail.py"
        ).read_text()
        self.assertIn(
            "if not args.MatrixTableAlreadyFiltered:",
            hail_lof_source,
        )

    def test_hail_lof_membership_uses_hail_sets(self):
        source = (ROOT / "scripts" / "extract_lof_carriers_hail.py").read_text()
        self.assertIn(
            "hl.literal(set(LOF_CLASSES)).contains(vat_ht.LoF)",
            source,
        )

        dockstore_source = (ROOT / ".dockstore.yml").read_text()
        self.assertIn(
            "primaryDescriptorPath: /workflow/HailLoFCarrierTable.wdl",
            dockstore_source,
        )
        self.assertIn("name: HailLoFCarrierTable", dockstore_source)

    def test_hail_lof_outputs_share_materialized_data(self):
        source = (ROOT / "scripts" / "extract_lof_carriers_hail.py").read_text()
        self.assertIn(
            "lof_intermediate_path = _join_cloud_path(\n        args.CloudTmpdir",
            source,
        )
        self.assertIn(
            "mt.write(lof_intermediate_path, overwrite=True)",
            source,
        )
        self.assertIn(
            "lof_mt = hl.read_matrix_table(lof_intermediate_path)",
            source,
        )
        self.assertIn("hl.export_vcf(lof_mt, lof_vcf_output)", source)
        self.assertIn(
            "carrier_ht = carrier_ht.checkpoint(carrier_intermediate_path, overwrite=True)",
            source,
        )
        self.assertIn("lof_variants_vcf_outpath.txt", source)

    def test_hail_lof_workflow_indexes_vcf_post_hoc(self):
        workflow_source = (
            ROOT / "workflow" / "HailLoFCarrierTable.wdl"
        ).read_text()
        self.assertIn("call IndexLoFVCF", workflow_source)
        self.assertIn("bcftools index --tbi --force", workflow_source)
        self.assertIn(
            'docker: "ghcr.io/aou-multiomics-analysis/mttovcf/utils:main"',
            workflow_source,
        )
        self.assertIn(
            "File LoFVariantsVCF = ExtractHailLoFCarriers.lof_variants_vcf",
            workflow_source,
        )
        self.assertIn(
            "File LoFVariantsVCFIndex = IndexLoFVCF.index",
            workflow_source,
        )

    def test_main_workflow_propagates_lof_vcf_outputs(self):
        source = (ROOT / "main.wdl").read_text()
        self.assertIn(
            "call HailLoFCarrierTable.HailLoFCarrierTable as HailLoFCarriers",
            source,
        )
        self.assertIn(
            "File? LoFVariantsVCF = HailLoFCarriers.LoFVariantsVCF",
            source,
        )
        self.assertIn(
            "File? LoFVariantsVCFIndex = HailLoFCarriers.LoFVariantsVCFIndex",
            source,
        )


if __name__ == "__main__":
    unittest.main()
