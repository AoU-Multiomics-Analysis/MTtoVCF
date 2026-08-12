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

    def test_main_workflow_propagates_transcript_output(self):
        source = (ROOT / "main.wdl").read_text()
        self.assertIn(
            "File? TranscriptAnnotations = filter.TranscriptAnnotations",
            source,
        )

    def test_main_workflow_exposes_lof_carrier_outputs(self):
        source = (ROOT / "main.wdl").read_text()
        self.assertIn("Boolean MakeLoFCarriers = false", source)
        self.assertIn("Int LoFCarrierThreads = 4", source)
        self.assertIn("vcf_index = IndexVCF.Index", source)
        self.assertIn(
            "make_lof_carriers = MakeLoFCarriers && AnnotateWithVAT",
            source,
        )
        self.assertIn(
            "File? LoFCarriersHC = postprocess.LoFCarriersHC",
            source,
        )
        self.assertIn(
            "File? LoFCarriersHCOrLC = postprocess.LoFCarriersHCOrLC",
            source,
        )

    def test_lof_carrier_workflow_uses_dedicated_image(self):
        source = (ROOT / "workflow" / "LoFCarrierTable.wdl").read_text()
        self.assertIn(
            'docker: "ghcr.io/aou-multiomics-analysis/mttovcf/lof-carriers:main"',
            source,
        )


if __name__ == "__main__":
    unittest.main()
