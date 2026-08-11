import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_WDL = ROOT / "workflow" / "FilterMT.wdl"
SMOKE_WDL = ROOT / "tests" / "wdl" / "filtermt_optional_output_smoke.wdl"
OUTPUT_PREFIX = "File? TranscriptAnnotations = if AnnotateWithVAT"


def _optional_output_declaration(path):
    declarations = [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip().startswith(OUTPUT_PREFIX)
    ]
    if len(declarations) != 1:
        raise AssertionError(
            f"expected one optional transcript output declaration in {path}, "
            f"found {len(declarations)}"
        )
    return declarations[0]


@unittest.skipUnless(shutil.which("miniwdl"), "miniwdl is not installed")
class FilterMTOptionalOutputSmokeTests(unittest.TestCase):
    def test_false_annotation_flag_yields_absent_transcript_output(self):
        self.assertEqual(
            _optional_output_declaration(PRODUCTION_WDL),
            _optional_output_declaration(SMOKE_WDL),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            result_path = temp_path / "result.json"
            subprocess.run(
                [
                    "miniwdl",
                    "run",
                    str(SMOKE_WDL),
                    "AnnotateWithVAT=false",
                    "--dir",
                    str(temp_path / "run") + "/.",
                    "--no-cache",
                    "--as-me",
                    "--no-color",
                    "-o",
                    str(result_path),
                ],
                cwd=ROOT,
                check=True,
                text=True,
                capture_output=True,
            )
            result = json.loads(result_path.read_text())

        self.assertIsNone(
            result["outputs"][
                "FilterMTOptionalOutputSmoke.TranscriptAnnotations"
            ]
        )


if __name__ == "__main__":
    unittest.main()
