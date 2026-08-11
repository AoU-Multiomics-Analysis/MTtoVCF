from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from output_manifest import write_output_manifest


class OutputManifestTests(unittest.TestCase):
    def test_write_output_manifest_records_exact_cloud_uri(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "annotations_outpath.txt"
            uri = "gs://bucket/results/sample.annotations.tsv.bgz"

            write_output_manifest(str(manifest), uri)

            self.assertEqual(manifest.read_text(), uri)


if __name__ == "__main__":
    unittest.main()
