from pathlib import Path
import unittest

import WDL


ROOT = Path(__file__).resolve().parents[1]


class WdlInterfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = WDL.load(str(ROOT / "main.wdl"))
        cls.workflow = cls.document.workflow
        cls.tasks = {task.name: task for task in cls.document.tasks}

    def test_workflow_index_inputs_have_expected_defaults(self):
        inputs = {decl.name: decl for decl in self.workflow.inputs}
        expected = {
            "UtilsImageTag": "main",
            "IndexDiskMultiplier": 2.0,
            "IndexDiskOverheadGiB": 10,
            "IndexMinDiskGiB": 20,
        }
        for name, value in expected.items():
            self.assertIn(name, inputs)
            self.assertEqual(inputs[name].expr.literal.value, value)

    def test_compiler_sees_both_index_task_interfaces(self):
        expected_inputs = {
            "IndexDestination",
            "UtilsImageTag",
            "IndexDiskMultiplier",
            "IndexDiskOverheadGiB",
            "IndexMinDiskGiB",
        }
        for task_name, source_name in (("IndexVCF", "VCF"), ("IndexAnnotations", "Annotations")):
            task = self.tasks[task_name]
            self.assertEqual({decl.name for decl in task.inputs}, expected_inputs | {source_name})
            self.assertEqual({decl.name for decl in task.outputs}, {"Index"})
            self.assertEqual(set(task.runtime), {"docker", "memory", "cpu", "disks"})

    def test_workflow_exposes_sources_and_indexes(self):
        outputs = {decl.name: str(decl.type) for decl in self.workflow.outputs}
        self.assertEqual(outputs["PathVCF"], "File")
        self.assertEqual(outputs["PathAnnotations"], "File")
        self.assertEqual(outputs["VCFIndex"], "File")
        self.assertEqual(outputs["AnnotationIndex"], "File")


if __name__ == "__main__":
    unittest.main()
