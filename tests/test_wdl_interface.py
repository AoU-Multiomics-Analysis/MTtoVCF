from pathlib import Path
import unittest

import WDL


ROOT = Path(__file__).resolve().parents[1]


def expression_shape(expr):
    """Reduce a parsed miniwdl expression to its semantic AST structure."""
    if isinstance(expr, WDL.Expr.Apply):
        return (
            "apply",
            str(expr.function_name),
            tuple(expression_shape(argument) for argument in expr.arguments),
        )
    if isinstance(expr, WDL.Expr.IfThenElse):
        return (
            "if",
            expression_shape(expr.condition),
            expression_shape(expr.consequent),
            expression_shape(expr.alternative),
        )
    if isinstance(expr, WDL.Expr.Get):
        if expr.member is None and isinstance(expr.expr, WDL.Expr.Ident):
            return ("ref", str(expr.expr.name))
        return ("get", expression_shape(expr.expr), expr.member)
    if isinstance(expr, WDL.Expr.Placeholder):
        return (
            "placeholder",
            tuple(sorted(expr.options.items())),
            expression_shape(expr.expr),
        )
    if isinstance(expr, WDL.Expr.String):
        if expr.literal is not None:
            return ("literal", expr.literal.value)
        return (
            "string",
            tuple(
                part if isinstance(part, str) else expression_shape(part)
                for part in expr.parts[1:-1]
            ),
        )
    if expr.literal is not None:
        return ("literal", expr.literal.value)
    raise TypeError(f"Unsupported expression node: {type(expr).__name__}")


class WdlInterfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = WDL.load(str(ROOT / "main.wdl"))
        cls.workflow = cls.document.workflow
        cls.tasks = {task.name: task for task in cls.document.tasks}
        cls.workflow_declarations = {
            node.name: node for node in cls.workflow.body if isinstance(node, WDL.Decl)
        }
        cls.calls = {
            node.name: node for node in cls.workflow.body if isinstance(node, WDL.Tree.Call)
        }
        cls.outputs = {decl.name: decl for decl in cls.workflow.outputs}

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

    def test_index_tasks_calculate_disk_and_enforce_minimum(self):
        for task_name, source_name in (("IndexVCF", "VCF"), ("IndexAnnotations", "Annotations")):
            with self.subTest(task=task_name):
                declarations = {decl.name: decl for decl in self.tasks[task_name].postinputs}
                self.assertEqual(
                    expression_shape(declarations["CalculatedDiskGiB"].expr),
                    (
                        "apply",
                        "_add",
                        (
                            (
                                "apply",
                                "ceil",
                                (
                                    (
                                        "apply",
                                        "_mul",
                                        (
                                            (
                                                "apply",
                                                "size",
                                                (
                                                    ("ref", source_name),
                                                    ("literal", "GiB"),
                                                ),
                                            ),
                                            ("ref", "IndexDiskMultiplier"),
                                        ),
                                    ),
                                ),
                            ),
                            ("ref", "IndexDiskOverheadGiB"),
                        ),
                    ),
                )
                self.assertEqual(
                    expression_shape(declarations["IndexDiskGiB"].expr),
                    (
                        "if",
                        (
                            "apply",
                            "_gt",
                            (
                                ("ref", "CalculatedDiskGiB"),
                                ("ref", "IndexMinDiskGiB"),
                            ),
                        ),
                        ("ref", "CalculatedDiskGiB"),
                        ("ref", "IndexMinDiskGiB"),
                    ),
                )

    def test_index_task_runtimes_use_utility_image_and_calculated_disk(self):
        for task_name in ("IndexVCF", "IndexAnnotations"):
            with self.subTest(task=task_name):
                runtime = self.tasks[task_name].runtime
                self.assertEqual(
                    expression_shape(runtime["docker"]),
                    (
                        "apply",
                        "_add",
                        (
                            (
                                "literal",
                                "ghcr.io/aou-multiomics-analysis/mttovcf/utils:",
                            ),
                            ("ref", "UtilsImageTag"),
                        ),
                    ),
                )
                self.assertEqual(
                    expression_shape(runtime["disks"]),
                    (
                        "string",
                        (
                            "local-disk ",
                            ("placeholder", (), ("ref", "IndexDiskGiB")),
                            " SSD",
                        ),
                    ),
                )

    def test_workflow_builds_exact_index_destinations(self):
        self.assertEqual(
            expression_shape(self.workflow_declarations["NormalizedOutputBucket"].expr),
            (
                "apply",
                "sub",
                (
                    ("ref", "OutputBucket"),
                    ("literal", "/+$"),
                    ("literal", ""),
                ),
            ),
        )
        common_prefix = (
            "apply",
            "_add",
            (
                (
                    "apply",
                    "_add",
                    (
                        ("ref", "NormalizedOutputBucket"),
                        ("literal", "/"),
                    ),
                ),
                ("ref", "FullPrefix"),
            ),
        )
        expected = {
            "VCFIndexDestination": (
                "apply",
                "_add",
                (common_prefix, ("literal", ".vcf.bgz.tbi")),
            ),
            "AnnotationIndexDestination": (
                "apply",
                "_add",
                (common_prefix, ("literal", ".annotations.tsv.bgz.tbi")),
            ),
        }
        self.assertEqual(
            {
                name: expression_shape(self.workflow_declarations[name].expr)
                for name in expected
            },
            expected,
        )

    def test_workflow_index_calls_bind_exact_dataflow(self):
        shared_controls = {
            "UtilsImageTag": ("ref", "UtilsImageTag"),
            "IndexDiskMultiplier": ("ref", "IndexDiskMultiplier"),
            "IndexDiskOverheadGiB": ("ref", "IndexDiskOverheadGiB"),
            "IndexMinDiskGiB": ("ref", "IndexMinDiskGiB"),
        }
        expected = {
            "IndexVCF": {
                "VCF": ("ref", "filter.PathVCF"),
                "IndexDestination": ("ref", "VCFIndexDestination"),
                **shared_controls,
            },
            "IndexAnnotations": {
                "Annotations": ("ref", "filter.PathAnnotations"),
                "IndexDestination": ("ref", "AnnotationIndexDestination"),
                **shared_controls,
            },
        }
        for call_name, expected_inputs in expected.items():
            with self.subTest(call=call_name):
                self.assertEqual(
                    {
                        name: expression_shape(expr)
                        for name, expr in self.calls[call_name].inputs.items()
                    },
                    expected_inputs,
                )

    def test_workflow_exposes_sources_and_indexes(self):
        outputs = {decl.name: str(decl.type) for decl in self.workflow.outputs}
        self.assertEqual(outputs["PathVCF"], "File")
        self.assertEqual(outputs["PathAnnotations"], "File")
        self.assertEqual(outputs["VCFIndex"], "File")
        self.assertEqual(outputs["AnnotationIndex"], "File")

    def test_workflow_sources_and_indexes_have_correct_provenance(self):
        expected = {
            "PathVCF": ("ref", "filter.PathVCF"),
            "PathAnnotations": ("ref", "filter.PathAnnotations"),
            "VCFIndex": ("ref", "IndexVCF.Index"),
            "AnnotationIndex": ("ref", "IndexAnnotations.Index"),
        }
        self.assertEqual(
            {name: expression_shape(self.outputs[name].expr) for name in expected},
            expected,
        )


if __name__ == "__main__":
    unittest.main()
