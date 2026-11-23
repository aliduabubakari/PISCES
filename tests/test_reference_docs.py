"""Keep published API/CLI references and curated documentation links usable."""

import ast
import importlib.util
import json
from pathlib import Path
import re

import pytest

ROOT = Path(__file__).resolve().parents[1]


def exporter():
    spec = importlib.util.spec_from_file_location(
        "reference_exporter", ROOT / "scripts/export_reference_docs.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generated_cli_help_is_current():
    assert (
        ROOT / "docs/reference/cli-help.md"
    ).read_text() == exporter().cli_reference()


def test_generated_openapi_is_current_and_refs_resolve():
    pytest.importorskip("fastapi")
    document = json.loads(exporter().openapi_reference())
    assert document == json.loads((ROOT / "docs/reference/openapi.json").read_text())
    assert document["security"] == [{"BearerAuth": []}]

    def visit(value):
        if isinstance(value, dict):
            if "$ref" in value:
                assert value["$ref"].startswith("#/")
                target = document
                for key in value["$ref"][2:].split("/"):
                    target = target[key.replace("~1", "/").replace("~0", "~")]
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)


def test_getting_started_local_links_exist():
    for relative in (
        "README.md",
        "docs/README.md",
        "docs/API.md",
        "docs/CLI_REFERENCE.md",
    ):
        path = ROOT / relative
        for link in re.findall(r"\]\(([^)]+)\)", path.read_text()):
            if ":" in link or link.startswith("#"):
                continue
            target = (path.parent / link.split("#", 1)[0]).resolve()
            assert target.is_relative_to(ROOT)
            assert target.exists(), f"Broken link in {relative}: {link}"


def test_documented_python_examples_parse():
    text = (ROOT / "docs/API.md").read_text()
    blocks = re.findall(r"```python\n(.*?)\n```", text, flags=re.DOTALL)
    assert len(blocks) == 2
    for block in blocks:
        ast.parse(block)
