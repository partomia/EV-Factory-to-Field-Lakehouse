"""Find workbench pages. No Streamlit import, so the unit tests can load it."""
from __future__ import annotations

import importlib.util
from pathlib import Path


def discover(root: Path, gold: str, semantic: str) -> list[tuple[str, object]]:
    """(title, render) from each usecases/*/app_page.py, in path order."""
    found = []
    for path in sorted((root / "usecases").glob("*/app_page.py")):
        spec = importlib.util.spec_from_file_location(f"evx_page_{path.parent.name}", path)
        if spec is None or spec.loader is None:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        found.extend(mod.pages(gold, semantic))
    return found
