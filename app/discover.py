"""Find workbench pages. No Streamlit import, so the unit tests can load it."""
from __future__ import annotations

import importlib.util
from pathlib import Path


def discover(root: Path, gold: str, semantic: str, module: str = "app_page.py") -> list[tuple[str, object]]:
    """(title, render) from each usecases/*/<module>, in path order."""
    found = []
    for path in sorted((root / "usecases").glob(f"*/{module}")):
        spec = importlib.util.spec_from_file_location(f"evx_{module}_{path.parent.name}", path)
        if spec is None or spec.loader is None:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        found.extend(mod.pages(gold, semantic))
    return found


def discover_media(root: Path, gold: str, semantic: str, bronze: str, landing: str, silver: str):
    """(title, render) from each usecases/*/media_page.py."""
    found = []
    for path in sorted((root / "usecases").glob("*/media_page.py")):
        spec = importlib.util.spec_from_file_location(f"evx_media_{path.parent.name}", path)
        if spec is None or spec.loader is None:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        found.extend(mod.pages(gold, semantic, bronze, landing, silver))
    return found
