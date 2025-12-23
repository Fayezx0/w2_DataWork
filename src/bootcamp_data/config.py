"""Configuration settings for data processing."""

from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"

# Other directories
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
REPORTS_DIR = PROJECT_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

from dataclasses import dataclass

@dataclass(frozen=True)# الباث يجب ان يكون ثابت ولا احد يستطيع تعديله
class Paths:
    root: Path
    raw: Path
    cache: Path
    processed: Path
    external: Path

def make_paths(root: Path) -> Paths:
    data = root / "data"
    return Paths(
        root=root,
        raw=data / "raw",
        cache=data / "cache",
        processed=data / "processed",
        external=data / "external",
    )
