from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    """Container for canonical project paths."""

    root: Path
    raw: Path
    cache: Path
    processed: Path
    external: Path


def make_paths(root: Path) -> Paths:
    """Return common paths under the repository root."""

    data = root / "data"
    return Paths(
        root=root,
        raw=data / "raw",
        cache=data / "cache",
        processed=data / "processed",
        external=data / "external",
    )

