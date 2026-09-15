import uuid
from pathlib import Path

from app.config import get_settings


def storage_root() -> Path:
    root = get_settings().storage_dir
    root.mkdir(parents=True, exist_ok=True)
    return root


def new_path(kind: str, suffix: str) -> Path:
    """Unique on-disk location for a new artifact, grouped by kind."""
    folder = storage_root() / kind
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{uuid.uuid4().hex}{suffix}"


def resolve(relative: str) -> Path:
    path = (storage_root() / relative).resolve()
    if storage_root().resolve() not in path.parents:
        raise ValueError("path escapes storage root")
    return path


def relative(path: Path) -> str:
    return str(path.resolve().relative_to(storage_root().resolve())).replace("\\", "/")
