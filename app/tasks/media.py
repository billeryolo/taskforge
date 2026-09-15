"""Image processing: re-encode an upload into an optimised web version plus a thumbnail.

Corrupt input (``UnidentifiedImageError``) is *not* retried — retrying a deterministic
failure only delays the dead letter. Only I/O errors are transient enough to retry.
"""

from pathlib import Path

from celery.exceptions import SoftTimeLimitExceeded
from PIL import Image, ImageOps, UnidentifiedImageError

from app import storage
from app.celery_app import celery_app
from app.db import session_scope
from app.logging import get_logger
from app.models import Artifact
from app.tasks.base import TrackedTask

log = get_logger(__name__)


class ImageTimeout(Exception):
    pass


def _save_variant(img: Image.Image, max_side: int, suffix: str, quality: int) -> Path:
    variant = ImageOps.exif_transpose(img).convert("RGB")
    variant.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
    path = storage.new_path("images", suffix)
    variant.save(path, format="WEBP", quality=quality, method=4)
    return path


@celery_app.task(
    base=TrackedTask,
    bind=True,
    name="media.process_image",
    autoretry_for=(OSError,),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=2,
    soft_time_limit=60,
    time_limit=90,
    # A corrupt file raises UnidentifiedImageError, which is a subclass of OSError; exclude it
    # from retries explicitly via `dont_autoretry_for` (Celery ≥ 5.3).
    dont_autoretry_for=(UnidentifiedImageError,),
)
def process_image(
    self: TrackedTask,
    *,
    source: str,
    original_name: str = "upload",
    job_id: str | None = None,
    max_side: int = 1600,
    thumb_side: int = 320,
) -> dict:
    src = storage.resolve(source)
    produced: list[Path] = []
    try:
        with Image.open(src) as img:
            img.load()  # force decode so corrupt data fails here, not during save
            width, height = img.size
            full = _save_variant(img, max_side, ".webp", quality=82)
            produced.append(full)
            thumb = _save_variant(img, thumb_side, ".thumb.webp", quality=75)
            produced.append(thumb)
    except SoftTimeLimitExceeded as exc:
        for p in produced:
            p.unlink(missing_ok=True)
        log.error("image_timeout", source=source)
        raise ImageTimeout("image processing exceeded the soft time limit") from exc
    except UnidentifiedImageError as exc:
        raise ValueError(f"not a valid image: {original_name}") from exc

    original_bytes = src.stat().st_size
    with session_scope() as session:
        rows = [
            Artifact(
                job_id=job_id,
                kind="image",
                path=storage.relative(p),
                content_type="image/webp",
                size_bytes=p.stat().st_size,
                meta={
                    "variant": variant,
                    "source": original_name,
                    "original_size": [width, height],
                },
            )
            for p, variant in ((full, "full"), (thumb, "thumb"))
        ]
        session.add_all(rows)
        session.flush()
        ids = {r.meta["variant"]: str(r.id) for r in rows}
    src.unlink(missing_ok=True)  # the original is no longer needed

    ratio = round(full.stat().st_size / original_bytes, 3) if original_bytes else None
    log.info("image_processed", original_bytes=original_bytes, ratio=ratio)
    return {
        "artifacts": ids,
        "original_bytes": original_bytes,
        "full_bytes": full.stat().st_size,
        "thumb_bytes": thumb.stat().st_size,
        "compression_ratio": ratio,
    }
