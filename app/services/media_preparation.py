from __future__ import annotations

import math
from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from PIL import Image, ImageOps


@dataclass(frozen=True, slots=True)
class ImageLimits:
    max_bytes: int
    max_dimension: int | None = None
    max_pixels: int | None = None
    supported_mime_types: tuple[str, ...] = ("image/jpeg", "image/png", "image/webp")
    min_width: int | None = None


@dataclass(frozen=True, slots=True)
class PreparedImage:
    path: Path
    mime_type: str
    width: int
    height: int


def _bounded_dimensions(width: int, height: int, limits: ImageLimits) -> tuple[int, int]:
    scale = max(1.0, limits.min_width / width) if limits.min_width else 1.0
    if limits.max_dimension:
        scale = min(scale, limits.max_dimension / max(width, height))
    if limits.max_pixels:
        scale = min(scale, math.sqrt(limits.max_pixels / (width * height)))
    target_width, target_height = max(1, math.floor(width * scale)), max(1, math.floor(height * scale))
    if limits.max_pixels and target_width * target_height > limits.max_pixels:
        if target_width >= target_height:
            target_width = max(1, limits.max_pixels // target_height)
        else:
            target_height = max(1, limits.max_pixels // target_width)
    if limits.min_width and target_width < limits.min_width:
        raise ValueError("This image cannot fit the destination's minimum width and maximum dimensions while preserving its aspect ratio.")
    return target_width, target_height


def _encode_image(image: Image.Image, mime_type: str, quality: int = 95) -> bytes:
    output = BytesIO()
    if mime_type == "image/jpeg":
        if image.mode == "RGBA":
            background = Image.new("RGB", image.size, "white")
            background.paste(image, mask=image.getchannel("A"))
            image = background
        image.convert("RGB").save(output, format="JPEG", quality=quality, optimize=True)
    elif mime_type == "image/png":
        image.save(output, format="PNG", optimize=True)
    else:
        image.save(output, format="WEBP", quality=quality, method=6)
    return output.getvalue()


@contextmanager
def prepare_image(path: Path, *, limits: ImageLimits) -> Iterator[PreparedImage]:
    """Prepare a temporary destination image, leaving the canonical file untouched."""
    if limits.max_bytes <= 0 or any(value is not None and value <= 0 for value in (limits.max_dimension, limits.max_pixels, limits.min_width)):
        raise ValueError("Image upload limits must be positive.")
    path = Path(path)
    with Image.open(path) as source:
        source_mime = Image.MIME.get(source.format or "", "application/octet-stream")
        dimensions = _bounded_dimensions(*source.size, limits)
        needs_preparation = (
            path.stat().st_size > limits.max_bytes
            or dimensions != source.size
            or source_mime not in limits.supported_mime_types
            or source.mode not in {"1", "L", "LA", "P", "RGB", "RGBA"}
            or source.getexif().get(274, 1) != 1
        )
        if not needs_preparation:
            prepared = PreparedImage(path, source_mime, *source.size)
            image = None
        else:
            if getattr(source, "is_animated", False):
                raise ValueError("This animated image exceeds the destination limits or uses an unsupported format; upload it as a video instead.")
            oriented = ImageOps.exif_transpose(source)
            has_alpha = oriented.mode in {"RGBA", "LA"} or "transparency" in oriented.info
            image = oriented.convert("RGBA" if has_alpha else "RGB")
            dimensions = _bounded_dimensions(*image.size, limits)
            if image.size != dimensions:
                image = image.resize(dimensions, Image.Resampling.LANCZOS)

    if image is None:
        yield prepared
        return

    supported = [mime for mime in limits.supported_mime_types if mime in {"image/jpeg", "image/png", "image/webp"}]
    if not supported:
        raise ValueError("The destination does not accept a supported image format (JPEG, PNG, or WebP).")
    if image.mode == "RGBA":
        preferred = [mime for mime in ("image/png", "image/webp", "image/jpeg") if mime in supported]
    else:
        preferred = [mime for mime in (source_mime, "image/jpeg", "image/png", "image/webp") if mime in supported]
    preferred = list(dict.fromkeys(preferred))

    for _ in range(24):
        smallest_size = None
        for mime_type in preferred:
            # Keep transparency when the destination accepts it, even if resizing is needed.
            if image.mode == "RGBA" and mime_type == "image/jpeg" and any(mime in supported for mime in ("image/png", "image/webp")):
                continue
            qualities = (95,) if mime_type == "image/png" else (95, 90, 85, 80, 70, 60, 50)
            for quality in qualities:
                data = _encode_image(image, mime_type, quality)
                smallest_size = min(smallest_size or len(data), len(data))
                if len(data) > limits.max_bytes:
                    continue
                suffix = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}[mime_type]
                with TemporaryDirectory(prefix="lynxposter-image-") as temp_dir:
                    target = Path(temp_dir) / f"image{suffix}"
                    target.write_bytes(data)
                    yield PreparedImage(target, mime_type, *image.size)
                return
        if image.size == (1, 1):
            break
        scale = min(0.85, math.sqrt(limits.max_bytes / (smallest_size or limits.max_bytes)) * 0.95)
        image = image.resize((max(1, int(image.width * scale)), max(1, int(image.height * scale))), Image.Resampling.LANCZOS)
        if limits.min_width and image.width < limits.min_width:
            break
    raise ValueError(f"Could not prepare an image within the destination's {limits.max_bytes:,}-byte upload limit.")
