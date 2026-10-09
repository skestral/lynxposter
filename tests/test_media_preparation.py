from __future__ import annotations

from random import Random

import pytest
from PIL import Image

from app.services.media_preparation import ImageLimits, prepare_image


def test_small_supported_image_is_unchanged(tmp_path):
    path = tmp_path / "photo.png"
    Image.new("RGB", (200, 100), "orange").save(path)
    original = path.read_bytes()
    with prepare_image(path, limits=ImageLimits(max_bytes=2_000_000, max_dimension=4000)) as prepared:
        assert prepared.path == path
        assert (prepared.width, prepared.height) == (200, 100)
    assert path.read_bytes() == original


def test_oversized_photo_is_compressed_without_changing_original(tmp_path):
    path = tmp_path / "large.png"
    Image.frombytes("RGB", (900, 600), Random(4).randbytes(900 * 600 * 3)).save(path)
    original = path.read_bytes()
    with prepare_image(path, limits=ImageLimits(max_bytes=60_000, max_dimension=800, max_pixels=300_000)) as prepared:
        target = prepared.path
        assert target.stat().st_size <= 60_000
        assert max(prepared.width, prepared.height) <= 800
        assert prepared.width * prepared.height <= 300_000
        assert prepared.width / prepared.height == pytest.approx(1.5, abs=0.01)
        with Image.open(target) as actual:
            assert actual.size == (prepared.width, prepared.height)
    assert not target.exists()
    assert path.read_bytes() == original


def test_transparent_images_keep_transparency(tmp_path):
    path = tmp_path / "transparent.png"
    image = Image.new("RGBA", (400, 200), (20, 40, 60, 0))
    image.putpixel((200, 100), (255, 0, 0, 255))
    image.save(path)
    with prepare_image(path, limits=ImageLimits(max_bytes=100_000, max_dimension=200)) as prepared:
        assert prepared.mime_type == "image/png"
        with Image.open(prepared.path) as actual:
            assert actual.mode == "RGBA"
            assert actual.getpixel((0, 0))[3] == 0


def test_jpeg_destination_flattens_transparency_on_white(tmp_path):
    path = tmp_path / "transparent.png"
    Image.new("RGBA", (100, 50), (0, 0, 0, 0)).save(path)
    with prepare_image(path, limits=ImageLimits(max_bytes=100_000, supported_mime_types=("image/jpeg",))) as prepared:
        assert prepared.mime_type == "image/jpeg"
        with Image.open(prepared.path) as actual:
            assert actual.getpixel((0, 0)) == (255, 255, 255)


def test_exif_rotation_is_applied_before_resizing(tmp_path):
    path = tmp_path / "rotated.jpg"
    image = Image.new("RGB", (400, 200), "red")
    exif = Image.Exif()
    exif[274] = 6
    image.save(path, exif=exif)
    with prepare_image(path, limits=ImageLimits(max_bytes=100_000, max_dimension=200)) as prepared:
        assert (prepared.width, prepared.height) == (100, 200)
        with Image.open(prepared.path) as actual:
            assert actual.getexif().get(274, 1) == 1


def test_temporary_file_is_removed_when_upload_raises(tmp_path):
    path = tmp_path / "large.png"
    Image.new("RGB", (400, 200)).save(path)
    with pytest.raises(RuntimeError, match="upload failed"):
        with prepare_image(path, limits=ImageLimits(max_bytes=100_000, max_dimension=200)) as prepared:
            target = prepared.path
            raise RuntimeError("upload failed")
    assert not target.exists()
    assert path.exists()


def test_animation_is_not_silently_flattened(tmp_path):
    path = tmp_path / "animated.gif"
    frames = [Image.new("RGB", (20, 20), color) for color in ("red", "blue")]
    frames[0].save(path, save_all=True, append_images=frames[1:], duration=100, loop=0)
    original = path.read_bytes()
    with pytest.raises(ValueError, match="animated image"):
        with prepare_image(path, limits=ImageLimits(max_bytes=100_000, supported_mime_types=("image/jpeg",))):
            pass
    assert path.read_bytes() == original


def test_pixel_limit_is_enforced_for_extreme_panorama(tmp_path):
    path = tmp_path / "panorama.png"
    Image.new("RGB", (1000, 1)).save(path)
    with prepare_image(path, limits=ImageLimits(max_bytes=100_000, max_pixels=100)) as prepared:
        assert prepared.width * prepared.height <= 100


def test_destination_minimum_width_is_respected(tmp_path):
    path = tmp_path / "small.png"
    Image.new("RGB", (100, 50)).save(path)
    with prepare_image(path, limits=ImageLimits(max_bytes=100_000, max_dimension=1440, min_width=320)) as prepared:
        assert (prepared.width, prepared.height) == (320, 160)
