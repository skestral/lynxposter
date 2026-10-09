from __future__ import annotations

import hashlib
import json
import shutil
import struct
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services import video_preparation as videos
from app.services.video_preparation import VideoLimits, prepare_video


def _info(**changes):
    baseline = videos._VideoInfo(
        width=640, height=480, duration=10.0, frame_rate=30.0,
        codec="h264", pixel_format="yuv420p", field_order="progressive", audio_codec="aac", audio_profile="LC", audio_channels=2,
        audio_sample_rate=48_000, audio_bitrate=96_000, video_bitrate=1_000_000,
        video_index=0, rotation=0.0, square_pixels=True, has_display_transform=False,
    )
    return replace(baseline, **changes)


def _mp4(path, *, size=200, fast_start=True, major_brand=b"isom"):
    ftyp = struct.pack(">I4s", 24, b"ftyp") + major_brand + b"\x00" * 4 + b"isomiso2"
    moov = struct.pack(">I4s", 8, b"moov")
    mdat = struct.pack(">I4s", size - len(ftyp) - len(moov), b"mdat") + b"\x00" * (size - 40)
    path.write_bytes(ftyp + (moov + mdat if fast_start else mdat + moov))
    return path


def _mock_conversion(monkeypatch, original, source_info, output_infos, output_sizes):
    calls = []
    probe_count = 0

    def fake_probe(path):
        nonlocal probe_count
        if path == original.resolve():
            return source_info
        result = output_infos[min(probe_count, len(output_infos) - 1)]
        probe_count += 1
        return result

    def fake_run(arguments, **kwargs):
        size = output_sizes[min(len(calls), len(output_sizes) - 1)]
        calls.append(arguments)
        assert kwargs["timeout"] >= 120
        assert "-nostdin" in arguments
        _mp4(Path(arguments[-1]), size=size)
        return ""

    monkeypatch.setattr(videos, "_probe", fake_probe)
    monkeypatch.setattr(videos, "_run", fake_run)
    monkeypatch.setattr(videos, "_tool", lambda name: name)
    return calls


def test_compatible_fast_start_video_passes_without_copy_or_encoder(tmp_path, monkeypatch):
    original = _mp4(tmp_path / "original.mp4")
    monkeypatch.setattr(videos, "_probe", lambda path: _info())
    monkeypatch.setattr(videos, "_tool", lambda name: pytest.fail("Compatible media must not invoke the encoder"))
    before = original.read_bytes()
    with prepare_video(original, limits=VideoLimits(max_bytes=1_000)) as prepared:
        assert prepared.path == original.resolve()
        assert prepared.mime_type == "video/mp4"
        assert (prepared.width, prepared.height, prepared.frame_rate) == (640, 480, 30)
        assert prepared.size_bytes == len(before)
    assert original.read_bytes() == before


@pytest.mark.parametrize("suffix,fast_start", [(".mp4", False), (".upload", True)])
def test_compatible_video_remuxes_for_fast_start_or_filename(tmp_path, monkeypatch, suffix, fast_start):
    original = _mp4(tmp_path / f"original{suffix}", fast_start=fast_start)
    before = original.read_bytes()
    calls = _mock_conversion(monkeypatch, original, _info(), [_info()], [220])
    with prepare_video(original, limits=VideoLimits(max_bytes=1_000)) as prepared:
        target = prepared.path
        assert target != original
        assert target.suffix == ".mp4"
        assert videos._mp4_layout(target) == (True, True)
    assert not target.exists()
    assert original.read_bytes() == before
    assert len(calls) == 1
    assert calls[0][calls[0].index("-c") + 1] == "copy"


def test_transcode_normalizes_rotated_hevc_audio_dimensions_and_frame_rate(tmp_path, monkeypatch):
    original = _mp4(tmp_path / "phone.mov", major_brand=b"qt  ")
    before = original.read_bytes()
    source = _info(width=1080, height=1920, codec="hevc", pixel_format="yuv420p10le", audio_codec="pcm_s16le", audio_channels=6, rotation=90, has_display_transform=True, frame_rate=60)
    output = _info(width=360, height=640, frame_rate=30)
    calls = _mock_conversion(monkeypatch, original, source, [output], [800])
    with prepare_video(original, limits=VideoLimits(max_bytes=1_000_000, max_dimension=640, max_pixels=640 * 360, max_frame_rate=30)) as prepared:
        assert (prepared.width, prepared.height, prepared.frame_rate) == (360, 640, 30)
    assert original.read_bytes() == before
    assert len(calls) == 1
    arguments = calls[0]
    assert arguments[arguments.index("-c:v") + 1] == "libx264"
    assert arguments[arguments.index("-pix_fmt") + 1] == "yuv420p"
    assert arguments[arguments.index("-c:a") + 1] == "aac"
    assert arguments[arguments.index("-ac") + 1] == "2"
    assert "scale=360:640:flags=lanczos,setsar=1,fps=30" in arguments
    assert "-t" not in arguments and "-shortest" not in arguments


def test_oversized_encode_retries_with_lower_bitrate_and_verified_size(tmp_path, monkeypatch):
    original = _mp4(tmp_path / "oversized.mp4", size=1_000_000)
    calls = _mock_conversion(monkeypatch, original, _info(), [_info(), _info()], [240_000, 160_000])
    with prepare_video(original, limits=VideoLimits(max_bytes=200_000)) as prepared:
        assert prepared.size_bytes == 160_000
    assert len(calls) == 2
    rates = [int(arguments[arguments.index("-b:v") + 1]) for arguments in calls]
    assert rates[1] < rates[0]


def test_minimum_frame_rate_and_unknown_source_bitrate_force_verified_conversion(tmp_path, monkeypatch):
    original = _mp4(tmp_path / "slow.mp4")
    source = _info(frame_rate=10, video_bitrate=None)
    output = _info(frame_rate=23, video_bitrate=170_000)
    calls = _mock_conversion(monkeypatch, original, source, [output], [800])
    limits = VideoLimits(max_bytes=1_000_000, min_frame_rate=23, max_frame_rate=60, max_video_bitrate=200_000)
    with prepare_video(original, limits=limits) as prepared:
        assert prepared.frame_rate == 23
    arguments = calls[0]
    assert "scale=640:480:flags=lanczos,setsar=1,fps=23" in arguments
    assert int(arguments[arguments.index("-maxrate") + 1]) <= 180_000


@pytest.mark.parametrize("audio_changes", [{"audio_sample_rate": 96_000}, {"audio_bitrate": 192_000}, {"audio_bitrate": None}])
def test_audio_outside_service_bounds_is_converted_instead_of_passed_through(tmp_path, monkeypatch, audio_changes):
    original = _mp4(tmp_path / "high-audio.mp4")
    calls = _mock_conversion(monkeypatch, original, _info(**audio_changes), [_info()], [800])
    with prepare_video(original, limits=VideoLimits(max_bytes=1_000_000)) as prepared:
        assert prepared.path != original.resolve()
    arguments = calls[0]
    assert arguments[arguments.index("-ar") + 1] == "48000"
    assert int(arguments[arguments.index("-b:a") + 1]) <= 128_000


@pytest.mark.parametrize("source_changes", [{"audio_profile": "HE-AAC"}, {"audio_profile": ""}, {"field_order": "tt"}, {"field_order": "unknown"}])
def test_audio_profile_and_interlacing_require_conversion(tmp_path, monkeypatch, source_changes):
    original = _mp4(tmp_path / "incompatible.mp4")
    calls = _mock_conversion(monkeypatch, original, _info(**source_changes), [_info()], [800])
    with prepare_video(original, limits=VideoLimits(max_bytes=1_000_000)) as prepared:
        assert prepared.path != original.resolve()
    assert len(calls) == 1
    arguments = calls[0]
    assert "copy" not in arguments
    assert arguments[arguments.index("-profile:a") + 1] == "aac_low"
    if "field_order" in source_changes:
        assert "bwdif=mode=send_frame:parity=auto:deint=" in arguments[arguments.index("-vf") + 1]


@pytest.mark.parametrize("output_changes", [{"audio_profile": "HE-AAC"}, {"field_order": "tt"}])
def test_encoder_profile_and_field_order_are_verified_before_upload(tmp_path, monkeypatch, output_changes):
    original = _mp4(tmp_path / "hevc.mp4")
    _mock_conversion(monkeypatch, original, _info(codec="hevc"), [_info(**output_changes)], [800])
    with pytest.raises(ValueError, match="compatible H.264/AAC MP4"):
        with prepare_video(original, limits=VideoLimits(max_bytes=1_000_000)):
            pytest.fail("An incompatible encoded profile must not reach publishing")


@pytest.mark.parametrize("width,height,expected", [(1000, 2, (50, 2)), (2, 1000, (2, 50)), (640, 480, (10, 8))])
def test_even_dimensions_respect_pixel_limit_for_extreme_aspect_ratios(width, height, expected):
    dimensions = videos._bounded_dimensions(_info(width=width, height=height), VideoLimits(max_bytes=1_000_000, max_pixels=100))
    assert dimensions == expected
    assert dimensions[0] * dimensions[1] <= 100
    assert dimensions[0] % 2 == dimensions[1] % 2 == 0


def test_size_retries_are_bounded_and_clean_up_failed_output(tmp_path, monkeypatch):
    original = _mp4(tmp_path / "oversized.mp4", size=1_000_000)
    calls = _mock_conversion(monkeypatch, original, _info(), [_info()], [240_000])
    with pytest.raises(ValueError, match="200,000-byte upload limit"):
        with prepare_video(original, limits=VideoLimits(max_bytes=200_000)):
            pytest.fail("Unverified oversized media must never be uploaded")
    assert len(calls) == 4
    assert not Path(calls[-1][-1]).exists()
    assert original.exists()


def test_overlong_video_is_rejected_before_any_encode_and_never_trimmed(tmp_path, monkeypatch):
    original = _mp4(tmp_path / "long.mp4")
    monkeypatch.setattr(videos, "_probe", lambda path: _info(duration=601))
    monkeypatch.setattr(videos, "_tool", lambda name: pytest.fail("Overlong media must fail before conversion"))
    with pytest.raises(ValueError, match="at most 600 seconds"):
        with prepare_video(original, limits=VideoLimits(max_bytes=300_000_000, max_duration=600)):
            pytest.fail("Overlong media must not reach publishing")


def test_encoder_output_codec_is_verified_before_yield(tmp_path, monkeypatch):
    original = _mp4(tmp_path / "hevc.mp4")
    calls = _mock_conversion(monkeypatch, original, _info(codec="hevc"), [_info(codec="hevc")], [200])
    with pytest.raises(ValueError, match="compatible H.264/AAC MP4"):
        with prepare_video(original, limits=VideoLimits(max_bytes=1_000_000)):
            pytest.fail("Incorrect encoder output must not reach publishing")
    assert len(calls) == 1


def test_probe_reads_actual_rotation_duration_codecs_and_sample_aspect(tmp_path, monkeypatch):
    payload = {
        "streams": [
            {"index": 0, "codec_type": "video", "codec_name": "mjpeg", "width": 64, "height": 64, "disposition": {"attached_pic": 1}},
            {"index": 1, "codec_type": "video", "codec_name": "hevc", "width": 1920, "height": 1080, "pix_fmt": "yuv420p10le", "field_order": "tt", "avg_frame_rate": "60000/1001", "sample_aspect_ratio": "1:1", "duration": "9.9", "side_data_list": [{"side_data_type": "Display Matrix", "rotation": -90}]},
            {"index": 2, "codec_type": "audio", "codec_name": "aac", "profile": "HE-AAC", "channels": 2, "duration": "10.0"},
        ],
        "format": {"duration": "10.0"},
    }
    captured = {}

    def fake_run(arguments, **kwargs):
        captured.update(arguments=arguments, **kwargs)
        return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(videos.shutil, "which", lambda name: name)
    monkeypatch.setattr(videos.subprocess, "run", fake_run)
    info = videos._probe(tmp_path / "video.upload")
    assert (info.width, info.height, info.duration, info.codec, info.video_index) == (1080, 1920, 10, "hevc", 1)
    assert info.frame_rate == pytest.approx(59.94005994)
    assert info.has_display_transform
    assert info.field_order == "tt"
    assert info.audio_profile == "HE-AAC"
    assert captured["timeout"] == 30
    assert isinstance(captured["arguments"], list)
    assert "shell" not in captured


@pytest.mark.parametrize("raw", ["not json", json.dumps({"streams": [], "format": {}})])
def test_invalid_probe_media_has_actionable_error(tmp_path, monkeypatch, raw):
    monkeypatch.setattr(videos, "_run", lambda *args, **kwargs: raw)
    monkeypatch.setattr(videos, "_tool", lambda name: name)
    with pytest.raises(ValueError, match="not a readable video"):
        videos._probe(tmp_path / "invalid.mp4")


def test_missing_tools_fail_before_attempting_upload(tmp_path, monkeypatch):
    original = _mp4(tmp_path / "original.mp4")
    monkeypatch.setattr(videos.shutil, "which", lambda name: None)
    with pytest.raises(ValueError, match="Install FFmpeg.*PATH"):
        with prepare_video(original, limits=VideoLimits(max_bytes=1_000)):
            pytest.fail("Missing video tools must not reach publishing")


def test_probe_timeout_is_actionable(tmp_path, monkeypatch):
    monkeypatch.setattr(videos.shutil, "which", lambda name: name)

    def timed_out(*args, **kwargs):
        raise subprocess.TimeoutExpired("ffprobe", 30)

    monkeypatch.setattr(videos.subprocess, "run", timed_out)
    with pytest.raises(ValueError, match="timed out while reading video metadata"):
        videos._probe(tmp_path / "clip.mp4")


@pytest.mark.parametrize("limits", [VideoLimits(0), VideoLimits(1_000, max_duration=float("nan")), VideoLimits(1_000, max_dimension=1)])
def test_invalid_video_limits_fail_before_probe(tmp_path, limits):
    with pytest.raises(ValueError, match="Video"):
        with prepare_video(tmp_path / "missing.mp4", limits=limits):
            pytest.fail("Invalid limits must not reach publishing")


@pytest.mark.skipif(not shutil.which("ffmpeg") or not shutil.which("ffprobe"), reason="FFmpeg tools are unavailable")
def test_real_ffmpeg_converts_rotated_mov_pcm_audio_and_preserves_original(tmp_path):
    ffmpeg = shutil.which("ffmpeg")
    raw_source = tmp_path / "raw.mov"
    original = tmp_path / "rotated.upload"
    subprocess.run(
        [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=30", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "0.5", "-c:v", "mpeg4", "-q:v", "4", "-c:a", "pcm_s16le", str(raw_source)],
        check=True, capture_output=True, timeout=30,
    )
    subprocess.run(
        [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-display_rotation", "90", "-i", str(raw_source), "-c", "copy", "-f", "mov", str(original)],
        check=True, capture_output=True, timeout=30,
    )
    before = hashlib.sha256(original.read_bytes()).hexdigest()
    source_info = videos._probe(original)
    assert (source_info.width, source_info.height) == (240, 320)
    with prepare_video(original, limits=VideoLimits(max_bytes=50_000, max_duration=2, max_dimension=160, max_frame_rate=15)) as prepared:
        target = prepared.path
        actual = videos._probe(target)
        assert actual.codec == "h264"
        assert actual.pixel_format == "yuv420p"
        assert actual.audio_codec == "aac"
        assert actual.audio_channels == 2
        assert (prepared.width, prepared.height) == (120, 160)
        assert prepared.frame_rate <= 15
        assert 0 < prepared.size_bytes <= 50_000
        assert videos._mp4_layout(target) == (True, True)
    assert not target.exists()
    assert hashlib.sha256(original.read_bytes()).hexdigest() == before


@pytest.mark.skipif(not shutil.which("ffmpeg") or not shutil.which("ffprobe"), reason="FFmpeg tools are unavailable")
def test_real_interlaced_h264_is_deinterlaced_to_progressive_frames(tmp_path):
    original = tmp_path / "interlaced.mp4"
    subprocess.run(
        [shutil.which("ffmpeg"), "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=30", "-t", "0.5", "-vf", "tinterlace=mode=interleave_top", "-c:v", "libx264", "-flags", "+ilme+ildct", "-x264-params", "tff=1", "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(original)],
        check=True, capture_output=True, timeout=30,
    )
    assert videos._probe(original).field_order == "tt"
    before = hashlib.sha256(original.read_bytes()).hexdigest()
    with prepare_video(original, limits=VideoLimits(max_bytes=100_000, max_duration=2)) as prepared:
        assert prepared.path != original.resolve()
        assert videos._probe(prepared.path).field_order == "progressive"
        frames = subprocess.run(
            [shutil.which("ffprobe"), "-v", "error", "-select_streams", "v:0", "-show_frames", "-show_entries", "frame=interlaced_frame", "-of", "json", str(prepared.path)],
            check=True, capture_output=True, text=True, timeout=30,
        )
        assert json.loads(frames.stdout)["frames"]
        assert all(frame["interlaced_frame"] == 0 for frame in json.loads(frames.stdout)["frames"])
    assert hashlib.sha256(original.read_bytes()).hexdigest() == before
