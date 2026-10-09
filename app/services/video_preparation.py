from __future__ import annotations

import json
import math
import shutil
import struct
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator


@dataclass(frozen=True, slots=True)
class VideoLimits:
    max_bytes: int
    max_duration: float | None = None
    max_dimension: int | None = None
    max_pixels: int | None = None
    max_frame_rate: float | None = None
    min_frame_rate: float | None = None
    max_video_bitrate: int | None = None


@dataclass(frozen=True, slots=True)
class PreparedVideo:
    path: Path
    mime_type: str
    width: int
    height: int
    duration: float
    size_bytes: int
    frame_rate: float


@dataclass(frozen=True, slots=True)
class _VideoInfo:
    width: int
    height: int
    duration: float
    frame_rate: float
    codec: str
    pixel_format: str
    field_order: str
    audio_codec: str | None
    audio_profile: str | None
    audio_channels: int
    audio_sample_rate: int
    audio_bitrate: float | None
    video_bitrate: float | None
    video_index: int
    rotation: float
    square_pixels: bool
    has_display_transform: bool


def _tool(name: str) -> str:
    executable = shutil.which(name)
    if not executable:
        raise ValueError(f"Video preparation requires {name}. Install FFmpeg (including ffprobe) and add its binaries to PATH.")
    return executable


def _run(arguments: list[str], *, timeout: float, action: str) -> str:
    try:
        result = subprocess.run(arguments, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, check=False)
    except OSError as exc:
        raise ValueError("Video preparation requires working FFmpeg and ffprobe binaries on PATH.") from exc
    except subprocess.TimeoutExpired as exc:
        raise ValueError(f"Video preparation timed out while {action}.") from exc
    if result.returncode:
        detail = (result.stderr or "").strip()[-800:]
        raise ValueError(f"Could not prepare this video while {action}. {detail}".strip())
    return result.stdout


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (ValueError, TypeError):
        return None
    return result if math.isfinite(result) else None


def _ratio(value: Any, *, separator: str = "/") -> float | None:
    try:
        numerator, denominator = str(value).split(separator)
        result = float(numerator) / float(denominator)
    except (ValueError, ZeroDivisionError):
        return None
    return result if math.isfinite(result) and result > 0 else None


def _probe(path: Path) -> _VideoInfo:
    raw = _run(
        [_tool("ffprobe"), "-v", "error", "-protocol_whitelist", "file,pipe", "-show_streams", "-show_format", "-of", "json", str(path)],
        timeout=30,
        action="reading video metadata",
    )
    try:
        payload = json.loads(raw)
        streams = payload.get("streams") or []
        video = next(stream for stream in streams if stream.get("codec_type") == "video" and not (stream.get("disposition") or {}).get("attached_pic"))
        audio = next((stream for stream in streams if stream.get("codec_type") == "audio"), None)
        width, height = int(video["width"]), int(video["height"])
        durations = [
            value for value in [_number((payload.get("format") or {}).get("duration")), _number(video.get("duration")), _number((audio or {}).get("duration"))]
            if value is not None and value > 0
        ]
        frame_rate = _ratio(video.get("avg_frame_rate")) or _ratio(video.get("r_frame_rate"))
        if width <= 0 or height <= 0 or not durations or not frame_rate:
            raise ValueError("Missing dimensions, duration, or frame rate.")
        rotation = _number((video.get("tags") or {}).get("rotate")) or 0.0
        has_display_transform = False
        for side_data in video.get("side_data_list") or []:
            if side_data.get("side_data_type") == "Display Matrix":
                has_display_transform = True
            if _number(side_data.get("rotation")) is not None:
                rotation = float(side_data["rotation"])
        sample_aspect = _ratio(video.get("sample_aspect_ratio"), separator=":") or 1.0
        displayed_width = width * sample_aspect
        angle = math.radians(rotation % 360)
        oriented_width = max(1, round(abs(displayed_width * math.cos(angle)) + abs(height * math.sin(angle))))
        oriented_height = max(1, round(abs(displayed_width * math.sin(angle)) + abs(height * math.cos(angle))))
        return _VideoInfo(
            width=oriented_width,
            height=oriented_height,
            duration=max(durations),
            frame_rate=frame_rate,
            codec=str(video.get("codec_name") or ""),
            pixel_format=str(video.get("pix_fmt") or ""),
            field_order=str(video.get("field_order") or "unknown"),
            audio_codec=str(audio.get("codec_name") or "") if audio else None,
            audio_profile=str(audio.get("profile") or "") if audio else None,
            audio_channels=int((audio or {}).get("channels") or 0),
            audio_sample_rate=int((audio or {}).get("sample_rate") or 0),
            audio_bitrate=_number((audio or {}).get("bit_rate")),
            video_bitrate=_number(video.get("bit_rate")),
            video_index=int(video.get("index") or 0),
            rotation=rotation,
            square_pixels=math.isclose(sample_aspect, 1.0),
            has_display_transform=has_display_transform,
        )
    except (ValueError, TypeError, KeyError, StopIteration, AttributeError) as exc:
        raise ValueError("This attachment is not a readable video with valid dimensions, duration, and frame rate.") from exc


def _mp4_layout(path: Path) -> tuple[bool, bool]:
    """Inspect MP4 box order without loading its video payload into memory."""
    is_mp4 = False
    moov_before_media = False
    media_seen = False
    file_size = path.stat().st_size
    with path.open("rb") as handle:
        while handle.tell() + 8 <= file_size:
            offset = handle.tell()
            size, box_type = struct.unpack(">I4s", handle.read(8))
            header_size = 8
            if size == 1:
                extended_size = handle.read(8)
                if len(extended_size) != 8:
                    return False, False
                size = struct.unpack(">Q", extended_size)[0]
                header_size = 16
            elif size == 0:
                size = file_size - offset
            if size < header_size or offset + size > file_size:
                return False, False
            if box_type == b"ftyp":
                brands = handle.read(min(size - header_size, 4096))
                major_brand = brands[:4]
                is_mp4 = major_brand in {b"isom", b"mp41", b"mp42", b"avc1", b"M4V ", b"MSNV", b"dash"} or (
                    major_brand.startswith(b"iso") and major_brand[3:4].isdigit()
                )
            elif box_type in {b"mdat", b"moof"}:
                media_seen = True
            elif box_type == b"moov":
                moov_before_media = not media_seen
            handle.seek(offset + size)
    return is_mp4, moov_before_media


def _bounded_dimensions(info: _VideoInfo, limits: VideoLimits) -> tuple[int, int]:
    scale = 1.0
    if limits.max_dimension:
        scale = min(scale, limits.max_dimension / max(info.width, info.height))
    if limits.max_pixels:
        scale = min(scale, math.sqrt(limits.max_pixels / (info.width * info.height)))
    width, height = max(2, math.floor(info.width * scale / 2) * 2), max(2, math.floor(info.height * scale / 2) * 2)
    if limits.max_pixels is not None and width * height > limits.max_pixels:
        # The minimum two-pixel minor axis can defeat the proportional scale for
        # extreme panoramas. Bound the other axis after even-pixel rounding.
        if width >= height:
            width = max(2, min(width, limits.max_pixels // height // 2 * 2))
        else:
            height = max(2, min(height, limits.max_pixels // width // 2 * 2))
    return width, height


def _compatible(info: _VideoInfo) -> bool:
    return (
        info.codec == "h264"
        and info.pixel_format == "yuv420p"
        and info.field_order == "progressive"
        and (info.audio_codec is None or (
            info.audio_codec == "aac" and 0 < info.audio_channels <= 2
            and info.audio_profile == "LC"
            and 0 < info.audio_sample_rate <= 48_000
            and info.audio_bitrate is not None and 0 < info.audio_bitrate <= 128_000
        ))
        and info.square_pixels
        and not info.has_display_transform
        and math.isclose(info.rotation % 360, 0.0)
        and info.width % 2 == 0
        and info.height % 2 == 0
    )


def _within_limits(info: _VideoInfo, size: int, limits: VideoLimits) -> bool:
    return (
        0 < size <= limits.max_bytes
        and (limits.max_duration is None or info.duration <= limits.max_duration)
        and (limits.max_dimension is None or max(info.width, info.height) <= limits.max_dimension)
        and (limits.max_pixels is None or info.width * info.height <= limits.max_pixels)
        and (limits.max_frame_rate is None or info.frame_rate <= limits.max_frame_rate + 0.001)
        and (limits.min_frame_rate is None or info.frame_rate >= limits.min_frame_rate - 0.001)
        and (limits.max_video_bitrate is None or (info.video_bitrate is not None and 0 < info.video_bitrate <= limits.max_video_bitrate))
    )


def _prepared(path: Path, info: _VideoInfo) -> PreparedVideo:
    return PreparedVideo(path, "video/mp4", info.width, info.height, info.duration, path.stat().st_size, info.frame_rate)


@contextmanager
def prepare_video(path: Path, *, limits: VideoLimits) -> Iterator[PreparedVideo]:
    """Prepare a destination MP4 without modifying or trimming the original file."""
    optional_limits = (limits.max_duration, limits.max_dimension, limits.max_pixels, limits.max_frame_rate, limits.min_frame_rate, limits.max_video_bitrate)
    if limits.max_bytes <= 0 or any(value is not None and (not math.isfinite(value) or value <= 0) for value in optional_limits):
        raise ValueError("Video upload limits must be positive finite values.")
    if (limits.max_dimension is not None and limits.max_dimension < 2) or (limits.max_pixels is not None and limits.max_pixels < 4):
        raise ValueError("Video dimensions must allow at least a 2 by 2 pixel frame.")
    if limits.min_frame_rate is not None and limits.max_frame_rate is not None and limits.min_frame_rate > limits.max_frame_rate:
        raise ValueError("Minimum video frame rate cannot exceed maximum video frame rate.")
    path = Path(path).resolve()
    try:
        size_bytes = path.stat().st_size
    except OSError as exc:
        raise ValueError("The original video file is missing or unreadable.") from exc
    info = _probe(path)
    if limits.max_duration is not None and info.duration > limits.max_duration:
        raise ValueError(f"Video duration is {info.duration:g} seconds; this destination allows at most {limits.max_duration:g} seconds. Shorten the video before publishing.")
    is_mp4, fast_start = _mp4_layout(path)
    if path.suffix.lower() == ".mp4" and is_mp4 and fast_start and _compatible(info) and _within_limits(info, size_bytes, limits):
        yield _prepared(path, info)
        return

    executable = _tool("ffmpeg")
    dimensions = _bounded_dimensions(info, limits)
    timeout = max(120.0, min(1800.0, info.duration * 12 + 60))
    base = [executable, "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-protocol_whitelist", "file,pipe", "-i", str(path), "-map", f"0:{info.video_index}", "-map", "0:a:0?", "-map_metadata", "-1", "-map_chapters", "-1"]

    with TemporaryDirectory(prefix="lynxposter-video-") as temp_dir:
        target = Path(temp_dir) / "video.mp4"
        if _compatible(info) and dimensions == (info.width, info.height) and _within_limits(info, size_bytes, limits):
            _run(base + ["-c", "copy", "-movflags", "+faststart", str(target)], timeout=timeout, action="making a streamable MP4")
            remuxed = _probe(target)
            if _compatible(remuxed) and _within_limits(remuxed, target.stat().st_size, limits) and all(_mp4_layout(target)):
                yield _prepared(target, remuxed)
                return

        total_bitrate = int(limits.max_bytes * 8 * 0.85 / info.duration)
        audio_bitrate = min(96_000, max(16_000, total_bitrate // 6)) if info.audio_codec else 0
        video_bitrate = min(12_000_000, total_bitrate - audio_bitrate)
        if limits.max_video_bitrate is not None:
            video_bitrate = min(video_bitrate, int(limits.max_video_bitrate * 0.9))
        if video_bitrate < 1_000:
            raise ValueError(f"This video's duration cannot fit the destination's {limits.max_bytes:,}-byte upload limit without trimming.")
        filters = []
        if info.field_order != "progressive":
            deinterlace = "all" if info.field_order in {"tt", "bb", "tb", "bt"} else "interlaced"
            filters.append(f"bwdif=mode=send_frame:parity=auto:deint={deinterlace}")
        filters += [f"scale={dimensions[0]}:{dimensions[1]}:flags=lanczos", "setsar=1"]
        target_frame_rate = info.frame_rate
        if limits.min_frame_rate is not None:
            target_frame_rate = max(target_frame_rate, limits.min_frame_rate)
        if limits.max_frame_rate is not None:
            target_frame_rate = min(target_frame_rate, limits.max_frame_rate)
        if not math.isclose(target_frame_rate, info.frame_rate):
            filters.append(f"fps={target_frame_rate:g}")

        for _ in range(4):
            arguments = base + [
                "-vf", ",".join(filters), "-c:v", "libx264", "-preset", "medium", "-pix_fmt", "yuv420p", "-tag:v", "avc1",
                "-b:v", str(video_bitrate), "-maxrate", str(video_bitrate), "-bufsize", str(video_bitrate * 2),
                "-metadata:s:v:0", "rotate=0", "-movflags", "+faststart",
            ]
            if info.audio_codec:
                arguments += ["-c:a", "aac", "-profile:a", "aac_low", "-b:a", str(audio_bitrate), "-ac", "2", "-ar", "48000"]
            else:
                arguments += ["-an"]
            _run(arguments + [str(target)], timeout=timeout, action="converting video to H.264 MP4")
            encoded = _probe(target)
            output_size = target.stat().st_size
            if not _compatible(encoded) or not all(_mp4_layout(target)):
                raise ValueError("Video conversion did not produce a compatible H.264/AAC MP4.")
            if _within_limits(encoded, output_size, limits):
                yield _prepared(target, encoded)
                return
            if output_size <= limits.max_bytes:
                raise ValueError("Video conversion did not satisfy the destination's duration, dimensions, or frame rate limits.")
            reduction = min(0.75, limits.max_bytes / output_size * 0.85)
            video_bitrate = max(1_000, int(video_bitrate * reduction))
            if audio_bitrate:
                audio_bitrate = max(16_000, int(audio_bitrate * reduction))
        raise ValueError(f"Could not prepare this video within the destination's {limits.max_bytes:,}-byte upload limit.")
