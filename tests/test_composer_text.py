from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest

from app.adapters import service_composer_constraints_context


@pytest.mark.parametrize("template", ["scheduled_post_create.html", "scheduled_post_detail.html"])
@pytest.mark.parametrize(
    ("caption", "images", "tone"),
    [
        ("e\u0301" * 300, 0, "ok"),
        ("🧑🏽‍🎨" * 100, 0, "ok"),
        ("e\u0301" * 301, 0, "danger"),
        ("👨‍👩‍👧‍👦" * 121, 0, "danger"),
        ("Gallery", 10, "ok"),
        ("Gallery", 11, "danger"),
    ],
)
def test_bluesky_composer_guidance_matches_unicode_and_gallery_limits(template, caption, images, tone):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to execute browser guidance.")
    source = Path("app/templates", template).read_text(encoding="utf-8")
    evaluator = "function evaluateServiceGuidance" + source.split("function evaluateServiceGuidance", 1)[1].split(
        "function renderGuidanceLines", 1
    )[0]
    helper = "\n".join(Path("app/static", name).read_text(encoding="utf-8") for name in ["composer_text.js", "composer_media.js"])
    rule = service_composer_constraints_context()["bluesky"]
    snapshot = {"total": images, "images": images, "videos": 0, "others": 0, "imageMimeTypes": ["image/jpeg"] if images else []}
    script = (
        "globalThis.window = {};\n" + helper + "\n"
        "function describeAttachmentSnapshot(snapshot) { return String(snapshot.total); }\n"
        + evaluator + "\n"
        + "process.stdout.write(JSON.stringify(evaluateServiceGuidance("
        + ",".join(json.dumps(value) for value in [rule, caption, snapshot]) + ")));"
    )
    result = subprocess.run([node, "-"], input=script, text=True, capture_output=True, timeout=15, check=True)

    guidance = json.loads(result.stdout)
    assert guidance["tone"] == tone, guidance["lines"]


def test_older_browser_defers_grapheme_validation_without_false_rejection():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to execute browser guidance.")
    helper = Path("app/static/composer_text.js").read_text(encoding="utf-8")
    script = "globalThis.window = {}; Intl.Segmenter = undefined;\n" + helper + "\n" + (
        "process.stdout.write(JSON.stringify({"
        "length: window.lynxposterComposerText.length('e\\u0301', 'graphemes'),"
        "bytes: window.lynxposterComposerText.utf8Bytes('e\\u0301'),"
        "legacyLength: window.lynxposterComposerText.length('abc', 'utf16')}));"
    )
    result = subprocess.run([node, "-"], input=script, text=True, capture_output=True, timeout=15, check=True)

    assert json.loads(result.stdout) == {"length": None, "bytes": 3, "legacyLength": 3}
