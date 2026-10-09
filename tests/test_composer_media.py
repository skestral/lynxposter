from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest

from app.adapters import service_composer_constraints_context


@pytest.mark.parametrize("template", ["scheduled_post_create.html", "scheduled_post_detail.html"])
@pytest.mark.parametrize(
    ("mime_type", "size", "count", "tone", "notice"),
    [
        ("image/png", 2_000_000, 10, "ok", None),
        ("image/png", 2_000_001, 10, "ok", "10 original images exceed 2 MB"),
        ("image/png", 3_000_000, 11, "danger", "11 original images exceed 2 MB"),
        ("video/quicktime", 300_000_000, 1, "ok", None),
        ("video/quicktime", 300_000_001, 1, "ok", "1 original video exceeds 300 MB"),
    ],
)
def test_composer_limits_and_preparation_guidance_for_original_media(template, mime_type, size, count, tone, notice):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to execute browser guidance.")
    source = Path("app/templates", template).read_text(encoding="utf-8")
    # Execute the actual UI snapshot, summary and validation code, without a DOM.
    functions = source.split("function attachmentSnapshot", 1)[1].split("function describeAttachmentSnapshot", 1)[0]
    functions = "function attachmentSnapshot" + functions
    functions += "function describeAttachmentSnapshot(snapshot) { return String(snapshot.total); }\n"
    functions += "function describeServiceGuidance" + source.split("function describeServiceGuidance", 1)[1].split("function renderGuidanceLines", 1)[0]
    helpers = "\n".join(Path("app/static", name).read_text(encoding="utf-8") for name in ["composer_text.js", "composer_media.js"])
    item = {"type": mime_type, "size": size} if template.endswith("create.html") else {"mime_type": mime_type, "size_bytes": size}
    script = "globalThis.window = {};\n" + helpers + "\n" + functions + "\n"
    script += f"const rule = {json.dumps(service_composer_constraints_context()['bluesky'])};\n"
    script += f"const snapshot = attachmentSnapshot({json.dumps([item] * count)});\n"
    script += "process.stdout.write(JSON.stringify({guidance: evaluateServiceGuidance(rule, 'Caption', snapshot), summary: describeServiceGuidance(rule)}));"
    result = subprocess.run([node, "-"], input=script, text=True, capture_output=True, check=True, timeout=15)
    rendered = json.loads(result.stdout)

    assert rendered["guidance"]["tone"] == tone
    assert "up to 10 images or 1 video" in rendered["summary"]
    assert "2 MB per prepared image" in rendered["summary"]
    assert "300 MB per prepared video" in rendered["summary"]
    assert "10 min video maximum" in rendered["summary"]
    lines = "\n".join(rendered["guidance"]["lines"])
    assert "2 MB each" in lines
    assert "300 MB and 600 seconds" in lines
    if notice:
        assert notice in lines
        assert "originals are preserved" in lines
    else:
        assert "original image" not in lines and "original video" not in lines


def test_media_size_display_uses_decimal_units_matching_upload_limits():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to execute browser guidance.")
    helper = Path("app/static/composer_media.js").read_text(encoding="utf-8")
    script = "globalThis.window = {};\n" + helper + "\n"
    script += "process.stdout.write(JSON.stringify([1024, 2000000, 300000000].map(window.lynxposterComposerMedia.fileSize)));"
    result = subprocess.run([node, "-"], input=script, text=True, capture_output=True, check=True, timeout=15)
    assert json.loads(result.stdout) == ["1.02 KB", "2 MB", "300 MB"]
