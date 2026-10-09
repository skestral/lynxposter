from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


def _run_sandbox_script(assertions: str) -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to exercise sandbox JavaScript.")
    template = (Path(__file__).parents[1] / "app/templates/sandbox.html").read_text(encoding="utf-8")
    functions = "function escapeHtml(value)" + template.split("function escapeHtml(value)", 1)[1].split("async function runSandboxPreview()", 1)[0]
    result = subprocess.run([node, "-e", functions + assertions], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_sandbox_body_edits_keep_seeded_media_overrides_and_all_attachment_ids():
    _run_sandbox_script(r'''
const assert = require("node:assert/strict");
const sandboxSeed = {publish_overrides_json: {
  selected: {body: "Saved body", attachment_ids: ["saved-9", "saved-4"]},
  cleared: {body: "Clear this", attachment_ids: []},
  textOnly: {body: "Clear only body"},
  legacy: "Legacy body",
  bluesky: {attachment_ids: ["saved-1"]},
}};
const persistedAttachments = Array.from({length: 10}, (_, index) => ({
  id: `saved-${index}`, filename: `photo-${index}.jpg`, mime_type: "image/jpeg", sort_order: index,
}));
const uploadsInput = {files: [{name: "new.jpg", type: "image/jpeg", size: 100}]};
const textareas = [
  {dataset: {accountId: "selected"}, value: "Edited body"},
  {dataset: {accountId: "cleared"}, value: ""},
  {dataset: {accountId: "textOnly"}, value: ""},
  {dataset: {accountId: "legacy"}, value: "Edited legacy"},
];
const form = {
  persona_id: {value: "persona"}, body: {value: "Common body"}, source_link: {value: ""}, visibility: {value: ""},
  expected_target_count: {value: ""}, body_must_contain: {value: ""}, body_must_not_contain: {value: ""},
  max_body_length: {value: ""}, expected_attachment_count: {value: ""}, expected_visibility: {value: ""},
  require_media: {value: ""}, require_source_link_in_payload: {value: ""},
  querySelectorAll(selector) {
    return selector === ".sandbox-override-body" ? textareas : [{checked: true, value: "selected"}];
  },
};
const payload = buildPayload();
assert.deepEqual(payload.publish_overrides_json.selected, {body: "Edited body", attachment_ids: ["saved-9", "saved-4"]});
assert.deepEqual(payload.publish_overrides_json.cleared, {attachment_ids: []});
assert.equal(payload.publish_overrides_json.textOnly, undefined);
assert.deepEqual(payload.publish_overrides_json.legacy, {body: "Edited legacy"});
assert.deepEqual(payload.publish_overrides_json.bluesky, {attachment_ids: ["saved-1"]});
assert.equal(sandboxSeed.publish_overrides_json.selected.body, "Saved body");
assert.equal(sandboxSeed.publish_overrides_json.cleared.body, "Clear this");
assert.equal(payload.attachment_inputs.length, 11);
assert.deepEqual(payload.attachment_inputs.slice(0, 10).map(item => item.id), persistedAttachments.map(item => item.id));
assert.equal(payload.attachment_inputs[10].filename, "new.jpg");
assert.equal(payload.attachment_inputs[10].sort_order, 10);
''')


def test_sandbox_results_show_selected_media_counts_without_live_readiness_claim():
    _run_sandbox_script(r'''
const assert = require("node:assert/strict");
const resultBadge = {}, summaryEl = {}, resultsEl = {};
renderPreviewResults({
  overall_valid: true, overall_expectations_passed: true, persona_name: "Preview persona", target_count: 1,
  attachment_count: 10, generated_at: "2050-01-01", global_checks: [], global_errors: [],
  previews: [{account_label: "Selected destination", service: "bluesky", action: "send_images", publish_ready: true,
    configured: true, body_length: 5, attachment_count: 2, rendered_body: "Hello", validation_issues: [],
    expectation_checks: [], request_shape: {}, notes: ["Media preparation remains pending."]}],
});
assert.equal(resultBadge.textContent, "Preview checks passed");
assert.ok(summaryEl.innerHTML.includes("10 attachments"));
assert.ok(resultsEl.innerHTML.includes("2 attachments"));
assert.ok(resultsEl.innerHTML.includes("Preview checks passed"));
assert.ok(resultsEl.innerHTML.includes("Media preparation remains pending."));
assert.ok(!resultsEl.innerHTML.includes("Publish Ready"));
''')


def test_clear_saved_attachments_resets_media_overrides_before_replacement_preview():
    template = (Path(__file__).parents[1] / "app/templates/sandbox.html").read_text(encoding="utf-8")
    handler_prefix = 'clearSavedAttachmentsButton?.addEventListener("click", () => {'
    handler = handler_prefix + template.split(handler_prefix, 1)[1].split('form?.addEventListener("submit"', 1)[0]
    _run_sandbox_script(r'''
const assert = require("node:assert/strict");
const sandboxSeed = {publish_overrides_json: {
  selected: {body: "Saved body", attachment_ids: ["saved-1"], visibility: "public"},
  emptySelected: {body: "Empty destination", attachment_ids: [], language: "fr"},
  mediaOnly: {attachment_ids: []},
  bluesky: {attachment_ids: ["saved-2"], language: "en"},
  legacy: "Legacy body",
}};
let persistedAttachments = [{id: "saved-1", filename: "saved.jpg", mime_type: "image/jpeg", sort_order: 0}];
const uploadsInput = {files: []};
let clear;
const attachmentList = {};
const clearSavedAttachmentsButton = {
  addEventListener(event, handler) { assert.equal(event, "click"); clear = handler; },
  classList: {add() {}, remove() {}},
};
const form = {
  persona_id: {value: "persona"}, body: {value: "Common body"}, source_link: {value: ""}, visibility: {value: ""},
  expected_target_count: {value: ""}, body_must_contain: {value: ""}, body_must_not_contain: {value: ""},
  max_body_length: {value: ""}, expected_attachment_count: {value: ""}, expected_visibility: {value: ""},
  require_media: {value: ""}, require_source_link_in_payload: {value: ""},
  querySelectorAll(selector) {
    return selector === ".sandbox-override-body"
      ? [{dataset: {accountId: "selected"}, value: "Edited body"}]
      : [{checked: true, value: "selected"}];
  },
};
assert.deepEqual(buildPayload().publish_overrides_json.selected.attachment_ids, ["saved-1"]);
''' + handler + r'''
clear();
assert.deepEqual(persistedAttachments, []);
assert.ok(attachmentList.innerHTML.includes("No media loaded."));
uploadsInput.files = [{name: "replacement.jpg", type: "image/jpeg", size: 1234}];
const payload = buildPayload();
assert.deepEqual(payload.attachment_inputs, [{filename: "replacement.jpg", mime_type: "image/jpeg", alt_text: "",
  size_bytes: 1234, sort_order: 0, storage_path: "sandbox/replacement.jpg"}]);
assert.deepEqual(payload.publish_overrides_json.selected, {body: "Edited body", visibility: "public"});
assert.deepEqual(payload.publish_overrides_json.emptySelected, {body: "Empty destination", language: "fr"});
assert.deepEqual(payload.publish_overrides_json.bluesky, {language: "en"});
assert.equal(payload.publish_overrides_json.mediaOnly, undefined);
assert.equal(payload.publish_overrides_json.legacy, "Legacy body");
assert.deepEqual(payload.target_account_ids, ["selected"]);
for (const override of Object.values(payload.publish_overrides_json)) {
  assert.ok(typeof override !== "object" || !("attachment_ids" in override));
}
''')
