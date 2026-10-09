from __future__ import annotations

import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment


TEMPLATE_PATH = Path(__file__).parents[1] / "app/templates/scheduled_post_detail.html"


def _run_node(source: str) -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to exercise post recovery JavaScript.")
    result = subprocess.run([node, "-e", source], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def _submission_source() -> str:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    marker = "async function submitPostPlan(form, {sendNow = false} = {})"
    return marker + template.split(marker, 1)[1].split('document.querySelectorAll(".giveaway-review-action")', 1)[0]


def test_normal_retry_sends_current_edits_and_uploads_in_one_atomic_request():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    payload_source = "function buildPostPayload(form)" + template.split("function buildPostPayload(form)", 1)[1].split("function pendingUploadAltTexts()", 1)[0]
    _run_node(r'''
const assert = require("node:assert/strict");
const postId = "recovery";
let postPlanSaveInProgress = false, submit, retry, reloads = 0;
const successfulTargetAccountIds = new Set(["posted"]);
const CSS = {escape(value) { return value; }};
const requests = [], alerts = [], buttons = [{disabled: false}, {disabled: false}, {disabled: true}];
const replacement = {name: "replacement.jpg", type: "image/jpeg", size: 321};
const form = {
  body: {value: "Fixed global body"}, post_type: {value: "standard"}, status: {value: "partial"},
  scheduled_for: {value: ""}, source_link: {value: ""}, visibility: {value: "public"}, uploads: {files: [replacement]},
  addEventListener(event, callback) { submit = callback; },
  querySelectorAll(selector) {
    if (selector === ".target-checkbox") return [{checked: true, value: "posted"}, {checked: true, value: "failed"}];
    if (selector === ".override-body") return [
      {dataset: {accountId: "posted"}, value: "Must not edit published body"},
      {dataset: {accountId: "failed"}, value: "Fixed Mastodon body"},
    ];
    if (selector === ".destination-media-custom") return [
      {checked: true, dataset: {accountId: "posted"}}, {checked: true, dataset: {accountId: "failed"}},
    ];
    return [{checked: true, dataset: {attachmentId: "valid-saved"}}];
  },
};
const document = {
  getElementById(id) { return id === "scheduled-post-update-form" ? form : {addEventListener(event, callback) { retry = callback; }}; },
  querySelectorAll() { return buttons; },
};
const FormData = class {
  constructor() { this.fields = new Map(); }
  append(key, value) { this.fields.set(key, [...(this.fields.get(key) || []), value]); }
};
const window = {location: {reload() { reloads++; }}, async readApiMessage(response) { return response.message; }};
function alert(message) { alerts.push(message); }
function activeExistingAttachments() { return [{id: "valid-saved", alt_text: "Fixed alt"}]; }
function activeExistingAttachmentIds() { return new Set(["valid-saved"]); }
function deletedAttachmentIds() { return ["invalid-image"]; }
function buildGiveawayPayload() { return null; }
function pendingUploadAltTexts() { return ["Replacement alt"]; }
let resolveRequest;
async function fetch(url, options) {
  requests.push({url, ...options});
  assert.ok(buttons.every(button => button.disabled));
  return await new Promise(resolve => { resolveRequest = resolve; });
}
''' + payload_source + _submission_source() + r'''
(async () => {
  const inFlight = retry();
  assert.equal(requests.length, 1);
  await submit({preventDefault() {}, currentTarget: form});
  assert.equal(requests.length, 1, "Concurrent save must not submit another request");
  const request = requests[0];
  assert.equal(request.url, "/scheduled-posts/recovery?send_now=true");
  assert.equal(request.method, "PUT");
  const fields = request.body.fields;
  assert.equal(fields.get("body")[0], "Fixed global body");
  assert.deepEqual(JSON.parse(fields.get("publish_overrides_json")[0]), {
    failed: {body: "Fixed Mastodon body", attachment_ids: ["valid-saved"]},
  });
  assert.deepEqual(JSON.parse(fields.get("target_account_ids")[0]), ["posted", "failed"]);
  assert.deepEqual(JSON.parse(fields.get("deleted_attachment_ids")[0]), ["invalid-image"]);
  assert.deepEqual(JSON.parse(fields.get("attachment_alt_texts")[0]), {"valid-saved": "Fixed alt"});
  assert.deepEqual(JSON.parse(fields.get("alt_texts")[0]), ["Replacement alt"]);
  assert.deepEqual(fields.get("uploads"), [replacement]);
  resolveRequest({ok: true});
  await inFlight;
  assert.equal(reloads, 1);
  assert.deepEqual(buttons.map(button => button.disabled), [false, false, true]);
  assert.equal(postPlanSaveInProgress, false);
  assert.deepEqual(alerts, []);
})().catch(error => { console.error(error); process.exit(1); });
''')


def test_retry_error_preserves_edits_and_giveaway_keeps_existing_publish_action():
    _run_node(r'''
const assert = require("node:assert/strict");
const postId = "recovery";
let postPlanSaveInProgress = false, submit, retry, reloads = 0;
const requests = [], alerts = [], buttons = [{disabled: false}, {disabled: false}];
const form = {uploads: {files: [{name: "edited.jpg"}]}, addEventListener(event, callback) { submit = callback; }};
const document = {
  getElementById(id) { return id === "scheduled-post-update-form" ? form : {addEventListener(event, callback) { retry = callback; }}; },
  querySelectorAll() { return buttons; },
};
const payload = {post_type: "standard", target_account_ids: ["posted", "failed"], body: "Edited body", status: "partial",
  scheduled_for: null, publish_overrides_json: {}, metadata_json: {}, attachment_order: [], deleted_attachment_ids: [],
  attachment_alt_texts: {}, giveaway: null};
function buildPostPayload() { return payload; }
function pendingUploadAltTexts() { return ["Edited alt"]; }
const FormData = class {append() {}};
const window = {location: {reload() { reloads++; }}, async readApiMessage(response) { return response.message; }};
function alert(message) { alerts.push(message); }
const giveawayBuilder = {validate() { throw Error("Publish Now must retain the saved giveaway action"); }};
async function fetch(url, options) { requests.push({url, ...options}); return {ok: false, message: "Fix invalid media"}; }
''' + _submission_source() + r'''
(async () => {
  await retry();
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, "/scheduled-posts/recovery?send_now=true");
  assert.deepEqual(alerts, ["Fix invalid media"]);
  assert.equal(reloads, 0);
  assert.equal(payload.body, "Edited body");
  assert.equal(form.uploads.files[0].name, "edited.jpg");
  assert.ok(buttons.every(button => !button.disabled));
  await submit({preventDefault() {}, currentTarget: form});
  assert.equal(requests[1].url, "/scheduled-posts/recovery");
  assert.equal(requests[1].method, "PUT");
  payload.post_type = "giveaway";
  await retry();
  assert.equal(requests[2].url, "/scheduled-posts/recovery/send-now");
  assert.equal(requests[2].method, "POST");
  assert.equal(requests[2].body, undefined);
  assert.equal(postPlanSaveInProgress, false);
})().catch(error => { console.error(error); process.exit(1); });
''')


def test_successful_destination_guidance_skips_current_body_and_media_validation():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    source = "function refreshPostGuidance()" + template.split("function refreshPostGuidance()", 1)[1].split("function buildPostPayload(form)", 1)[0]
    _run_node(source + r'''
const assert = require("node:assert/strict");
const successfulTargetAccountIds = new Set(["posted"]);
const evaluated = [];
function wrapper(id) {
  const fields = {
    ".target-checkbox": {checked: true, value: id},
    ".post-guidance-panel": {classList: {add() {}, remove() {}}},
    ".post-guidance-summary": {}, ".post-guidance-status": {},
    ".override-body": {value: `Body ${id}`}, ".destination-media-custom": {checked: false},
  };
  return {dataset: {accountService: "mastodon"}, fields, querySelector(selector) { return fields[selector]; }};
}
const wrappers = [wrapper("posted"), wrapper("failed")];
const document = {querySelectorAll(selector) {
  return selector === ".target-checkbox" ? wrappers.map(item => item.fields[selector]) : wrappers;
}};
const postStatusSelect = {value: "partial"};
const bodyTextarea = {value: "Edited body"}, bodyLengthIndicator = {}, snapshotBodyLength = {}, snapshotAttachments = {}, snapshotTargets = {};
const uploadsInput = {files: []}, uploadSummary = {}, servicePostGuidance = {mastodon: {}};
function combinedAttachmentSnapshot() { return {total: 1}; }
function fileSnapshot() { return {total: 0}; }
function refreshGiveawayState() {}
function destinationAttachmentSnapshot() { return {total: 1}; }
function describeServiceGuidance() { return "Service guidance"; }
function evaluateServiceGuidance(rule, body) { evaluated.push(body); return {tone: "ok", lines: ["Current edits checked"]}; }
function renderGuidanceLines(container, lines) { container.lines = lines; }
refreshPostGuidance();
assert.deepEqual(evaluated, ["Body failed"]);
assert.ok(wrappers[0].fields[".post-guidance-status"].lines[0].includes("will not be sent again"));
assert.deepEqual(wrappers[1].fields[".post-guidance-status"].lines, ["Current edits checked"]);
''')


def test_successful_destination_body_and_media_render_locked_while_failure_stays_editable():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    source = template.split('<div class="row g-2 mt-2" id="target-account-list">', 1)[1].split('<div class="tab-pane fade" id="detail-giveaway-pane"', 1)[0]
    accounts = [SimpleNamespace(id="posted", service="bluesky", label="Published"), SimpleNamespace(id="failed", service="mastodon", label="Failed")]
    post = SimpleNamespace(
        deliveries={"posted": SimpleNamespace(status="posted"), "failed": SimpleNamespace(status="failed")},
        publish_overrides_json={}, target_account_ids=["posted", "failed"],
        attachments=[SimpleNamespace(id="image", alt_text="Image", mime_type="image/jpeg")],
    )
    html = Environment(autoescape=True).from_string(source).render(accounts=accounts, post=post)

    class ControlParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.fieldset_disabled = False
            self.controls = {}

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == "fieldset":
                self.fieldset_disabled = "disabled" in attrs
            if tag in {"input", "textarea"}:
                account_id = attrs.get("data-account-id")
                if account_id:
                    self.controls.setdefault(account_id, []).append(self.fieldset_disabled or "disabled" in attrs)

        def handle_endtag(self, tag):
            if tag == "fieldset":
                self.fieldset_disabled = False

    parser = ControlParser()
    parser.feed(html)
    assert parser.controls["posted"] == [True, True, True]
    assert parser.controls["failed"] == [False, False, False]
    assert "Published · Locked" in html
    assert "Its body and media are locked." in html


def test_custom_media_replacement_payload_and_snapshot_include_only_explicit_replacements():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    snapshot_source = "function attachmentSnapshot(items)" + template.split("function attachmentSnapshot(items)", 1)[1].split("function escapeHtml(value)", 1)[0]
    payload_source = "function buildPostPayload(form)" + template.split("function buildPostPayload(form)", 1)[1].split("function pendingUploadAltTexts()", 1)[0]
    _run_node(snapshot_source + payload_source + r'''
const assert = require("node:assert/strict");
const successfulTargetAccountIds = new Set();
const CSS = {escape(value) { return value; }};
const existingAttachments = [
  {id: "invalid-image", mime_type: "image/jpeg", size_bytes: 100, deleted: true},
  {id: "valid-image", mime_type: "image/jpeg", size_bytes: 200, deleted: false},
];
const replacement = {name: "replacement.jpg", type: "image/jpeg", size: 300};
const uploadsInput = {files: [replacement]};
const mediaToggle = {checked: true, dataset: {accountId: "failed"}};
const removedSelection = {checked: true, dataset: {attachmentId: "invalid-image"}};
const remainingSelection = {checked: true, dataset: {attachmentId: "valid-image"}};
const selections = [removedSelection, remainingSelection];
const wrapper = {
  querySelector() { return mediaToggle; },
  querySelectorAll() { return selections; },
};
const form = {
  body: {value: "Fixed body"}, post_type: {value: "standard"}, status: {value: "failed"},
  scheduled_for: {value: ""}, source_link: {value: ""}, visibility: {value: ""}, uploads: uploadsInput,
  querySelectorAll(selector) {
    if (selector === ".target-checkbox") return [{checked: true, value: "failed"}];
    if (selector === ".override-body") return [];
    if (selector === ".destination-media-custom") return [mediaToggle];
    return selections;
  },
};
function buildGiveawayPayload() { return null; }
let payload = buildPostPayload(form);
assert.deepEqual(payload.publish_overrides_json.failed.attachment_ids, ["invalid-image", "valid-image"]);
assert.deepEqual(payload.deleted_attachment_ids, ["invalid-image"]);
assert.deepEqual(payload.attachment_order, ["valid-image"]);
let snapshot = destinationAttachmentSnapshot(wrapper);
assert.equal(snapshot.total, 2);
assert.deepEqual(snapshot.imageSizes, [200, 300]);

removedSelection.checked = false;
payload = buildPostPayload(form);
assert.deepEqual(payload.publish_overrides_json.failed.attachment_ids, ["valid-image"]);
snapshot = destinationAttachmentSnapshot(wrapper);
assert.equal(snapshot.total, 1);
assert.deepEqual(snapshot.imageSizes, [200]);

remainingSelection.checked = false;
assert.deepEqual(buildPostPayload(form).publish_overrides_json.failed.attachment_ids, []);
assert.equal(destinationAttachmentSnapshot(wrapper).total, 0, "Explicit no-media selection stays empty");

removedSelection.checked = true;
uploadsInput.files = [];
assert.deepEqual(buildPostPayload(form).publish_overrides_json.failed.attachment_ids, []);
assert.equal(destinationAttachmentSnapshot(wrapper).total, 0, "Removed media alone does not add replacements");

mediaToggle.checked = false;
uploadsInput.files = [replacement];
assert.equal(buildPostPayload(form).publish_overrides_json.failed, undefined);
assert.equal(destinationAttachmentSnapshot(wrapper).total, 2, "Global media keeps new uploads");
''')


@pytest.mark.parametrize("origin_kind", [None, "composer", "account_import"])
def test_imported_post_type_option_is_locked_and_recovery_help_preserves_source_links(origin_kind):
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    marker = '<select class="form-select" name="post_type" id="post-type-select">'
    source = marker + template.split(marker, 1)[1].split("</select>", 1)[0] + "</select>"
    post = SimpleNamespace(post_type="standard")
    if origin_kind is not None:
        post.origin_kind = origin_kind
    html = Environment(autoescape=True).from_string(source).render(post=post)

    class OptionParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.disabled = {}

        def handle_starttag(self, tag, attrs):
            if tag == "option":
                attrs = dict(attrs)
                self.disabled[attrs["value"]] = "disabled" in attrs

    parser = OptionParser()
    parser.feed(html)
    assert parser.disabled["standard"] is False
    assert parser.disabled["giveaway"] is (origin_kind == "account_import")
    help_marker = "{% if post.origin_kind|default('composer') == 'account_import' %}"
    help_source = help_marker + template.split(help_marker, 1)[1].split("{% endif %}", 1)[0] + "{% endif %}"
    help_html = Environment(autoescape=True).from_string(help_source).render(post=post)
    assert ("the original source and published copies stay linked" in help_html) is (origin_kind == "account_import")
