from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


def test_successful_target_lock_survives_guidance_refresh_and_giveaway_payload():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to exercise destination-selection JavaScript.")
    template = (Path(__file__).parents[1] / "app/templates/scheduled_post_detail.html").read_text(encoding="utf-8")
    sync_source = "function syncGiveawayTargets()" + template.split("function syncGiveawayTargets()", 1)[1].split("function refreshGiveawayState()", 1)[0]
    payload_source = "function buildPostPayload(form)" + template.split("function buildPostPayload(form)", 1)[1].split("function pendingUploadAltTexts()", 1)[0]
    script = sync_source + payload_source + r'''
const assert = require("node:assert/strict");
const successfulTargetAccountIds = new Set(["successful"]);
const successfulDestinationReason = "Already published successfully.";
const postTypeSelect = {value: "standard"};
const checkboxes = [
  {value: "successful", checked: false, disabled: false},
  {value: "failed", checked: false, disabled: true},
  {value: "queued", checked: true, disabled: true},
];
const document = {querySelectorAll() { return checkboxes; }};
const giveawayBuilder = {getChannelAccountIds() { return ["failed"]; }};
function channelControl(value) {
  const removeButton = {disabled: false, parentElement: {title: ""}};
  return {
    value, disabled: false, parentElement: {title: ""}, removeButton,
    closest(selector) {
      assert.equal(selector, "div[data-channel-id]");
      return {querySelector() { return removeButton; }};
    },
  };
}
const successfulChannel = channelControl("successful");
const failedChannel = channelControl("failed");
const giveawayBuilderRoot = {querySelectorAll() { return [successfulChannel, failedChannel]; }};
syncGiveawayTargets();
assert.equal(checkboxes[0].checked, true);
assert.equal(checkboxes[0].disabled, true);
assert.equal(checkboxes[1].checked, false);
assert.equal(checkboxes[1].disabled, false);
assert.equal(checkboxes[2].checked, true);
assert.equal(checkboxes[2].disabled, false);
assert.equal(successfulChannel.disabled, true);
assert.equal(successfulChannel.removeButton.disabled, true);
assert.equal(successfulChannel.removeButton.parentElement.title, successfulDestinationReason);
assert.equal(failedChannel.disabled, false);
assert.equal(failedChannel.removeButton.disabled, false);

postTypeSelect.value = "giveaway";
syncGiveawayTargets();
assert.equal(checkboxes[0].checked, true);
assert.equal(checkboxes[0].disabled, true);
assert.equal(checkboxes[1].checked, true);
assert.equal(checkboxes[2].checked, false);
function activeExistingAttachmentIds() { return new Set(); }
function activeExistingAttachments() { return []; }
function deletedAttachmentIds() { return []; }
function buildGiveawayPayload() { return {}; }
const form = {
  body: {value: "Body"}, post_type: {value: "giveaway"}, status: {value: "queued"},
  scheduled_for: {value: ""}, source_link: {value: ""}, visibility: {value: ""},
  querySelectorAll(selector) { return selector === ".target-checkbox" ? checkboxes : []; },
};
assert.deepEqual(new Set(buildPostPayload(form).target_account_ids), new Set(["successful", "failed"]));
'''
    result = subprocess.run([node, "-e", script], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_giveaway_builder_preserves_empty_saved_selection_and_allows_final_channel_removal():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to exercise the giveaway builder.")
    builder_path = Path(__file__).parents[1] / "app/static/giveaway_builder.js"
    script = r'''
const assert = require("node:assert/strict");
global.window = {};
require("node:vm").runInThisContext(require("node:fs").readFileSync(process.argv[1], "utf8"));
const accounts = {bluesky: [{id: "unsent", label: "Bluesky"}], instagram: []};
function create(initialValue) {
  const handlers = {};
  const root = {innerHTML: "", addEventListener(event, handler) { handlers[event] = handler; }};
  const builder = window.LynxGiveawayBuilder.create({root, getServiceAccounts: () => accounts, initialValue});
  return {root, handlers, builder};
}
const created = create(null);
assert.deepEqual(created.builder.getChannelAccountIds(), ["unsent"]);
const cancelled = create({giveaway_end_at: "2050-01-01T00:00", channels: []});
assert.deepEqual(cancelled.builder.getChannelAccountIds(), []);
cancelled.builder.refresh();
assert.deepEqual(cancelled.builder.getChannelAccountIds(), []);
const editing = create({giveaway_end_at: "2050-01-01T00:00", channels: [{service: "bluesky", account_id: "unsent"}]});
const channelId = editing.root.innerHTML.match(/data-action="remove-channel" data-channel-id="([^"]+)"/)[1];
editing.handlers.click({target: {closest() { return {dataset: {action: "remove-channel", channelId}}; }}});
assert.deepEqual(editing.builder.getChannelAccountIds(), []);
assert.deepEqual(editing.builder.getValue().channels, []);
'''
    result = subprocess.run([node, "-e", script, str(builder_path)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_edit_submit_allows_empty_giveaway_target_selection_to_reach_api():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to exercise the edit submission handler.")
    template = (Path(__file__).parents[1] / "app/templates/scheduled_post_detail.html").read_text(encoding="utf-8")
    listener_prefix = 'document.getElementById("scheduled-post-update-form").addEventListener("submit", async (event) => {'
    helper_source = "async function submitPostPlan(form, {sendNow = false} = {})" + template.split("async function submitPostPlan(form, {sendNow = false} = {})", 1)[1].split(listener_prefix, 1)[0]
    submit_source = listener_prefix + template.split(listener_prefix, 1)[1].split('document.getElementById("send-now-button")', 1)[0]
    script = r'''
const assert = require("node:assert/strict");
const postId = "giveaway-post";
let postPlanSaveInProgress = false;
let submit, validationCalls = 0, requests = 0, reloads = 0;
const document = {
  getElementById() { return {addEventListener(event, handler) { submit = handler; }}; },
  querySelectorAll() { return []; },
};
const giveawayBuilder = {validate() { validationCalls++; return {ok: false, message: "No channels."}; }};
const window = {location: {reload() { reloads++; }}};
const FormData = class { append() {} };
function pendingUploadAltTexts() { return []; }
function buildPostPayload() {
  return {post_type: "giveaway", target_account_ids: [], body: "", status: "queued", scheduled_for: null,
    publish_overrides_json: {}, metadata_json: {}, attachment_order: [], deleted_attachment_ids: [],
    attachment_alt_texts: {}, giveaway: {channels: []}};
}
async function fetch(url, options) { requests++; assert.equal(url, "/scheduled-posts/giveaway-post"); return {ok: true}; }
''' + helper_source + submit_source + r'''
(async () => {
  await submit({preventDefault() {}, currentTarget: {uploads: {files: []}}});
  assert.equal(validationCalls, 0);
  assert.equal(requests, 1);
  assert.equal(reloads, 1);
})().catch(error => { console.error(error); process.exit(1); });
'''
    result = subprocess.run([node, "-e", script], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_browser_reselected_destination_normalizes_cancelled_status_to_draft():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed to exercise destination reselection.")
    template = (Path(__file__).parents[1] / "app/templates/scheduled_post_detail.html").read_text(encoding="utf-8")
    refresh_source = "function refreshPostGuidance()" + template.split("function refreshPostGuidance()", 1)[1].split("function buildPostPayload(form)", 1)[0]
    payload_source = "function buildPostPayload(form)" + template.split("function buildPostPayload(form)", 1)[1].split("function pendingUploadAltTexts()", 1)[0]
    script = refresh_source + payload_source + r'''
const assert = require("node:assert/strict");
const checkbox = {value: "selected", checked: true};
const successfulTargetAccountIds = new Set();
const document = {querySelectorAll(selector) { return selector === ".target-checkbox" ? [checkbox] : []; }};
const postStatusSelect = {value: "cancelled"};
const bodyTextarea = {value: "Body"}, bodyLengthIndicator = {}, snapshotBodyLength = {}, snapshotAttachments = {}, snapshotTargets = {};
const uploadsInput = {files: []}, uploadSummary = {};
const giveawayBuilder = {getChannelAccountIds() { return ["selected"]; }};
function combinedAttachmentSnapshot() { return {total: 0}; }
function fileSnapshot() { return {total: 0}; }
function refreshGiveawayState() {}
function activeExistingAttachmentIds() { return new Set(); }
function activeExistingAttachments() { return []; }
function deletedAttachmentIds() { return []; }
function buildGiveawayPayload() { return {}; }
const form = {
  body: bodyTextarea, post_type: {value: "standard"}, status: postStatusSelect,
  scheduled_for: {value: ""}, source_link: {value: ""}, visibility: {value: ""},
  querySelectorAll(selector) { return selector === ".target-checkbox" ? [checkbox] : []; },
};
for (const postType of ["standard", "giveaway"]) {
  form.post_type.value = postType;
  postStatusSelect.value = "cancelled";
  refreshPostGuidance();
  assert.equal(postStatusSelect.value, "draft");
  assert.equal(buildPostPayload(form).status, "draft");
  assert.deepEqual(buildPostPayload(form).target_account_ids, ["selected"]);
  postStatusSelect.value = "scheduled";
  refreshPostGuidance();
  assert.equal(buildPostPayload(form).status, "scheduled");
}
checkbox.checked = false;
postStatusSelect.value = "cancelled";
refreshPostGuidance();
assert.equal(postStatusSelect.value, "cancelled");
'''
    result = subprocess.run([node, "-e", script], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
