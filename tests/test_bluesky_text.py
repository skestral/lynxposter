from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.adapters.bluesky import BlueskyDestinationAdapter, _build_facets, _build_preview_facets
from app.models import Account, CanonicalPost, Persona


@pytest.mark.parametrize(
    ("caption", "tags"),
    [
        ("🦊 #art #art #猫", ["#art", "#art", "#猫"]),
        ("Café 🎨\n#mañana #日本語", ["#mañana", "#日本語"]),
        ("🎨 #cafe\u0301 #mañana", ["#cafe\u0301", "#mañana"]),
        ("🧑🏽‍🎨 Handmade #𝓕𝓾𝓻𝓻𝔂 #art", ["#𝓕𝓾𝓻𝓻𝔂", "#art"]),
        ("Linux caption…\n#art #猫...", ["#art", "#猫"]),
        ("Curly “quotes” and café...\r\n#art #art…", ["#art", "#art"]),
        ("Emoji tags #🎨 #art🎨 and ＃猫…", ["#🎨", "#art🎨", "＃猫"]),
        ("Hyphenated #digital-art... #under_score!", ["#digital-art", "#under_score"]),
        ("\ufeff#art #１２３ #١٢٣", ["#art", "#１２３", "#١٢٣"]),
    ],
)
def test_instagram_unicode_caption_keeps_text_and_hashtag_ranges(monkeypatch, caption, tags):
    captured = {}

    class FakeClient:
        def send_post(self, *, text, facets):
            captured.update(text=text, facets=facets)
            return SimpleNamespace(uri="at://did:plc:artist/app.bsky.feed.post/example")

    monkeypatch.setattr("app.adapters.bluesky._get_client", lambda *args, **kwargs: FakeClient())
    post = CanonicalPost(
        body=caption,
        origin_kind="imported",
        publish_overrides_json={},
        metadata_json={"source_service": "instagram"},
        attachments=[],
    )
    account = Account(id="bluesky-account", service="bluesky", credentials_json={"handle": "artist.bsky.social"})
    persona = Persona(settings_json={})
    adapter = BlueskyDestinationAdapter()

    preview = adapter.preview(post, persona, account)
    adapter.publish(None, post, persona, account)

    assert captured["text"] == caption
    assert preview.rendered_body == caption
    encoded = caption.encode("utf-8")
    facets = captured["facets"]
    assert [encoded[item["index"]["byteStart"] : item["index"]["byteEnd"]].decode("utf-8") for item in facets] == tags
    assert [item["features"][0]["tag"] for item in facets] == [tag[1:] for tag in tags]
    assert all(left["index"]["byteEnd"] <= right["index"]["byteStart"] for left, right in zip(facets, facets[1:]))
    assert [(item["byteStart"], item["byteEnd"]) for item in preview.request_shape["facets"]] == [
        (item["index"]["byteStart"], item["index"]["byteEnd"]) for item in facets
    ]


def test_hashtag_indices_are_utf8_bytes_for_every_occurrence():
    facets = _build_facets("🦊 #art #art #猫")

    assert [item["index"] for item in facets] == [
        {"byteStart": 5, "byteEnd": 9},
        {"byteStart": 10, "byteEnd": 14},
        {"byteStart": 15, "byteEnd": 19},
    ]


def test_linux_punctuation_and_emoji_do_not_shift_hashtag_byte_offsets():
    caption = "Linux… 🎨\n#art #猫..."
    facets = _build_facets(caption)

    assert [facet["index"] for facet in facets] == [
        {"byteStart": 14, "byteEnd": 18},
        {"byteStart": 19, "byteEnd": 23},
    ]


def test_at_handle_inside_url_is_not_resolved_or_overlapped(monkeypatch):
    caption = "🎨 https://example.com/@artist.bsky.social#art #art"
    facets = _build_facets(caption)
    preview, _ = _build_preview_facets(caption)

    assert [facet["features"][0]["$type"] for facet in facets] == [
        "app.bsky.richtext.facet#link", "app.bsky.richtext.facet#tag"
    ]
    assert [facet["type"] for facet in preview] == ["link", "tag"]
    assert facets[0]["index"]["byteEnd"] < facets[1]["index"]["byteStart"]


def test_numeric_tags_and_inline_hashes_remain_plain_text():
    caption = "#123 word#art #123art #️⃣ #art"
    facets = _build_facets(caption)
    preview, _ = _build_preview_facets(caption)

    assert [facet["features"][0]["tag"] for facet in facets] == ["123art", "art"]
    assert [facet["tag"] for facet in preview] == ["123art", "art"]


def test_non_javascript_whitespace_does_not_create_a_tag_boundary():
    caption = "word\u0085#art #art"
    facets = _build_facets(caption)

    assert len(facets) == 1
    assert facets[0]["index"]["byteStart"] == len("word\u0085#art ".encode("utf-8"))


def test_overlong_hashtag_remains_plain_text():
    caption = "#" + "x" * 64 + " #" + "x" * 65
    facets = _build_facets(caption)
    preview_facets, _ = _build_preview_facets(caption)

    assert len(facets) == len(preview_facets) == 1
    assert facets[0]["features"][0]["tag"] == "x" * 64
    assert facets[0]["index"] == {"byteStart": 0, "byteEnd": 65}


def test_unicode_links_mentions_and_tags_do_not_overlap(monkeypatch):
    caption = "🎨 #art https://example.com/gallery#art @artist.bsky.social #art"
    resolved_handles = []

    def fake_resolve(url, *, params, timeout):
        resolved_handles.append(params["handle"])
        return SimpleNamespace(status_code=200, json=lambda: {"did": "did:plc:artist"})

    monkeypatch.setattr("app.adapters.bluesky.requests.get", fake_resolve)
    facets = _build_facets(caption)
    preview_facets, notes = _build_preview_facets(caption)

    assert resolved_handles == ["artist.bsky.social"]
    assert len(facets) == len(preview_facets) == 4
    encoded = caption.encode("utf-8")
    assert [encoded[item["index"]["byteStart"] : item["index"]["byteEnd"]].decode("utf-8") for item in facets] == [
        "#art",
        "https://example.com/gallery#art",
        "@artist.bsky.social",
        "#art",
    ]
    assert all(left["index"]["byteEnd"] <= right["index"]["byteStart"] for left, right in zip(facets, facets[1:]))
    assert [(item["byteStart"], item["byteEnd"]) for item in preview_facets] == [
        (item["index"]["byteStart"], item["index"]["byteEnd"]) for item in facets
    ]
    assert notes


@pytest.mark.parametrize("caption", ["é" * 300, "e\u0301" * 300, "🧑🏽‍🎨" * 100])
def test_bluesky_text_limit_counts_displayed_characters(caption):
    post = CanonicalPost(body=caption, publish_overrides_json={}, attachments=[])
    account = Account(id="bluesky-account", service="bluesky", credentials_json={})

    assert BlueskyDestinationAdapter().validate(post, Persona(settings_json={}), account) == []


def test_bluesky_text_limit_rejects_more_than_300_graphemes():
    post = CanonicalPost(body="e\u0301" * 301, publish_overrides_json={}, attachments=[])
    account = Account(id="bluesky-account", service="bluesky", credentials_json={})

    issues = BlueskyDestinationAdapter().validate(post, Persona(settings_json={}), account)

    assert [issue.message for issue in issues] == ["Bluesky posts are limited to 300 characters."]


def test_bluesky_text_limit_also_rejects_more_than_3000_utf8_bytes():
    post = CanonicalPost(body="👨‍👩‍👧‍👦" * 121, publish_overrides_json={}, attachments=[])
    account = Account(id="bluesky-account", service="bluesky", credentials_json={})

    issues = BlueskyDestinationAdapter().validate(post, Persona(settings_json={}), account)

    assert [issue.message for issue in issues] == ["Bluesky post text is limited to 3,000 UTF-8 bytes."]
