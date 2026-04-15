import random
import string
import time
import urllib.request
import arrow
import requests

from settings import settings
from settings.paths import image_path
from local.functions import write_log
from local.db import db_write
from output.twitter import tweet, retweet
from output.mastodon import toot, retoot
from output.discord import post_to_discord
from output.tumblr import post_to_tumblr
from atproto import Client
from input.bluesky import load_session_string
from datetime import datetime
import re

def download_image(image_url):
    try:
        filename = ''.join(random.choice(string.ascii_lowercase) for i in range(10)) + ".jpg"
        filepath = image_path + filename
        urllib.request.urlretrieve(image_url, filepath)
        return filepath
    except Exception as e:
        write_log(f"Failed to download image: {e}", "error")
        return None

def get_images(images):
    local_images = []
    for image in images:
        #if image["filename"] is str: #already downloaded, has filename (videos)
        if "filename" in image: #already downloaded, has filename (videos)
            local_images.append(image)
        else: #needs to be downloaded, has url
            alt = image["alt"]
            filename = ''.join(random.choice(string.ascii_lowercase) for i in range(10)) + ".jpg"
            filename = image_path + filename
            urllib.request.urlretrieve(image["url"], filename)
            image_info = {
                "filename": filename,
                "alt": alt
            }
            local_images.append(image_info)
    return local_images

def extract_hashtags(text):
    hashtags = re.findall(r'#\w+', text)
    return [tag.strip('#') for tag in hashtags]

def parse_mentions(text):
    spans = []
    mention_regex = rb"(@([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)"
    text_bytes = text.encode("UTF-8")
    for m in re.finditer(mention_regex, text_bytes):
        spans.append({
            "start": m.start(1),
            "end": m.end(1),
            "handle": m.group(1).decode("UTF-8")
        })
    return spans

def parse_urls(text):
    spans = []
    url_regex = rb"(https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*))"
    text_bytes = text.encode("UTF-8")
    for m in re.finditer(url_regex, text_bytes):
        spans.append({
            "start": m.start(1),
            "end": m.end(1),
            "url": m.group(1).decode("UTF-8"),
        })
    return spans

def parse_facets(text):
    facets = []
    for m in parse_mentions(text):
        resp = requests.get(
            "https://bsky.social/xrpc/com.atproto.identity.resolveHandle",
            params={"handle": m["handle"]},
        )
        if resp.status_code == 400:
            continue
        did = resp.json()["did"]
        facets.append({
            "index": {
                "byteStart": m["start"],
                "byteEnd": m["end"],
            },
            "features": [{"$type": "app.bsky.richtext.facet#mention", "did": did}],
        })
    for u in parse_urls(text):
        facets.append({
            "index": {
                "byteStart": u["start"],
                "byteEnd": u["end"],
            },
            "features": [
                {
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": u["url"],
                }
            ],
        })
    for h in extract_hashtags(text):
        start = text.find("#" + h)
        end = start + len("#" + h)
        facets.append({
            "index": {
                "byteStart": start,
                "byteEnd": end,
            },
            "features": [
                {
                    "$type": "app.bsky.richtext.facet#tag",
                    "tag": h,
                }
            ],
        })
    return facets

def post_to_bluesky(text, images):
    session_string = load_session_string()
    client = Client()

    if not session_string:
        write_log("BSKY_SESSION_STRING is not set. Cannot login to Bluesky.", "error")
        return False, None
    try:
        client.login(session_string=session_string)
    except ValueError as e:
        write_log(f"Failed to login to Bluesky: {e}", "error")
        return False, None

    facets = parse_facets(text)

    try:
        if len(images) == 1 and images[0]["filename"].endswith(".mp4"): # video
            with open(images[0]["filename"], 'rb') as video_file:
                video_data = video_file.read()
            video_alt = images[0]["alt"]
            response = client.send_video(text, video_data, video_alt, facets=facets)
        else: # images
            image_datas: list[bytes] = []
            image_alts: list[str] = []
            for image in images:
                with open(image["filename"], 'rb') as img_file:
                    img_data = img_file.read()
                image_datas.append(img_data)
                image_alts.append(image["alt"])
            response = client.send_images(text, image_datas, image_alts, facets=facets)
        write_log("Bluesky post created.")
        time.sleep(10)
        bluesky_link = f"https://bsky.app/post/{response.uri}"  # Assuming the response contains a URI field for the post
        return True, bluesky_link
    except Exception as e:
        write_log(f"Failed to create Bluesky post: {e}", "error")
        return False, None

def post(posts, database, post_cache):
    updates = False
    for cid in reversed(list(posts.keys())):
        post = posts[cid]
        if settings.max_per_hour != 0 and len(post_cache) >= settings.max_per_hour:
            write_log("Max posts per hour reached.")
            break

        posted = False
        tweet_id = ""
        toot_id = ""
        discord_id = ""
        tumblr_id = ""
        bsky_id = ""
        t_fail = 0
        m_fail = 0
        d_fail = 0
        tu_fail = 0
        bsky_fail = 0
        if cid in database:
            tweet_id = database[cid]["ids"]["twitter_id"]
            toot_id = database[cid]["ids"]["mastodon_id"]
            discord_id = database[cid]["ids"]["discord_id"]
            tumblr_id = database[cid]["ids"]["tumblr_id"]
            bsky_id = database[cid]["ids"].get("bsky_id", "")
            t_fail = database[cid]["failed"]["twitter"]
            m_fail = database[cid]["failed"]["mastodon"]
            d_fail = database[cid]["failed"]["discord"]
            tu_fail = database[cid]["failed"]["tumblr"]
            bsky_fail = database[cid]["failed"].get("bsky", 0)
        if m_fail >= settings.max_retries:
            write_log("Error limit reached, not posting to Mastodon", "error")
            if not toot_id:
                updates = True
                toot_id = "FailedToPost"
        if t_fail >= settings.max_retries:
            write_log("Error limit reached, not posting to Twitter", "error")
            if not tweet_id:
                updates = True
                tweet_id = "FailedToPost"
        if d_fail >= settings.max_retries:
            write_log("Error limit reached, not posting to Discord", "error")
            if not discord_id:
                updates = True
                discord_id = "FailedToPost"
        if tu_fail >= settings.max_retries:
            write_log("Error limit reached, not posting to Tumblr", "error")
            if not tumblr_id:
                updates = True
                tumblr_id = "FailedToPost"

        text = post["text"]
        reply_to_post = post.get("reply_to_post", "")
        quoted_post = post.get("quoted_post", "")
        quote_url = post.get("quote_url", "")
        link = post.get("link", "")
        images = post.get("images", [])
        visibility = post.get("visibility", "public")
        allowed_reply = post.get("allowed_reply", "All")
        instagram_link = post.get("instagram_link", "")  # Get Instagram link
        tweet_reply = ""
        toot_reply = ""
        tweet_quote = ""
        toot_quote = ""

        post.setdefault("twitter", True)
        post.setdefault("mastodon", True)
        post.setdefault("discord", True)
        post.setdefault("tumblr", True)

        if tweet_id and toot_id and discord_id and tumblr_id and bsky_id and not post.get("repost", False):
            continue

        repost_timelimit = arrow.utcnow().shift(hours=-1)
        if cid in post_cache:
            repost_timelimit = post_cache[cid]

        if reply_to_post in database:
            tweet_reply = database[reply_to_post]["ids"]["twitter_id"]
            toot_reply = database[reply_to_post]["ids"]["mastodon_id"]
        elif reply_to_post and reply_to_post not in database:
            write_log(f"Post {cid} was a reply to a post that is not in the database.", "error")
            continue

        if quoted_post in database:
            tweet_quote = database[quoted_post]["ids"]["twitter_id"]
            toot_quote = database[quoted_post]["ids"]["mastodon_id"]
        elif quoted_post and quoted_post not in database:
            if settings.quote_posts and quote_url not in text:
                text += "\n" + quote_url
            elif not settings.quote_posts:
                write_log(f"Post {cid} was a quote of a post that is not in the database.", "error")
                continue

        if not tweet_reply:
            tweet_reply = None
        if not toot_reply:
            toot_reply = None
        if not tweet_quote:
            tweet_quote = None

        if images and (not tweet_id or not toot_id or not discord_id or not tumblr_id or not bsky_id):
            images = get_images(images)

        # Post to Bluesky first for Instagram posts
        if "instagram" in post:
            if bsky_id:
                write_log(f"Instagram post {cid} already posted to Bluesky.")
            else:
                success, bsky_link = post_to_bluesky(text, images)
                if success:
                    updates = True
                    bsky_id = bsky_link.split('/')[-1]
                    post["link"] = bsky_link
                    post_cache[cid] = arrow.utcnow()
                else:
                    bsky_fail += 1
                    bsky_id = ""
            continue

        # Post to Twitter
        if not post["twitter"]:
            tweet_id = "skipped"
            write_log("Not posting to Twitter because posting was set to false.")
        elif tweet_id and not post.get("repost", False):
            write_log("Post " + cid + " already sent to Twitter.")
        elif tweet_id and post.get("repost", False) and post["timestamp"] > repost_timelimit:
            try:
                retweet(tweet_id)
                posted = True
            except Exception as error:
                write_log(error, "error")
        elif not tweet_id and tweet_reply != "skipped" and tweet_reply != "FailedToPost":
            updates = True
            try:
                tweet_id = tweet(text, tweet_reply, tweet_quote, images, allowed_reply)
                posted = True
            except Exception as error:
                write_log(error, "error")
                t_fail += 1
                tweet_id = ""
        else:
            write_log("Not posting " + cid + " to Twitter")

        # Post to Mastodon
        if not post["mastodon"]:
            toot_id = "skipped"
            write_log("Not posting to Mastodon because posting was set to false.")
        elif toot_id and not post.get("repost", False):
            write_log("Post " + cid + " already sent to Mastodon.")
        elif toot_id and post.get("repost", False) and post["timestamp"] > repost_timelimit:
            try:
                retoot(toot_id)
                posted = True
            except Exception as error:
                write_log(error, "error")
        elif not toot_id and toot_reply != "skipped" and toot_reply != "FailedToPost":
            updates = True
            try:
                toot_id = toot(text, toot_reply, toot_quote, images, visibility)
                posted = True
            except Exception as error:
                write_log(error, "error")
                m_fail += 1
                toot_id = ""
            else:
                write_log("Not posting " + cid + " to Mastodon")

            # Post to Discord
        if not post["discord"]:
            discord_id = "skipped"
            write_log("Not posting to Discord because posting was set to false.")
        elif discord_id and not post.get("repost", False):
            write_log("Post " + cid + " already sent to Discord.")
        elif discord_id and post.get("repost", False) and post["timestamp"] > repost_timelimit:
            try:
                pass  # Add logic for Discord repost if needed
            except Exception as error:
                write_log(error, "error")
        elif not discord_id and toot_reply != "skipped" and toot_reply != "FailedToPost":
            updates = True
            try:
                image_paths = [img['filename'] for img in images]
                post_to_discord(text, link, image_paths)  # Pass the link and images to the function
                discord_id = "posted"  # Placeholder for actual Discord message ID if needed
                posted = True
            except Exception as error:
                write_log(error, "error")
                d_fail += 1
                discord_id = ""
        else:
            write_log("Not posting " + cid + " to Discord")

            # Post to Tumblr
        if not post["tumblr"]:
            tumblr_id = "skipped"
            write_log("Not posting to Tumblr because posting was set to false.")
        elif tumblr_id and not post.get("repost", False):
            write_log(f"Post {cid} already sent to Tumblr.")
        elif tumblr_id and post.get("repost", False) and post["timestamp"] > repost_timelimit:
            try:
                pass  # Add logic for Tumblr repost if needed
            except Exception as error:
                write_log(error, "error")
        elif not tumblr_id:
            updates = True
            try:
                tumblr_id = post_to_tumblr(text, images)  # Pass the images to the post_to_tumblr function
                posted = True
            except Exception as error:
                write_log(error, "error")
                tu_fail += 1
                tumblr_id = ""
        else:
            write_log(f"Not posting {cid} to Tumblr")

            # Update the database with the new post IDs and failure counts
        database = db_write(cid, tweet_id, toot_id, discord_id, tumblr_id, bsky_id,
                            {"twitter": t_fail, "mastodon": m_fail, "discord": d_fail, "tumblr": tu_fail,
                             "bsky": bsky_fail}, database)
        if posted:
            post_cache[cid] = arrow.utcnow()

    return updates, database, post_cache
