from atproto import Client
from settings.auth import BSKY_HANDLE, BSKY_PASSWORD, BSKY_SESSION_STRING
from settings.paths import *
from settings import settings
from local.functions import write_log, lang_toggle
import arrow
import os
import subprocess
import random
import string
from settings.paths import image_path
import time

# Date format adjustment
date_in_format = 'YYYY-MM-DDTHH:mm:ssZ'

# # Load the session string from the file
# load_session_string()

# Setting up connections to Bluesky using session string
bsky = Client()

# Function to load the session string from the file
def load_session_string():
    try:
        with open("session.txt", "r") as file:
            contents = file.read().strip()
            if contents:
                write_log("Session string loaded successfully.")
                return contents
            else:
                write_log("Session file is empty.")
                return None
    except FileNotFoundError:
        write_log("Session file not found.")
        return None

# Function to save the session string to a file
def save_session_string(session_string):
    """Save the session string to a file."""
    with open("session.txt", "w") as file:
        file.write(session_string)
    write_log("Session string saved to file.")

# Function to handle session changes
def on_session_changed(new_session_string):
    """Callback function to handle session changes when tokens are refreshed."""
    write_log("Session has been refreshed, updating session string.")
    save_session_string(new_session_string)  # Save the new session string
    write_log("Session string has been saved successfully.")

# Load the session string from the file
session_string = load_session_string()

# Setting up connections to Bluesky using the session string
bsky = Client()

# Authenticate using the stored session string or fall back to login if invalid
try:
    write_log("Attempting to login using the stored session string.")
    bsky.login(session_string=session_string)
    write_log("Login using session string successful.")

    # Subscribe to the session change event after successful login
    if hasattr(bsky, '_session') and bsky._session is not None:
        bsky._session.on_session_changed = on_session_changed
        write_log("Session change event subscribed successfully.")
    else:
        write_log("Session not initialized after login. Cannot subscribe to session changes.")
except Exception as e:
    write_log(f"Login using session string failed: {e}")
    write_log("Falling back to login using credentials.")
    bsky.login(BSKY_HANDLE, BSKY_PASSWORD)
    session_string = bsky.export_session_string()
    save_session_string(session_string)
    write_log("New session string has been exported and saved.")

    # Subscribe to session change event after fallback login
    if hasattr(bsky, '_session') and bsky._session is not None:
        bsky._session.on_session_changed = on_session_changed
        write_log("Session change event subscribed successfully.")
    else:
        write_log("Session not initialized after fallback login. Cannot subscribe to session changes.")

# Function to return the authenticated bsky session
def get_bsky_session():
    """Return the initialized and authenticated bsky session."""
    if bsky and bsky._session is not None:
        return bsky
    else:
        raise Exception("Bluesky session is not initialized properly.")

# Ensure the session string is exported at the end of the script
session_string = bsky.export_session_string()
save_session_string(session_string)
write_log("Session string exported and saved at the end of the script.")

# Date format adjustment
date_in_format = 'YYYY-MM-DDTHH:mm:ssZ'

# Getting posts from Bluesky
def get_posts(timelimit=arrow.utcnow().shift(hours=-1)):  # Adjust `hours` to your desired time window
    write_log("Gathering posts")
    posts = {}
    bsky = get_bsky_session()  # Ensure session management is in place
    profile_feed = bsky.app.bsky.feed.get_author_feed({'actor': BSKY_HANDLE})
    visibility = settings.visibility

    for feed_view in profile_feed.feed:
        try:
            if feed_view.post.author.handle != BSKY_HANDLE:
                continue

            # Get and parse created_at date
            created_at_str = feed_view.post.record.created_at.split(".")[0]
            if not created_at_str.endswith('Z'):
                created_at_str += 'Z'
            created_at = arrow.get(created_at_str, 'YYYY-MM-DDTHH:mm:ssZ')

            # Skip posts older than the timelimit
            if created_at < timelimit:
                continue

            repost = False
            if hasattr(feed_view.reason, "indexed_at"):
                repost = True
                created_at = arrow.get(feed_view.reason.indexed_at.split(".")[0], 'YYYY-MM-DDTHH:mm:ssZ')

            langs = feed_view.post.record.langs
            mastodon_post = (lang_toggle(langs, "mastodon") and settings.Mastodon)
            twitter_post = (lang_toggle(langs, "twitter") and settings.Twitter)
            if not mastodon_post and not twitter_post:
                continue
            cid = feed_view.post.cid
            text = feed_view.post.record.text
            send_mention = True
            if feed_view.post.record.facets:
                text = restore_urls(feed_view.post.record)
                if settings.mentions != "ignore":
                    text, send_mention = parse_mentioned_username(feed_view.post.record, text)
            if not send_mention:
                continue
            reply_to_user = BSKY_HANDLE
            reply_to_post = ""
            quoted_post = ""
            quote_url = ""
            allowed_reply = get_allowed_reply(feed_view.post)
            if feed_view.post.embed and hasattr(feed_view.post.embed, "record"):
                try:
                    quoted_user, quoted_post, quote_url, open_quote = get_quote_post(feed_view.post.embed.record)
                except Exception as e:
                    write_log(f"Post {cid} is of a type the crossposter can't parse. Error: {e}", "error")
                    continue
                if quoted_user != BSKY_HANDLE and (not settings.quote_posts or not open_quote):
                    continue
                elif quoted_user == BSKY_HANDLE:
                    text = text.replace(quote_url, "")
            if feed_view.post.record.reply:
                reply_to_post = feed_view.post.record.reply.parent.cid
                try:
                    reply_to_user = feed_view.reply.parent.author.handle
                except:
                    reply_to_user = get_reply_to_user(feed_view.post.record.reply.parent)
            if not reply_to_user:
                write_log(f"Unable to find the user that post {cid} replies to or quotes", "error")
                continue
            if created_at > timelimit and reply_to_user == BSKY_HANDLE:
                image_data = ""
                images = []
                if feed_view.post.embed and hasattr(feed_view.post.embed, "images"):
                    image_data = feed_view.post.embed.images
                elif feed_view.post.embed and hasattr(feed_view.post.embed, "playlist"):
                    m3u8_url = feed_view.post.embed.playlist
                    output_mp4 = download_bsky_video(m3u8_url)
                    if output_mp4:
                        images.append({"filename":output_mp4, "alt":feed_view.post.embed.alt})
                    else:
                        write_log(f"Failed to download or convert {m3u8_url} to mp4.", "error")
                elif feed_view.post.embed and hasattr(feed_view.post.embed, "media") and hasattr(feed_view.post.embed.media, "images"):
                    image_data = feed_view.post.embed.media.images
                if feed_view.post.embed and hasattr(feed_view.post.embed, "external") and hasattr(feed_view.post.embed.external, "uri"):
                    if feed_view.post.embed.external.uri not in text:
                        text += '\n' + feed_view.post.embed.external.uri
                if image_data:
                    for image in image_data:
                        images.append({"url": image.fullsize, "alt": image.alt})
                if visibility == "hybrid" and reply_to_post:
                    visibility = "unlisted"
                elif visibility == "hybrid":
                    visibility = "public"
                link = f"https://bsky.app/profile/{BSKY_HANDLE}/post/{feed_view.post.uri.split('/')[-1]}"
                post_info = {
                    "text": text,
                    "reply_to_post": reply_to_post,
                    "quoted_post": quoted_post,
                    "quote_url": quote_url,
                    "link": link,
                    "images": images,
                    "visibility": visibility,
                    "twitter": twitter_post,
                    "mastodon": mastodon_post,
                    "allowed_reply": allowed_reply,
                    "repost": repost,
                    "timestamp": created_at
                }
                posts[cid] = post_info

        except Exception as e:
            write_log(f"An error occurred while processing post {feed_view.post.cid}: {e}", "error")

    return posts

def get_quote_post(post):
    try:
        if isinstance(post, dict):
            user = post["record"]["author"]["handle"]
            cid = post["record"]["cid"]
            uri = post["record"]["uri"]
            labels = post["record"]["author"].get("labels", [])
        elif hasattr(post, "author"):
            user = post.author.handle
            cid = post.cid
            uri = post.uri
            labels = getattr(post.author, "labels", [])
        elif hasattr(post, "record") and hasattr(post.record, "author"):
            user = post.record.author.handle
            cid = post.record.cid
            uri = post.record.uri
            labels = getattr(post.record.author, "labels", [])
        else:
            raise AttributeError("Post object structure is not recognized")

        open = True
        if labels and labels[0].val == "!no-unauthenticated":
            open = False

        url = "https://bsky.app/profile/" + user + "/post/" + uri.split("/")[-1]
        return user, cid, url, open
    except Exception as e:
        write_log(f"Error in get_quote_post: {e}", "error")
        return None, None, None, False

def get_reply_to_user(reply):
    uri = reply.uri
    username = ""
    try:
        response = bsky.app.bsky.feed.get_post_thread(params={"uri": uri})
        username = response.thread.post.author.handle
    except Exception as e:
        write_log(f"Unable to retrieve reply_to-user of post. Error: {e}", "error")
    return username

def restore_urls(record):
    text = record.text
    encoded_text = text.encode("UTF-8")
    for facet in record.facets:
        if facet.features[0].py_type != "app.bsky.richtext.facet#link":
            continue
        url = facet.features[0].uri
        # The index section designates where a URL starts and ends. Using this we can pick out the exact
        # string representing the URL in the post, and replace it with the actual URL.
        start = facet.index.byte_start
        end = facet.index.byte_end
        section = encoded_text[start:end]
        shortened = section.decode("UTF-8")
        text = text.replace(shortened, url)
    return text

def parse_mentioned_username(record, text):
    # send_mention keeps track if the post should be sent at all.
    send_mention = True
    encoded_text = text.encode("UTF-8")
    for facet in record.facets:
        if facet.features[0].py_type != "app.bsky.richtext.facet#mention":
            continue
        # The index section designates where a username starts and ends. Using this we can pick out the exact
        # string representing the user in the post, and replace it with the corrected value
        start = facet.index.byte_start
        end = facet.index.byte_end
        username = encoded_text[start:end]
        username = username.decode("UTF-8")
        # If the mentions setting is set to skip, None will be returned, if it's set to strip the
        # text will be returned with the @ of the username removed, if it's set to URL the name will
        # be replaced with a link to their profile.
        if settings.mentions == "skip":
            send_mention = False
        elif settings.mentions == "strip":
            text = text.replace(username, username.replace("@", ""))
        elif settings.mentions == "url":
            base_url = "https://bsky.app/profile/"
            did = facet.features[0].did
            url = base_url + did
            text = text.replace(username, url)
    return text, send_mention

def get_allowed_reply(post):
    reply_restriction = post.threadgate
    if reply_restriction is None:
        return "All"
    if len(reply_restriction.record.allow) == 0:
        return "None"
    if reply_restriction.record.allow[0].py_type == "app.bsky.feed.threadgate#followingRule":
        return "Following"
    if reply_restriction.record.allow[0].py_type == "app.bsky.feed.threadgate#mentionRule":
        return "Mentioned"
    return "Unknown"

def download_bsky_video(m3u8_url):
    """Download and convert an .m3u8 stream to .mp4 format."""
    output_filename = ''.join(random.choice(string.ascii_lowercase) for i in range(10)) + ".mp4"
    output_path = image_path + output_filename
    try:
        ffmpeg_command = ["ffmpeg", "-y", "-i", m3u8_url, "-c", "copy", output_path]
        subprocess.run(ffmpeg_command, check=True)
        if os.path.exists(output_path):
            write_log(f"Successfully downloaded and converted {m3u8_url} to {output_path}.")
            return output_path
        else:
            write_log(f"Failed to create output file {output_path}.")
            return None

    except subprocess.CalledProcessError as e:
        write_log(f"Error during ffmpeg conversion: {e}", "error")
        return None

# def get_reply_to_user(reply):
#     uri = reply.uri
#     username = ""
#     try:
#         response = bsky.app.bsky.feed.get_post_thread(params={"uri": uri})
#         username = response.thread.post.author.handle
#     except:
#         write_log("Unable to retrieve reply_to-user of post.", "error")
#     return username

# Function to post to Bluesky
def post_to_bluesky(text, images):
    try:
        media_ids = []
        for image in images:
            res = bsky.upload_media(image["filename"])
            media_ids.append(res["id"])

        bsky.create_post(text, media_ids)
        time.sleep(10)
        write_log("Posted to Bluesky successfully")
    except Exception as e:
        write_log(f"Failed to post to Bluesky: {e}", "error")

if __name__ == "__main__":
    # Add test or main function logic if needed
    pass