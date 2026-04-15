import requests
from settings import settings
from settings.auth import DISCORD_WEBHOOK_URL
from local.functions import write_log


def post_to_discord(content, link, images=None, username="lynx-todon-otron", avatar_url=None, bluesky_link=None):
    """
    Posts a message to a Discord channel via webhook.

    Parameters:
        content (str): The message content to post.
        link (str): The link to the original Bluesky post.
        images (list): List of image file paths to upload.
        username (str): The username to post as. Default is "lynx-todon-otron".
        avatar_url (str): The avatar URL to use for the post. Default is None.
        bluesky_link (str): The link to the Bluesky post.
    """
    data = {
        "username": username,
        "content": f"{content}\nNew Lynx Content: {link}"
    }
    if bluesky_link:
        data["content"] += f"\n[Bluesky Post]({bluesky_link})"

    if avatar_url:
        data["avatar_url"] = avatar_url

    response = requests.post(DISCORD_WEBHOOK_URL, data=data)

    if response.status_code < 300:
        write_log("Posted to Discord successfully")
    else:
        write_log(f"Failed to post to Discord: {response.status_code} - {response.text}")


if __name__ == "__main__":
    if settings.Discord:
        post_content = "Hello, this is a test message from the bot."
        post_link = "https://bluesky.link/to/original/post"
        post_images = ["path_to_image1.jpg", "path_to_image2.jpg"]
        bluesky_link = "https://bsky.app/post/example"
        post_to_discord(post_content, post_link, post_images, bluesky_link=bluesky_link)
