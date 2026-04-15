import requests
import arrow
import random
import string
import urllib.request
from settings.auth import INSTAGRAM_API_KEY
from settings.paths import image_path
from local.functions import write_log

def get_images(images):
    local_images = []
    for image in images:
        url: str = image["url"]
        alt: str = image["alt"]
        type = ".mp4" if ".mp4" in url else ".jpg"
        filename = ''.join(random.choice(string.ascii_lowercase) for i in range(10)) + type
        filepath = image_path + filename
        urllib.request.urlretrieve(url, filepath)
        image_info = {
            "filename": filepath,
            "alt": alt
        }
        local_images.append(image_info)
    return local_images

def get_instagram_posts(timelimit=arrow.utcnow().shift(hours=-1)):
    write_log("Gathering Instagram posts")
    posts = {}
    url = f"https://graph.instagram.com/me/media?fields=id,caption,media_url,timestamp,media_type,children&access_token={INSTAGRAM_API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        write_log(f"Failed to fetch Instagram posts: {response.status_code} - {response.text}", "error")
        return posts

    media_list = response.json().get('data', [])
    for media in media_list:
        created_at = arrow.get(media['timestamp'])
        if created_at > timelimit:
            images = []
            if media['media_type'] == 'CAROUSEL_ALBUM':
                children_url = f"https://graph.instagram.com/{media['id']}/children?fields=media_url&access_token={INSTAGRAM_API_KEY}"
                children_response = requests.get(children_url)
                if children_response.status_code == 200:
                    children_data = children_response.json().get('data', [])
                    for child in children_data:
                        images.append({"url": child.get('media_url', ''), "alt": ''})
            else:
                images.append({"url": media.get('media_url', ''), "alt": ''})

            images = get_images(images)
            post_info = {
                "text": media.get('caption', ''),
                "reply_to_post": "",  # Add default value
                "quoted_post": "",  # Add default value
                "quote_url": "",  # Add default value
                "link": "",  # Add default value
                "images": images,
                "visibility": "public",  # Set default visibility
                "allowed_reply": "All",  # Set default allowed replies
                "repost": False,  # Set default repost value
                "timestamp": created_at,
                "instagram": True  # Mark as an Instagram post
            }
            posts[media['id']] = post_info

    return posts

# Test function
if __name__ == "__main__":
    posts = get_instagram_posts()
    for post_id, post in posts.items():
        print(
            f"Post ID: {post_id}, Text: {post['text']}, Image URLs: {post['images']}, Timestamp: {post['timestamp']}")
