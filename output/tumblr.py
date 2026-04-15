import pytumblr
import re
from settings.auth import TUMBLR_CONSUMER_KEY, TUMBLR_CONSUMER_SECRET, TUMBLR_OAUTH_TOKEN, TUMBLR_OAUTH_SECRET, \
    TUMBLR_BLOG_NAME
from local.functions import write_log

# Initialize the Tumblr client
tumblr_client = pytumblr.TumblrRestClient(
    TUMBLR_CONSUMER_KEY,
    TUMBLR_CONSUMER_SECRET,
    TUMBLR_OAUTH_TOKEN,
    TUMBLR_OAUTH_SECRET
)


# Function to extract hashtags from the post text
def extract_hashtags(text):
    hashtags = re.findall(r'#\w+', text)
    return [tag.strip('#') for tag in hashtags]  # Remove the '#' for Tumblr tags


def post_to_tumblr(post, media=None):
    try:
        hashtags = extract_hashtags(post)  # Extract hashtags from the post text
        hashtags = hashtags if hashtags else ""

        if media:
            # Separate images and videos
            photoset = []
            video = None
            for item in media:
                if item["filename"].endswith((".jpg", ".jpeg", ".png", ".gif")):
                    photoset.append(item["filename"])
                elif item["filename"].endswith(".mp4"):
                    video = item["filename"]

            # Post as a video if there's an .mp4 file
            if video:
                response = tumblr_client.create_video(
                    TUMBLR_BLOG_NAME,
                    state="published",
                    caption=post,  # Use the post text as the caption
                    data=video,  # Video file
                    tags=hashtags  # Add the extracted hashtags as tags
                )
            # Post as a photo if there are images
            elif photoset:
                response = tumblr_client.create_photo(
                    TUMBLR_BLOG_NAME,
                    state="published",
                    caption=post,  # Use the post text as the caption
                    data=photoset,  # List of image files
                    tags=hashtags  # Add the extracted hashtags as tags
                )

            if 'id' in response:
                write_log("Posted to Tumblr successfully")
                return response['id']
            else:
                write_log(f"Failed to upload media to Tumblr: {response}")
                return None
        else:
            # If no media, create a regular text post with tags
            response = tumblr_client.create_text(
                TUMBLR_BLOG_NAME,
                state="published",
                title="",
                body=post,
                tags=hashtags  # Add the extracted hashtags as tags
            )

            if 'id' in response:
                write_log("Posted to Tumblr successfully")
                return response['id']
            else:
                write_log(f"Failed to post to Tumblr: {response}")
                return None
    except Exception as e:
        write_log(f"Failed to post to Tumblr: {e}", "error")
        return None
