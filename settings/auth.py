import os

# All necessary tokens, passwords, etc.
# Your bluesky handle should include your instance, so for example handle.bsky.social if you are on the main one.
BSKY_HANDLE = ""
# Generate an app password in the settings on bluesky. DO NOT use your main password.
BSKY_PASSWORD = ""
BSKY_SESSION_STRING = ""
# Your mastodon handle. Not needed for authentication, but used for making "quote posts".
MASTODON_HANDLE = ""
# The mastodon instance your account is on.
MASTODON_INSTANCE = ""
# Generate your token in the development settings on your mastodon account. Token must have the permissions to
# post statuses (write:statuses)
MASTODON_TOKEN = ""
# Get api keys and tokens from the twitter developer portal (developer.twitter.com). You need to create a project
# and make sure the access token and secret has read and write permissions.

TWITTER_APP_KEY = ""
TWITTER_APP_SECRET = ""
TWITTER_ACCESS_TOKEN = ""
TWITTER_ACCESS_TOKEN_SECRET = ""

DISCORD_WEBHOOK_URL = ""

TUMBLR_CONSUMER_KEY = ""
TUMBLR_CONSUMER_SECRET = ""
TUMBLR_OAUTH_TOKEN = ""
TUMBLR_OAUTH_SECRET = ""
TUMBLR_BLOG_NAME = ""  # Add Tumblr blog name without .tumblr.com

INSTAGRAM_API_KEY = ""

# Override settings with environment variables if they exist
BSKY_HANDLE = os.environ.get('BSKY_HANDLE') if os.environ.get('BSKY_HANDLE') else BSKY_HANDLE
BSKY_PASSWORD = os.environ.get('BSKY_PASSWORD') if os.environ.get('BSKY_PASSWORD') else BSKY_PASSWORD
BSKY_SESSION_STRING = os.environ.get('BSKY_SESSION_STRING') if os.environ.get('BSKY_SESSION_STRING') else BSKY_SESSION_STRING
MASTODON_INSTANCE = os.environ.get('MASTODON_INSTANCE') if os.environ.get('MASTODON_INSTANCE') else MASTODON_INSTANCE
MASTODON_HANDLE = os.environ.get('MASTODON_HANDLE') if os.environ.get('MASTODON_HANDLE') else MASTODON_HANDLE
MASTODON_TOKEN = os.environ.get('MASTODON_TOKEN') if os.environ.get('MASTODON_TOKEN') else MASTODON_TOKEN
TWITTER_APP_KEY = os.environ.get('TWITTER_APP_KEY') if os.environ.get('TWITTER_APP_KEY') else TWITTER_APP_KEY
TWITTER_APP_SECRET = os.environ.get('TWITTER_APP_SECRET') if os.environ.get('TWITTER_APP_SECRET') else TWITTER_APP_SECRET
TWITTER_ACCESS_TOKEN = os.environ.get('TWITTER_ACCESS_TOKEN') if os.environ.get('TWITTER_ACCESS_TOKEN') else TWITTER_ACCESS_TOKEN
TWITTER_ACCESS_TOKEN_SECRET = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET') if os.environ.get('TWITTER_ACCESS_TOKEN_SECRET') else TWITTER_ACCESS_TOKEN_SECRET
TUMBLR_CONSUMER_KEY = os.environ.get('TUMBLR_CONSUMER_KEY') if os.environ.get('TUMBLR_CONSUMER_KEY') else TUMBLR_CONSUMER_KEY
TUMBLR_CONSUMER_SECRET = os.environ.get('TUMBLR_CONSUMER_SECRET') if os.environ.get('TUMBLR_CONSUMER_SECRET') else TUMBLR_CONSUMER_SECRET
TUMBLR_OAUTH_TOKEN = os.environ.get('TUMBLR_OAUTH_TOKEN') if os.environ.get('TUMBLR_OAUTH_TOKEN') else TUMBLR_OAUTH_TOKEN
TUMBLR_OAUTH_SECRET = os.environ.get('TUMBLR_OAUTH_SECRET') if os.environ.get('TUMBLR_OAUTH_SECRET') else TUMBLR_OAUTH_SECRET
TUMBLR_BLOG_NAME = os.environ.get('TUMBLR_BLOG_NAME') if os.environ.get('TUMBLR_BLOG_NAME') else TUMBLR_BLOG_NAME  # Add this line
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL') if os.environ.get('DISCORD_WEBHOOK_URL') else DISCORD_WEBHOOK_URL
INSTAGRAM_API_KEY = os.environ.get('INSTAGRAM_API_KEY') if os.environ.get('INSTAGRAM_API_KEY') else INSTAGRAM_API_KEY

def save_session_string(session_string):
    global BSKY_SESSION_STRING
    BSKY_SESSION_STRING = session_string
    # Save to file or secure storage if needed
    with open("session.txt", "w") as file:
        file.write(session_string)

def load_session_string():
    global BSKY_SESSION_STRING
    try:
        with open("session.txt", "r") as file:
            BSKY_SESSION_STRING = file.read().strip()
    except FileNotFoundError:
        BSKY_SESSION_STRING = ""