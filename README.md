# LynxPoster

LynxPoster is a self-hosted FastAPI service for multi-persona social scheduling and automatic crossposting.

## Core concepts

- `Persona`: a self-contained bubble of posting defaults, retry/throttle rules, accounts, and routing.
- `Account`: one configured social account inside a persona. In v1 each persona supports one account per service.
- `CanonicalPost`: the internal post record that media, metadata, delivery jobs, and external references attach to.
- `AccountRoute`: an enabled source-account to destination-account edge used when imported posts fan out automatically.

## Current capabilities

- Manual scheduled posts with account-based destination targeting
- Automatic polling for `bluesky`, `instagram`, `mastodon`, and `telegram`
- Outbound publishing for `bluesky`, `instagram`, `mastodon`, `twitter`, `discord`, `telegram`, and `tumblr`
- One-click manual automation runs plus pause/start controls for the 5-minute autorun cycle
- A dry-run sandbox that previews per-account payload shapes, validations, and expectation checks without hitting live sites
- Safe first-scan baselining so new or reset source accounts do not repost historical content; autorun never performs first-sync historical imports, and any intentional backfill is manual-only
- Bootstrap admin UI with persona, account, routing, scheduled post, settings, sandbox, and logs pages
- Local run/alert logging plus optional notification webhooks for Home Assistant, Discord, or other receivers
- First-start import from legacy flat-file settings and history

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create the config directory and copy `env.example` to `app_data/config/.env`, or export the variables you want to use.

```bash
mkdir -p app_data/config
cp env.example app_data/config/.env
```

4. Start the service:

```bash
python crosspost.py
```

The app serves on `http://127.0.0.1:8000/`.

## Telegram notes

- Telegram accounts use a bot token plus a channel ID or `@channelusername`.
- For inbound polling, add the bot as an administrator in the source channel and leave Telegram in polling mode. The Bot API docs note that `getUpdates` will not work while an outgoing webhook is configured.
- Use a dedicated bot token for each source-enabled Telegram account. Telegram update offsets are bot-wide, so sharing one polling bot across multiple source accounts can skip updates.

## Instagram notes

- Instagram inbound polling still uses the Graph access token (`INSTAGRAM_API_KEY`).
- Instagram outbound publishing now uses `instagrapi` direct uploads, so `APP_BASE_URL` does not need to be public just for Instagram publishing.
- For publishing, configure either `INSTAGRAPI_SESSIONID` or both `INSTAGRAPI_USERNAME` and `INSTAGRAPI_PASSWORD`.
- Instagram giveaway webhooks use `INSTAGRAM_WEBHOOKS_ENABLED`, `INSTAGRAM_WEBHOOK_VERIFY_TOKEN`, and `INSTAGRAM_APP_SECRET`, and the Settings page shows the callback URL at `/webhooks/instagram`.
- `instagrapi` is an unofficial/private API client. Session IDs are often the most reliable option when Instagram challenge or MFA flows block password logins.

### Testing Instagram webhooks with a tunnel

When you want to test giveaway webhooks before deploying publicly:

1. Start LynxPoster locally, usually on `http://127.0.0.1:8000`.
2. Start a tunnel from the same machine.

```bash
cloudflared tunnel --url http://127.0.0.1:8000
```

or:

```bash
ngrok http 8000
```

3. Copy the public HTTPS tunnel URL into `APP_BASE_URL` or save it from the Settings page as `Public Base URL`.
4. Enable Instagram webhooks, save the verify token and app secret, and then use the callback URL shown in Settings.
5. In the Meta developer panel, paste:
   - callback URL: `https://<your-public-url>/webhooks/instagram`
   - verify token: the same `INSTAGRAM_WEBHOOK_VERIFY_TOKEN` value saved in LynxPoster
6. Subscribe to the giveaway fields shown in Settings, currently `comments`, `mentions`, `likes`, and `shares`.

Once the tunnel is live, a quick verification probe looks like:

```bash
curl "https://<your-public-url>/webhooks/instagram?hub.mode=subscribe&hub.verify_token=<your-token>&hub.challenge=test123"
```

If the app is reachable and the token matches, it returns `test123`.

## Settings UI

Use `http://127.0.0.1:8000/settings/page` to manage app-wide settings.

The Settings page currently lets you:

- Change the instance name
- Set the public base URL used by authentication callbacks
- Adjust the scheduler autorun interval
- Configure OIDC login for Authelia or another OpenID Connect provider
- Save notification settings for Home Assistant or other JSON webhook receivers
- Save a separate Discord notification webhook
- Configure Instagram webhook verification details for giveaway comment/story-mention capture
- Send a test notification after saving
- Review the active config and data paths

## Authentication Modes

- When `AUTH_OIDC_ENABLED=false`, LynxPoster uses a local user selector at `http://127.0.0.1:8000/auth/select`.
- Local mode seeds an admin account named `Lynx` automatically.
- You can also create additional local user accounts from that selector page for testing or non-OIDC use.
- When OIDC is enabled, the local selector is bypassed and external login takes over instead.
- Default OIDC scope is `openid profile email`. Only add `groups` if your provider and client are explicitly configured to allow that scope.

## Admin Users

- Use `http://127.0.0.1:8000/admin/users/page` as an admin to review users, owned personas, stored groups, and last login time.
- Admins can update a user timezone and disable access locally from that screen.
- Disabled users cannot sign in until they are re-enabled.

## Docker

Dependencies are installed inside the image from [requirements.txt](/workspaces/lynxposter/requirements.txt) during `docker build`.

For the full Linux and Docker deployment guide, including ports, environment variables, config precedence, and persistent data layout, see [docs/docker-linux-deployment.md](/workspaces/lynxposter/docs/docker-linux-deployment.md).

### Default container paths

- App data directory: `/data`
- Config directory: `/data/config`
- Config file: `/data/config/.env`
- Default port: `8000`

### Build arguments

- `PYTHON_VERSION`
  Default: `3.12-slim`
- `LYNXPOSTER_PYTHON_VERSION`
  Compose-only override for the Docker build arg. Default: `3.12-slim`

### Runtime environment

- `APP_PORT`
  Default: `8000`
- `APP_DATA_DIR`
  Container default: `/data`
- `APP_CONFIG_DIR`
  Container default: `/data/config`
- `APP_ENV_FILE`
  Container default: `/data/config/.env`

### Docker Compose

The included [compose.yaml](/workspaces/lynxposter/compose.yaml) mounts:

- `lynxposter_data:/data` for the database, configuration, uploads, logs, and backups
- `./docker/config:/data/config` so your `.env` survives image updates and stays inside app data

Before starting, copy [env.example](/workspaces/lynxposter/env.example) to `docker/config/.env` and fill in your values.

```bash
mkdir -p docker/config
cp env.example docker/config/.env
docker compose up --build -d
```

If you need to override the Python base image when using Compose, set `LYNXPOSTER_PYTHON_VERSION`, not the generic host `PYTHON_VERSION`.

The app will be available on `http://127.0.0.1:${APP_PORT:-8000}`.

The settings UI can also create and update `docker/config/.env` for you, as long as the mounted directory is writable by the container user.

The Docker image installs `ffmpeg`, so the container does not rely on the repo-local `ffmpeg` binary.

### Import existing local data

If you already have a working local install and want to carry its current settings, keys, database, uploads, and imported media into Docker, run:

```bash
bash docker/import-existing-install.sh
```

By default, the importer:

- copies your current config directory into `docker/config`
- imports your existing `app_data/` contents into the compose `/data` volume

Discovery order:

- data dir: `--source-data-dir`, then `$APP_DATA_DIR`, then `./app_data`
- config dir: `--source-config-dir`, then `$APP_CONFIG_DIR`, then the parent of `$APP_ENV_FILE`, then `<data dir>/config`

Useful options:

- `--replace-config` to clear `docker/config` before copying
- `--replace-volume` to clear the compose data volume before importing

Example:

```bash
bash docker/import-existing-install.sh --replace-config --replace-volume
docker compose up --build -d
```

### Plain Docker

```bash
docker build \
  --build-arg PYTHON_VERSION=3.12-slim \
  -t lynxposter:latest .

docker run -d \
  --name lynxposter \
  -p 8000:8000 \
  -e APP_PORT=8000 \
  -e APP_DATA_DIR=/data \
  -e APP_CONFIG_DIR=/data/config \
  -e APP_ENV_FILE=/data/config/.env \
  -v lynxposter_data:/data \
  -v "$(pwd)/docker/config:/data/config" \
  lynxposter:latest
```

If you want to override the port, change both `APP_PORT` and the published port mapping.

### Linux host validation

On the target Linux server, validate the deployment with:

```bash
docker compose up --build -d
docker compose ps
docker compose logs --tail=200
curl http://127.0.0.1:${APP_PORT:-8000}/health
```

If you use OIDC or Instagram webhooks, make sure `APP_BASE_URL` is set to the public URL that reaches the server.

## App data

All runtime-managed state lives under `APP_DATA_DIR`:

- `crossposter.db`: SQLite database
- `config/.env`: persisted environment configuration, including auth and webhook settings
- `uploads/`: scheduled-post uploads
- `imported_media/`: downloaded source media used for crossposting
- `logs/`: runtime log assets
- `backups/`: backed-up legacy artifacts and pre-refactor database copies

## JSON API

- `GET /health`
- `GET /scheduler/status`
- `POST /scheduler/run`
- `POST /scheduler/pause`
- `POST /scheduler/start`
- `GET /settings`
- `PUT /settings`
- `GET /account`
- `PUT /account`
- `GET /admin/users`
- `PUT /admin/users/{user_id}`
- `POST /sandbox/preview`
- `GET /personas`
- `POST /personas`
- `GET /personas/{id}`
- `PUT /personas/{id}`
- `GET /personas/{id}/accounts`
- `POST /personas/{id}/accounts`
- `PUT /personas/{id}/accounts/{account_id}`
- `GET /personas/{id}/routes`
- `PUT /personas/{id}/routes`
- `GET /scheduled-posts`
- `POST /scheduled-posts`
- `GET /scheduled-posts/{id}`
- `PUT /scheduled-posts/{id}`
- `POST /scheduled-posts/{id}/send-now`
- `GET /runs/recent`
- `GET /errors/recent`

## Tests

```bash
pytest -q
```

## Dev container notes

- The repo may contain a Windows `.venv/` when you move between Windows and the Linux dev container.
- The dev container uses a separate Linux virtual environment at `/home/vscode/.venvs/lynxposter/` for VS Code, pytest discovery, and dependency installs.
- If VS Code still points at the wrong interpreter after opening the container, run `Dev Containers: Rebuild Container` or `Python: Select Interpreter` and choose `/home/vscode/.venvs/lynxposter/bin/python`.
