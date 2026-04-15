# Docker and Linux Deployment

This guide documents the current Docker and Linux runtime model for LynxPoster.

## Runtime model

LynxPoster is designed to run as a single FastAPI container on Linux:

- the app listens on `0.0.0.0`
- the port is configurable with `APP_PORT`
- runtime-managed state is written under `APP_DATA_DIR`
- the settings UI can create and update the env file at `APP_ENV_FILE` or `APP_CONFIG_DIR/.env`
- `ffmpeg` is installed in the container image for media workflows that require it

## Host requirements

- 64-bit Linux host
- Docker Engine with Compose v2
- outbound HTTPS access to the social providers and any OIDC issuer
- persistent storage for `/data`

Optional but common:

- reverse proxy terminating TLS in front of LynxPoster
- public hostname if you use OIDC callbacks or Instagram webhooks

## Network and ports

### Container listener

- default internal port: `8000`
- configured by: `APP_PORT`
- health endpoint: `GET /health`

### Common external routes

- app UI: `/`
- settings UI: `/settings/page`
- OIDC callback: `/auth/callback`
- Instagram webhook verify/callback: `/webhooks/instagram`
- health probe: `/health`

### Firewall / reverse proxy notes

- if you publish the container directly, expose the app port, usually `8000`
- if you run behind a reverse proxy, only the proxy needs to be public
- OIDC and Instagram webhooks both require `APP_BASE_URL` to match the externally reachable URL

## Container paths

Default container paths:

| Purpose | Path |
|---|---|
| App data root | `/data` |
| Config directory | `/data/config` |
| Env file | `/data/config/.env` |
| SQLite database | `/data/crossposter.db` |
| Uploads | `/data/uploads` |
| Imported media | `/data/imported_media` |
| Logs | `/data/logs` |
| Backups | `/data/backups` |

The app creates missing runtime directories automatically during bootstrap.

## Configuration precedence

LynxPoster resolves configuration in this order:

1. `APP_ENV_FILE` if explicitly set
2. `APP_CONFIG_DIR/.env`
3. legacy repo-root `.env` if present, copied into the config directory on first load

For Docker deployments, the recommended path is:

- `APP_DATA_DIR=/data`
- `APP_CONFIG_DIR=/data/config`
- env file at `/data/config/.env`

## Persistent storage model

The included `compose.yaml` uses:

- a named volume for `/data`
- a bind mount for `./docker/config` to `/data/config`

That means:

- the database, uploads, logs, and backups live in the Docker-managed volume
- the editable `.env` stays in the repo-local `docker/config` directory

If you prefer fully host-visible data instead of a named volume, bind mount a host directory to `/data`.

Example:

```yaml
services:
  lynxposter:
    volumes:
      - ./docker/data:/data
```

If you use host bind mounts on Linux, make sure the mounted directory is writable by the container user.

## Environment variables

### Core runtime

| Variable | Default | Purpose |
|---|---|---|
| `APP_PORT` | `8000` | HTTP listen port |
| `APP_DATA_DIR` | repo `app_data/` locally, `/data` in Docker | Runtime data root |
| `APP_CONFIG_DIR` | `<APP_DATA_DIR>/config` | Config directory |
| `APP_ENV_FILE` | unset | Optional explicit env file path |
| `APP_INSTANCE_NAME` | hostname | Instance label shown in logs and health |
| `APP_BASE_URL` | unset | Public URL used for callbacks and external links |

### Scheduler

| Variable | Default | Purpose |
|---|---|---|
| `SCHEDULER_AUTORUN_INTERVAL_SECONDS` | `300` | Automation polling cadence |

### Notification webhooks

| Variable | Default | Purpose |
|---|---|---|
| `WEBHOOK_LOGGING_ENABLED` | `false` | Enable generic JSON webhook notifications |
| `WEBHOOK_LOGGING_ENDPOINT` | unset | Generic webhook target |
| `WEBHOOK_LOGGING_BEARER_TOKEN` | unset | Optional bearer token |
| `WEBHOOK_LOGGING_TIMEOUT_SECONDS` | `10` | Outbound timeout |
| `WEBHOOK_LOGGING_RETRY_COUNT` | `2` | Outbound retries |
| `WEBHOOK_LOGGING_MIN_SEVERITY` | `warning` | Minimum severity to send |
| `DISCORD_NOTIFICATION_WEBHOOK_ENABLED` | `false` | Enable Discord webhook notifications |
| `DISCORD_NOTIFICATION_WEBHOOK_URL` | unset | Discord webhook URL |
| `DISCORD_NOTIFICATION_WEBHOOK_USERNAME` | `LynxPoster` | Display username |
| `DISCORD_NOTIFICATION_MIN_SEVERITY` | `warning` | Minimum severity to send |

### Authentication

| Variable | Default | Purpose |
|---|---|---|
| `AUTH_OIDC_ENABLED` | `false` | Enable OIDC login |
| `AUTH_OIDC_ISSUER_URL` | unset | OIDC issuer base URL |
| `AUTH_OIDC_CLIENT_ID` | unset | OIDC client ID |
| `AUTH_OIDC_CLIENT_SECRET` | unset | OIDC client secret |
| `AUTH_OIDC_SCOPE` | `openid profile email` | Requested OIDC scope |
| `AUTH_OIDC_GROUPS_CLAIM` | `groups` | Groups claim name |
| `AUTH_OIDC_USERNAME_CLAIM` | `preferred_username` | Username/display claim |
| `AUTH_OIDC_ADMIN_GROUPS` | unset | Comma or space separated admin groups |
| `AUTH_OIDC_USER_GROUPS` | unset | Comma or space separated user groups |
| `AUTH_SESSION_SECRET` | dev default | Session signing secret |

### Instagram webhooks / giveaway capture

| Variable | Default | Purpose |
|---|---|---|
| `INSTAGRAM_WEBHOOKS_ENABLED` | `false` | Enable Instagram webhook routes |
| `INSTAGRAM_WEBHOOK_VERIFY_TOKEN` | unset | Meta webhook verify token |
| `INSTAGRAM_APP_SECRET` | unset | Meta app secret for signature validation |

### Legacy import and account seeding

These are optional and mostly useful when migrating from the old flat config model. See [env.example](../env.example) for the full list.

Notable examples:

- `BSKY_HANDLE`
- `MASTODON_TOKEN`
- `TWITTER_ACCESS_TOKEN`
- `DISCORD_WEBHOOK_URL`
- `TELEGRAM_BOT_TOKEN`
- `TUMBLR_OAUTH_TOKEN`
- `INSTAGRAM_API_KEY`
- `INSTAGRAPI_USERNAME`
- `INSTAGRAPI_PASSWORD`
- `INSTAGRAPI_SESSIONID`

## Recommended Linux deployment flow

1. Create the config directory:

```bash
mkdir -p docker/config
cp env.example docker/config/.env
```

2. Edit `docker/config/.env` with at least:

- `APP_BASE_URL` if using reverse proxy, OIDC, or webhooks
- `AUTH_SESSION_SECRET`
- any provider credentials you need

3. Start the container:

```bash
docker compose up --build -d
```

4. Validate the service:

```bash
curl http://127.0.0.1:8000/health
```

5. Sign in to the UI and review the resolved paths in `Settings -> Paths`.

## Import an existing local install

If you are migrating from a current non-Docker LynxPoster checkout and want to keep the existing keys, settings, database, uploads, and imported media:

```bash
bash docker/import-existing-install.sh --replace-config --replace-volume
docker compose up --build -d
```

The importer copies:

- your current config directory into `docker/config`
- your existing app data into the compose `/data` volume

Default discovery order:

- data dir: `--source-data-dir`, then `APP_DATA_DIR`, then `./app_data`
- config dir: `--source-config-dir`, then `APP_CONFIG_DIR`, then the parent of `APP_ENV_FILE`, then `<data dir>/config`

If you want to preserve any existing Docker-side data and only merge files in, omit the `--replace-*` flags.

## Operational notes

- The container runs as a non-root application user.
- The SQLite database lives inside `APP_DATA_DIR`; back up `/data` or the named volume regularly.
- If OIDC login fails on Linux, verify the container can reach the issuer URL from inside the host network.
- If Instagram webhooks are enabled, make sure the public callback URL in Meta exactly matches `APP_BASE_URL + /webhooks/instagram`.

## Suggested host-side validation

Run these on the target Linux server:

```bash
docker compose up --build -d
docker compose ps
docker compose logs --tail=200
curl http://127.0.0.1:8000/health
```

If you use a reverse proxy, also verify:

- the proxy forwards requests to the container port
- the externally visible URL matches `APP_BASE_URL`
- OIDC callback and Instagram webhook routes are reachable publicly
