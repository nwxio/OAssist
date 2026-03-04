# OAssist Developer Guide

## 1) Architecture

OAssist is a FastAPI service (`app/main.py`) with a background sync worker (`worker/main.py`), Qdrant vector storage, optional Ollama local runtime, and a single-page UI (`app/static/chat.html`).

Main flow:

1. User authenticates (local auth or OIDC).
2. UI loads profile state from `/chat/state` and access metadata from `/acl/me`.
3. Assistant query uses retrieval + LLM provider selection.
4. Sync worker periodically re-indexes Outline/Notion into Qdrant.

## 2) Authentication and Roles

### Local auth

- Backed by SQLite (`users`, `sessions`) in `AUTH_DB_PATH`.
- Bootstrap admin is created from:
  - `AUTH_BOOTSTRAP_ADMIN_USERNAME`
  - `AUTH_BOOTSTRAP_ADMIN_PASSWORD`

### OIDC auth (Keycloak-compatible)

- Runtime config in `.env` via `AUTH_OIDC_*`.
- Login flow endpoints:
  - `GET /auth/oidc/config`
  - `GET /auth/oidc/start`
  - `GET /auth/oidc.callback`
- Admin runtime toggle:
  - `GET /auth/oidc/global`
  - `PUT /auth/oidc/global`

### Role model

- `admin` and `user` roles exist at DB level.
- Super-admin is the local account with username `admin`.
- Super-admin-only controls:
  - default ACL (`/acl/global`)
  - global source toggles (`/sources/global`)
  - OIDC login switch (`/auth/oidc/global`)
- Regular admins can manage users but are restricted on super-admin/default-global operations.

### Session method

- Session includes `auth_method` (`local` / `oidc`).
- UI hides local account-management surfaces for OIDC sessions.

## 3) Source Model

Supported sources:

- `outline`
- `notion`
- `files`

Global source switches are persisted in DB (`source_global_settings`), and enforced in both UI and backend access checks.

`files` source specifics:

- Roots are managed in DB (`files_roots`) and mapped to `document_id` format `files:<root_id>:<relative_path>`.
- Runtime settings are stored in `files_settings` (`read-only`/`read-write`, upload/artifact policy).
- Tree is exposed by `/files/tree` with ACL filtering the same way as Outline/Notion.

## 4) ACL Model

### Global default ACL

- Stored in `acl_global_defaults`.
- Acts as baseline selection for users that do not have personal overrides.

### User ACL override

- Stored in `acl_user_overrides`.
- Effective ACL for non-super-admin users:
  - if user override is non-empty -> use user override
  - else -> use global default ACL

### Important super-admin UX behavior

When super-admin opens user/admin ACL editor:

- full tree for target user sources is visible,
- default ACL documents are pre-selected when target has no override,
- super-admin can add/remove explicit selections and save.

Regular admins continue to operate under default-scope restrictions.

## 5) Chat State Isolation

### Backend isolation

- `chat_states` table key is `user_id` (`PRIMARY KEY`).
- `/chat/state*` reads/writes by current session user only.

### Frontend profile keying

- Local profile key: `u:<user.id>`.
- Anonymous profile key: `__anon`.
- This prevents cross-user chat mixing in normal operation.

## 6) Indexing and Retrieval

### Sync

- Worker executes `run_full_sync` periodically.
- API can trigger sync manually (`/sync/start`, `/sync/full`).

### Document ID normalization

All indexed `document_id` values are normalized to `source:native_id`:

- `outline:<id>`
- `notion:<id>`

Legacy/broken forms are cleaned during sync, and retrieval normalizes IDs before filtering/merging.

### Embeddings

- `EMBEDDING_PROVIDER=ollama|openai`
- Ollama embedding requests are batched for better throughput.

## 7) UI State Sync Performance

`chat.html` uses two save modes:

- immediate canonical save for meaningful chat/config changes,
- debounced local-only save for noisy input fields (search filters etc).

This reduces unnecessary remote `/chat/state` traffic while keeping UX responsive.

## 8) Endpoint Catalog

### System/UI

- `GET /health`
- `GET /health/providers`
- `GET /`
- `GET /ui`

### Auth

- `POST /auth/login`
- `POST /auth/logout`
- `GET /auth/me`
- `POST /auth/me/change-password`
- `GET /auth/users`
- `POST /auth/users`
- `PATCH /auth/users/{id}`
- `DELETE /auth/users/{id}`

### OIDC auth and toggle

- `GET /auth/oidc/config`
- `GET /auth/oidc/start`
- `GET /auth/oidc.callback`
- `GET /auth/oidc/global`
- `PUT /auth/oidc/global`

### Sources and Notion

- `GET /outline/tree`
- `GET /notion/tree`
- `GET /notion/connection`
- `POST /notion/connection/token`
- `DELETE /notion/connection`
- `GET /notion/oauth/start`
- `GET /notion/oauth/callback`
- `GET /sources/global`
- `PUT /sources/global`

### ACL

- `GET /acl/global`
- `PUT /acl/global`
- `GET /acl/users/{id}`
- `PUT /acl/users/{id}`
- `GET /acl/me`

### Chat and tools

- `GET /chat/state`
- `PUT /chat/state`
- `GET /chat/state/meta`
- `POST /chat`
- `POST /assistant/chat`
- `POST /assistant/chat/stream`
- `POST /chat/uploads`
- `GET /chat/uploads/{id}`
- `DELETE /chat/uploads/{id}`
- `POST /assistant/artifacts/generate`
- `GET /assistant/artifacts/{id}`
- `GET /assistant/artifacts/{id}/download`
- `DELETE /assistant/artifacts/{id}`
- `POST /tasks/summarize`
- `POST /tasks/rewrite`
- `POST /tasks/translate`

### Files source and write ops

- `GET /files/roots`
- `POST /files/roots`
- `PATCH /files/roots/{id}`
- `DELETE /files/roots/{id}`
- `GET /files/settings`
- `PUT /files/settings`
- `GET /files/tree`
- `GET /files/document`
- `POST /files/write/preview`
- `POST /files/write/apply`
- `POST /files/create`
- `POST /files/move`
- `POST /files/delete`
- `GET /files/audit`

### Sync

- `POST /sync/full`
- `POST /sync/start`
- `GET /sync/status`

## 9) Operational Checklist

After any auth/ACL/index/files change:

1. `python3 -m py_compile app/*.py worker/*.py`
2. `docker compose up -d --build oassist-api oassist-worker`
3. Validate:
   - login (local + OIDC if enabled)
   - `/acl/me` response correctness
   - user manager ACL save/load
   - `/outline/tree` visibility for target users
4. Run one full sync if index logic changed:
   - `POST /sync/full` or worker cycle
5. Validate files flow if enabled:
   - `/files/tree` returns roots/files with ACL filtering
   - upload endpoint accepts allowed formats and respects policy limits
   - artifact download works and respects owner access

## 10) Troubleshooting

### OIDC says `Invalid parameter: redirect_uri`

- Ensure Keycloak client matches `AUTH_OIDC_CLIENT_ID`.
- Add exact redirect URI from `AUTH_OIDC_REDIRECT_URI` to client `Valid Redirect URIs`.

### OIDC says user not in allowed groups

- Verify group claim mapper (`groups` by default).
- Check values match `AUTH_OIDC_ADMIN_GROUP` / `AUTH_OIDC_USER_GROUP`.

### Document visible in tree but not used in answer

- Check effective ACL (`GET /acl/me`).
- Check `use_knowledge=true` in assistant payload.
- Verify document is present in index (post-sync).

### Chat states look stale

- Check `/chat/state/meta` timestamps.
- Confirm browser has latest JS (`Ctrl+F5`).
