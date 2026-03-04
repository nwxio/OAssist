from __future__ import annotations

import json
import hashlib
import hmac
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.config import Settings
from app.document_ids import SOURCE_FILES, SOURCE_MODE_OUTLINE, SOURCE_NOTION, SOURCE_OUTLINE, normalize_document_id, normalize_source_mode


ROLE_ADMIN = "admin"
ROLE_USER = "user"
VALID_ROLES = {ROLE_ADMIN, ROLE_USER}

AUTH_METHOD_LOCAL = "local"
AUTH_METHOD_OIDC = "oidc"
VALID_AUTH_METHODS = {AUTH_METHOD_LOCAL, AUTH_METHOD_OIDC}

NOTION_CONNECTION_MODE_TOKEN = "token"
NOTION_CONNECTION_MODE_OAUTH = "oauth"
VALID_NOTION_CONNECTION_MODES = {NOTION_CONNECTION_MODE_TOKEN, NOTION_CONNECTION_MODE_OAUTH}
SUPER_ADMIN_USERNAME = "admin"

GLOBAL_SOURCE_DEFAULTS = {
    SOURCE_OUTLINE: True,
    SOURCE_NOTION: True,
    SOURCE_FILES: True,
}

AUTH_GLOBAL_KEY_OIDC_LOGIN_ENABLED = "oidc_login_enabled"
AUTH_GLOBAL_KEY_OIDC_LOGIN_SET_BY_USER = "oidc_login_set_by_user"
AUTH_GLOBAL_DEFAULTS = {
    AUTH_GLOBAL_KEY_OIDC_LOGIN_ENABLED: False,
    AUTH_GLOBAL_KEY_OIDC_LOGIN_SET_BY_USER: False,
}


@dataclass
class AuthUser:
    id: int
    username: str
    role: str
    is_active: bool
    source_mode: str
    auth_method: str = AUTH_METHOD_LOCAL


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _utc_now_precise() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _hash_password(password: str, salt_hex: str) -> str:
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 310_000)
    return digest.hex()


def _new_salt() -> str:
    return secrets.token_bytes(16).hex()


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_auth_db(settings: Settings) -> None:
    path = Path(settings.auth_db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with _connect(settings.auth_db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT NOT NULL UNIQUE,
              password_hash TEXT NOT NULL,
              salt TEXT NOT NULL,
              role TEXT NOT NULL,
              is_active INTEGER NOT NULL DEFAULT 1,
              source_mode TEXT NOT NULL DEFAULT 'outline',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
        user_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(users)").fetchall()}
        if "source_mode" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN source_mode TEXT NOT NULL DEFAULT 'outline'")
        conn.execute(
            "UPDATE users SET source_mode=? WHERE source_mode IS NULL OR trim(source_mode)=''",
            (SOURCE_MODE_OUTLINE,),
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              token_hash TEXT NOT NULL UNIQUE,
              user_id INTEGER NOT NULL,
              auth_method TEXT NOT NULL DEFAULT 'local',
              created_at TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        session_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(sessions)").fetchall()}
        if "auth_method" not in session_columns:
            conn.execute("ALTER TABLE sessions ADD COLUMN auth_method TEXT NOT NULL DEFAULT 'local'")
        conn.execute(
            "UPDATE sessions SET auth_method='local' WHERE auth_method IS NULL OR trim(auth_method)=''"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_states (
              user_id INTEGER PRIMARY KEY,
              state_json TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS acl_global_defaults (
              document_id TEXT PRIMARY KEY,
              updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS acl_user_overrides (
              user_id INTEGER NOT NULL,
              document_id TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY(user_id, document_id),
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notion_connections (
              user_id INTEGER PRIMARY KEY,
              mode TEXT NOT NULL,
              access_token TEXT NOT NULL,
              workspace_id TEXT,
              workspace_name TEXT,
              bot_id TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notion_oauth_states (
              state TEXT PRIMARY KEY,
              user_id INTEGER NOT NULL,
              expires_at TEXT NOT NULL,
              created_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_global_settings (
              source TEXT PRIMARY KEY,
              is_enabled INTEGER NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_global_settings (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_oidc_states (
              state TEXT PRIMARY KEY,
              return_to TEXT,
              expires_at TEXT NOT NULL,
              created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_oidc_login_tickets (
              ticket_hash TEXT PRIMARY KEY,
              user_id INTEGER NOT NULL,
              expires_at TEXT NOT NULL,
              created_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        now = _utc_now()
        conn.executemany(
            """
            INSERT INTO source_global_settings (source, is_enabled, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(source) DO NOTHING
            """,
            [
                (SOURCE_OUTLINE, 1, now),
                (SOURCE_NOTION, 1, now),
                (SOURCE_FILES, 1, now),
            ],
        )
        conn.executemany(
            """
            INSERT INTO auth_global_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO NOTHING
            """,
            [
                (AUTH_GLOBAL_KEY_OIDC_LOGIN_ENABLED, "1", now),
                (AUTH_GLOBAL_KEY_OIDC_LOGIN_SET_BY_USER, "0", now),
            ],
        )

        if bool(getattr(settings, "auth_oidc_enabled", False)):
            flag_row = conn.execute(
                "SELECT value FROM auth_global_settings WHERE key=?",
                (AUTH_GLOBAL_KEY_OIDC_LOGIN_SET_BY_USER,),
            ).fetchone()
            set_by_user = str(flag_row["value"] if flag_row else "0").strip().lower() in {"1", "true", "yes", "on"}
            if not set_by_user:
                conn.execute(
                    "UPDATE auth_global_settings SET value=?, updated_at=? WHERE key=?",
                    ("1", now, AUTH_GLOBAL_KEY_OIDC_LOGIN_ENABLED),
                )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_acl_user_overrides_user ON acl_user_overrides(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_notion_oauth_states_expires ON notion_oauth_states(expires_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_oidc_states_expires ON auth_oidc_states(expires_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_oidc_login_tickets_expires ON auth_oidc_login_tickets(expires_at)")
        conn.commit()

    _bootstrap_default_admin(settings)


def _bootstrap_default_admin(settings: Settings) -> None:
    username = settings.auth_bootstrap_admin_username.strip()
    password = settings.auth_bootstrap_admin_password.strip()
    if not username or not password:
        return

    with _connect(settings.auth_db_path) as conn:
        row = conn.execute("SELECT id FROM users LIMIT 1").fetchone()
        if row:
            return
    create_user(settings, username=username, password=password, role=ROLE_ADMIN)


def _row_to_user(row: sqlite3.Row | None) -> AuthUser | None:
    if row is None:
        return None
    return AuthUser(
        id=int(row["id"]),
        username=str(row["username"]),
        role=str(row["role"]),
        is_active=bool(int(row["is_active"])),
        source_mode=normalize_source_mode(str(row["source_mode"] if "source_mode" in row.keys() else SOURCE_MODE_OUTLINE)),
        auth_method=(
            str(row["auth_method"]).strip().lower()
            if "auth_method" in row.keys() and str(row["auth_method"]).strip().lower() in VALID_AUTH_METHODS
            else AUTH_METHOD_LOCAL
        ),
    )


def authenticate_user(settings: Settings, username: str, password: str) -> AuthUser | None:
    uname = username.strip()
    if not uname or not password:
        return None

    with _connect(settings.auth_db_path) as conn:
        row = conn.execute(
            "SELECT id, username, role, is_active, password_hash, salt FROM users WHERE lower(username)=lower(?)",
            (uname,),
        ).fetchone()
        if row is None:
            return None
        if not bool(int(row["is_active"])):
            return None

        expected = str(row["password_hash"])
        got = _hash_password(password, str(row["salt"]))
        if not hmac.compare_digest(expected, got):
            return None
        return _row_to_user(row)


def create_session(settings: Settings, user_id: int, *, auth_method: str = AUTH_METHOD_LOCAL) -> str:
    token = secrets.token_urlsafe(32)
    now = datetime.now(UTC)
    expires = now + timedelta(hours=settings.auth_session_ttl_hours)
    method = str(auth_method or AUTH_METHOD_LOCAL).strip().lower()
    if method not in VALID_AUTH_METHODS:
        method = AUTH_METHOD_LOCAL

    with _connect(settings.auth_db_path) as conn:
        conn.execute(
            "INSERT INTO sessions (token_hash, user_id, auth_method, created_at, expires_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                _token_hash(token),
                int(user_id),
                method,
                now.isoformat(timespec="seconds"),
                expires.isoformat(timespec="seconds"),
                now.isoformat(timespec="seconds"),
            ),
        )
        conn.execute("DELETE FROM sessions WHERE expires_at < ?", (_utc_now(),))
        conn.commit()
    return token


def revoke_session(settings: Settings, token: str) -> None:
    if not token:
        return
    with _connect(settings.auth_db_path) as conn:
        conn.execute("DELETE FROM sessions WHERE token_hash=?", (_token_hash(token),))
        conn.commit()


def get_user_by_session(settings: Settings, token: str) -> AuthUser | None:
    if not token:
        return None

    with _connect(settings.auth_db_path) as conn:
        row = conn.execute(
            """
            SELECT u.id, u.username, u.role, u.is_active, u.source_mode, s.auth_method, s.id AS session_id
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token_hash = ? AND s.expires_at > ?
            """,
            (_token_hash(token), _utc_now()),
        ).fetchone()
        user = _row_to_user(row)
        if user is None or not user.is_active:
            return None

        conn.execute("UPDATE sessions SET last_seen_at=? WHERE id=?", (_utc_now(), int(row["session_id"])))
        conn.commit()
        return user


def list_users(settings: Settings) -> list[dict[str, Any]]:
    with _connect(settings.auth_db_path) as conn:
        rows = conn.execute(
            """
            SELECT
              u.id,
              u.username,
              u.role,
              u.is_active,
              u.source_mode,
              u.created_at,
              u.updated_at,
              CASE WHEN nc.user_id IS NULL THEN 0 ELSE 1 END AS notion_connected
            FROM users u
            LEFT JOIN notion_connections nc ON nc.user_id = u.id
            ORDER BY u.username
            """
        ).fetchall()
    return [
        {
            "id": int(row["id"]),
            "username": str(row["username"]),
            "role": str(row["role"]),
            "is_active": bool(int(row["is_active"])),
            "source_mode": normalize_source_mode(str(row["source_mode"] or SOURCE_MODE_OUTLINE)),
            "notion_connected": bool(int(row["notion_connected"])),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }
        for row in rows
    ]


def create_user(
    settings: Settings,
    username: str,
    password: str,
    role: str,
    source_mode: str = SOURCE_MODE_OUTLINE,
) -> dict[str, Any]:
    uname = username.strip()
    pwd = password.strip()
    role_name = role.strip().lower()
    if not uname:
        raise ValueError("username is required")
    if len(pwd) < 8:
        raise ValueError("password must be at least 8 characters")
    if role_name not in VALID_ROLES:
        raise ValueError("invalid role")
    source_mode_name = normalize_source_mode(source_mode)

    salt = _new_salt()
    now = _utc_now()
    with _connect(settings.auth_db_path) as conn:
        try:
            cur = conn.execute(
                """
                INSERT INTO users (username, password_hash, salt, role, is_active, source_mode, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?)
                """,
                (uname, _hash_password(pwd, salt), salt, role_name, source_mode_name, now, now),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError("username already exists") from exc
        conn.commit()
        last_row_id = cur.lastrowid
        if last_row_id is None:
            raise ValueError("failed to create user")
        user_id = int(last_row_id)

    return get_user(settings, user_id)


def get_user(settings: Settings, user_id: int) -> dict[str, Any]:
    with _connect(settings.auth_db_path) as conn:
        row = conn.execute(
            """
            SELECT
              u.id,
              u.username,
              u.role,
              u.is_active,
              u.source_mode,
              u.created_at,
              u.updated_at,
              CASE WHEN nc.user_id IS NULL THEN 0 ELSE 1 END AS notion_connected
            FROM users u
            LEFT JOIN notion_connections nc ON nc.user_id = u.id
            WHERE u.id=?
            """,
            (int(user_id),),
        ).fetchone()
    if row is None:
        raise ValueError("user not found")
    return {
        "id": int(row["id"]),
        "username": str(row["username"]),
        "role": str(row["role"]),
        "is_active": bool(int(row["is_active"])),
        "source_mode": normalize_source_mode(str(row["source_mode"] or SOURCE_MODE_OUTLINE)),
        "notion_connected": bool(int(row["notion_connected"])),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _count_active_admins(settings: Settings) -> int:
    with _connect(settings.auth_db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM users WHERE role=? AND is_active=1", (ROLE_ADMIN,)).fetchone()
    return int(row["c"]) if row else 0


def update_user(
    settings: Settings,
    user_id: int,
    *,
    username: str | None = None,
    password: str | None = None,
    role: str | None = None,
    is_active: bool | None = None,
    source_mode: str | None = None,
) -> dict[str, Any]:
    user = get_user(settings, user_id)
    updates: dict[str, Any] = {}

    if username is not None:
        cleaned = username.strip()
        if not cleaned:
            raise ValueError("username is required")
        updates["username"] = cleaned

    if role is not None:
        role_name = role.strip().lower()
        if role_name not in VALID_ROLES:
            raise ValueError("invalid role")
        if user["role"] == ROLE_ADMIN and role_name != ROLE_ADMIN and _count_active_admins(settings) <= 1:
            raise ValueError("cannot demote last active admin")
        updates["role"] = role_name

    if is_active is not None:
        active = bool(is_active)
        if user["role"] == ROLE_ADMIN and not active and _count_active_admins(settings) <= 1:
            raise ValueError("cannot deactivate last active admin")
        updates["is_active"] = 1 if active else 0

    if source_mode is not None:
        updates["source_mode"] = normalize_source_mode(source_mode)

    password_hash = None
    salt = None
    if password is not None:
        pwd = password.strip()
        if len(pwd) < 8:
            raise ValueError("password must be at least 8 characters")
        salt = _new_salt()
        password_hash = _hash_password(pwd, salt)

    if not updates and password_hash is None:
        return user

    updates["updated_at"] = _utc_now()
    parts = [f"{key}=?" for key in updates.keys()]
    params = list(updates.values())
    if password_hash is not None and salt is not None:
        parts.extend(["password_hash=?", "salt=?"])
        params.extend([password_hash, salt])

    params.append(int(user_id))

    with _connect(settings.auth_db_path) as conn:
        try:
            conn.execute(f"UPDATE users SET {', '.join(parts)} WHERE id=?", params)
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise ValueError("username already exists") from exc

    return get_user(settings, user_id)


def delete_user(settings: Settings, user_id: int) -> None:
    user = get_user(settings, user_id)
    if user["role"] == ROLE_ADMIN and user["is_active"] and _count_active_admins(settings) <= 1:
        raise ValueError("cannot delete last active admin")

    with _connect(settings.auth_db_path) as conn:
        cur = conn.execute("DELETE FROM users WHERE id=?", (int(user_id),))
        conn.commit()
        if cur.rowcount == 0:
            raise ValueError("user not found")


def _parse_ts(raw: Any) -> datetime | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _state_ts(state: dict[str, Any] | None, fallback: Any = None) -> datetime | None:
    if isinstance(state, dict):
        parsed = _parse_ts(state.get("_state_updated_at"))
        if parsed is not None:
            return parsed
    return _parse_ts(fallback)


def get_chat_state(settings: Settings, user_id: int) -> dict[str, Any] | None:
    with _connect(settings.auth_db_path) as conn:
        row = conn.execute("SELECT state_json FROM chat_states WHERE user_id=?", (int(user_id),)).fetchone()
    if row is None:
        return None
    raw = str(row["state_json"])
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def get_chat_state_meta(settings: Settings, user_id: int) -> dict[str, Any]:
    with _connect(settings.auth_db_path) as conn:
        row = conn.execute(
            "SELECT updated_at FROM chat_states WHERE user_id=?",
            (int(user_id),),
        ).fetchone()
    return {"updated_at": str(row["updated_at"]) if row else None}


def save_chat_state(settings: Settings, user_id: int, state: dict[str, Any]) -> dict[str, Any]:
    incoming = dict(state)
    incoming["_state_updated_at"] = str(incoming.get("_state_updated_at") or _utc_now_precise())

    with _connect(settings.auth_db_path) as conn:
        row = conn.execute(
            "SELECT state_json, updated_at FROM chat_states WHERE user_id=?",
            (int(user_id),),
        ).fetchone()

        if row is not None:
            existing_state: dict[str, Any] | None = None
            try:
                parsed = json.loads(str(row["state_json"]))
                if isinstance(parsed, dict):
                    existing_state = parsed
            except Exception:
                existing_state = None

            incoming_base_ts = _parse_ts(incoming.get("_state_base_updated_at"))
            server_row_ts = _parse_ts(row["updated_at"])
            if incoming_base_ts is not None and server_row_ts is not None and incoming_base_ts != server_row_ts:
                return {"updated_at": str(row["updated_at"]), "applied": False}

            incoming_ts = _state_ts(incoming)
            existing_ts = _state_ts(existing_state, fallback=row["updated_at"])
            if incoming_ts is not None and existing_ts is not None and incoming_ts < existing_ts:
                return {"updated_at": str(row["updated_at"]), "applied": False}

        server_updated_at = _utc_now_precise()
        payload = json.dumps(incoming, ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO chat_states (user_id, state_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              state_json=excluded.state_json,
              updated_at=excluded.updated_at
            """,
            (int(user_id), payload, server_updated_at),
        )
        conn.commit()
    return {"updated_at": server_updated_at, "applied": True}


def get_acl_global_defaults(settings: Settings) -> set[str]:
    with _connect(settings.auth_db_path) as conn:
        rows = conn.execute("SELECT document_id FROM acl_global_defaults").fetchall()
    return {
        normalize_document_id(str(row["document_id"]).strip())
        for row in rows
        if normalize_document_id(str(row["document_id"]).strip())
    }


def set_acl_global_defaults(settings: Settings, document_ids: set[str]) -> None:
    now = _utc_now()
    clean_ids = sorted({normalize_document_id(doc_id) for doc_id in document_ids if normalize_document_id(doc_id)})
    with _connect(settings.auth_db_path) as conn:
        conn.execute("DELETE FROM acl_global_defaults")
        if clean_ids:
            conn.executemany(
                "INSERT INTO acl_global_defaults (document_id, updated_at) VALUES (?, ?)",
                [(doc_id, now) for doc_id in clean_ids],
            )
        conn.commit()


def get_acl_user_overrides(settings: Settings, user_id: int) -> set[str]:
    with _connect(settings.auth_db_path) as conn:
        rows = conn.execute(
            "SELECT document_id FROM acl_user_overrides WHERE user_id=?",
            (int(user_id),),
        ).fetchall()
    return {
        normalize_document_id(str(row["document_id"]).strip())
        for row in rows
        if normalize_document_id(str(row["document_id"]).strip())
    }


def set_acl_user_overrides(settings: Settings, user_id: int, document_ids: set[str]) -> None:
    now = _utc_now()
    clean_ids = sorted({normalize_document_id(doc_id) for doc_id in document_ids if normalize_document_id(doc_id)})
    with _connect(settings.auth_db_path) as conn:
        conn.execute("DELETE FROM acl_user_overrides WHERE user_id=?", (int(user_id),))
        if clean_ids:
            conn.executemany(
                "INSERT INTO acl_user_overrides (user_id, document_id, updated_at) VALUES (?, ?, ?)",
                [(int(user_id), doc_id, now) for doc_id in clean_ids],
            )
        conn.commit()


def get_global_source_settings(settings: Settings) -> dict[str, bool]:
    result = dict(GLOBAL_SOURCE_DEFAULTS)
    with _connect(settings.auth_db_path) as conn:
        rows = conn.execute("SELECT source, is_enabled FROM source_global_settings").fetchall()
    for row in rows:
        source = str(row["source"] or "").strip().lower()
        if source not in result:
            continue
        result[source] = bool(int(row["is_enabled"]))
    return result


def get_auth_global_settings(settings: Settings) -> dict[str, bool]:
    result = dict(AUTH_GLOBAL_DEFAULTS)
    with _connect(settings.auth_db_path) as conn:
        rows = conn.execute("SELECT key, value FROM auth_global_settings").fetchall()
    for row in rows:
        key = str(row["key"] or "").strip().lower()
        if key not in result:
            continue
        raw = str(row["value"] or "").strip().lower()
        result[key] = raw in {"1", "true", "yes", "on"}
    return result


def get_oidc_login_enabled(settings: Settings) -> bool:
    state = get_auth_global_settings(settings)
    set_by_user = bool(state.get(AUTH_GLOBAL_KEY_OIDC_LOGIN_SET_BY_USER, False))
    if set_by_user:
        return bool(state.get(AUTH_GLOBAL_KEY_OIDC_LOGIN_ENABLED, False))
    return bool(getattr(settings, "auth_oidc_enabled", False))


def set_oidc_login_enabled(settings: Settings, enabled: bool) -> bool:
    now = _utc_now()
    value = "1" if bool(enabled) else "0"
    with _connect(settings.auth_db_path) as conn:
        conn.executemany(
            """
            INSERT INTO auth_global_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=excluded.updated_at
            """,
            [
                (AUTH_GLOBAL_KEY_OIDC_LOGIN_ENABLED, value, now),
                (AUTH_GLOBAL_KEY_OIDC_LOGIN_SET_BY_USER, "1", now),
            ],
        )
        conn.commit()
    return get_oidc_login_enabled(settings)


def set_global_source_settings(
    settings: Settings,
    *,
    outline_enabled: bool,
    notion_enabled: bool,
    files_enabled: bool,
) -> dict[str, bool]:
    now = _utc_now()
    values = {
        SOURCE_OUTLINE: bool(outline_enabled),
        SOURCE_NOTION: bool(notion_enabled),
        SOURCE_FILES: bool(files_enabled),
    }
    with _connect(settings.auth_db_path) as conn:
        conn.executemany(
            """
            INSERT INTO source_global_settings (source, is_enabled, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(source) DO UPDATE SET
              is_enabled=excluded.is_enabled,
              updated_at=excluded.updated_at
            """,
            [(source, 1 if enabled else 0, now) for source, enabled in values.items()],
        )
        conn.commit()
    return get_global_source_settings(settings)


def get_notion_connection(settings: Settings, user_id: int) -> dict[str, Any] | None:
    with _connect(settings.auth_db_path) as conn:
        row = conn.execute(
            """
            SELECT user_id, mode, access_token, workspace_id, workspace_name, bot_id, created_at, updated_at
            FROM notion_connections
            WHERE user_id=?
            """,
            (int(user_id),),
        ).fetchone()
    if row is None:
        return None
    mode = str(row["mode"] or "").strip().lower()
    if mode not in VALID_NOTION_CONNECTION_MODES:
        mode = NOTION_CONNECTION_MODE_TOKEN
    return {
        "user_id": int(row["user_id"]),
        "mode": mode,
        "access_token": str(row["access_token"] or ""),
        "workspace_id": str(row["workspace_id"] or ""),
        "workspace_name": str(row["workspace_name"] or ""),
        "bot_id": str(row["bot_id"] or ""),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def upsert_notion_connection(
    settings: Settings,
    user_id: int,
    *,
    mode: str,
    access_token: str,
    workspace_id: str | None = None,
    workspace_name: str | None = None,
    bot_id: str | None = None,
) -> dict[str, Any]:
    clean_mode = str(mode or "").strip().lower()
    if clean_mode not in VALID_NOTION_CONNECTION_MODES:
        raise ValueError("invalid notion connection mode")
    token = str(access_token or "").strip()
    if not token:
        raise ValueError("notion token is required")
    now = _utc_now()

    with _connect(settings.auth_db_path) as conn:
        conn.execute(
            """
            INSERT INTO notion_connections (user_id, mode, access_token, workspace_id, workspace_name, bot_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              mode=excluded.mode,
              access_token=excluded.access_token,
              workspace_id=excluded.workspace_id,
              workspace_name=excluded.workspace_name,
              bot_id=excluded.bot_id,
              updated_at=excluded.updated_at
            """,
            (
                int(user_id),
                clean_mode,
                token,
                str(workspace_id or "").strip() or None,
                str(workspace_name or "").strip() or None,
                str(bot_id or "").strip() or None,
                now,
                now,
            ),
        )
        conn.commit()
    connection = get_notion_connection(settings, user_id)
    if connection is None:
        raise ValueError("failed to persist notion connection")
    return connection


def delete_notion_connection(settings: Settings, user_id: int) -> None:
    with _connect(settings.auth_db_path) as conn:
        conn.execute("DELETE FROM notion_connections WHERE user_id=?", (int(user_id),))
        conn.execute("DELETE FROM notion_oauth_states WHERE user_id=?", (int(user_id),))
        conn.commit()


def create_notion_oauth_state(settings: Settings, user_id: int, ttl_minutes: int = 15) -> str:
    now_dt = datetime.now(UTC)
    expires_dt = now_dt + timedelta(minutes=max(1, ttl_minutes))
    state = secrets.token_urlsafe(32)

    with _connect(settings.auth_db_path) as conn:
        conn.execute("DELETE FROM notion_oauth_states WHERE expires_at <= ?", (_utc_now(),))
        conn.execute(
            "INSERT INTO notion_oauth_states (state, user_id, expires_at, created_at) VALUES (?, ?, ?, ?)",
            (
                state,
                int(user_id),
                expires_dt.isoformat(timespec="seconds"),
                now_dt.isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
    return state


def consume_notion_oauth_state(settings: Settings, user_id: int, state: str) -> bool:
    clean_state = str(state or "").strip()
    if not clean_state:
        return False

    with _connect(settings.auth_db_path) as conn:
        row = conn.execute(
            "SELECT state FROM notion_oauth_states WHERE state=? AND user_id=? AND expires_at > ?",
            (clean_state, int(user_id), _utc_now()),
        ).fetchone()
        if row is None:
            return False
        conn.execute("DELETE FROM notion_oauth_states WHERE state=?", (clean_state,))
        conn.commit()
    return True


def create_auth_oidc_state(settings: Settings, return_to: str | None = None, ttl_minutes: int = 15) -> str:
    now_dt = datetime.now(UTC)
    expires_dt = now_dt + timedelta(minutes=max(1, ttl_minutes))
    state = secrets.token_urlsafe(32)
    target = str(return_to or "").strip()
    if not target.startswith("/"):
        target = "/ui"

    with _connect(settings.auth_db_path) as conn:
        conn.execute("DELETE FROM auth_oidc_states WHERE expires_at <= ?", (_utc_now(),))
        conn.execute(
            "INSERT INTO auth_oidc_states (state, return_to, expires_at, created_at) VALUES (?, ?, ?, ?)",
            (
                state,
                target,
                expires_dt.isoformat(timespec="seconds"),
                now_dt.isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
    return state


def consume_auth_oidc_state(settings: Settings, state: str) -> str | None:
    clean_state = str(state or "").strip()
    if not clean_state:
        return None

    with _connect(settings.auth_db_path) as conn:
        row = conn.execute(
            "SELECT return_to FROM auth_oidc_states WHERE state=? AND expires_at > ?",
            (clean_state, _utc_now()),
        ).fetchone()
        if row is None:
            return None
        conn.execute("DELETE FROM auth_oidc_states WHERE state=?", (clean_state,))
        conn.commit()
    target = str(row["return_to"] or "").strip()
    return target if target.startswith("/") else "/ui"


def create_auth_oidc_login_ticket(settings: Settings, user_id: int, ttl_seconds: int = 180) -> str:
    now_dt = datetime.now(UTC)
    expires_dt = now_dt + timedelta(seconds=max(30, ttl_seconds))
    ticket = secrets.token_urlsafe(32)
    ticket_hash = _token_hash(ticket)

    with _connect(settings.auth_db_path) as conn:
        conn.execute("DELETE FROM auth_oidc_login_tickets WHERE expires_at <= ?", (_utc_now(),))
        conn.execute(
            "INSERT INTO auth_oidc_login_tickets (ticket_hash, user_id, expires_at, created_at) VALUES (?, ?, ?, ?)",
            (
                ticket_hash,
                int(user_id),
                expires_dt.isoformat(timespec="seconds"),
                now_dt.isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
    return ticket


def consume_auth_oidc_login_ticket(settings: Settings, ticket: str) -> AuthUser | None:
    clean_ticket = str(ticket or "").strip()
    if not clean_ticket:
        return None
    ticket_hash = _token_hash(clean_ticket)

    with _connect(settings.auth_db_path) as conn:
        row = conn.execute(
            """
            SELECT u.id, u.username, u.role, u.is_active, u.source_mode
            FROM auth_oidc_login_tickets t
            JOIN users u ON u.id = t.user_id
            WHERE t.ticket_hash=? AND t.expires_at > ?
            """,
            (ticket_hash, _utc_now()),
        ).fetchone()
        conn.execute("DELETE FROM auth_oidc_login_tickets WHERE ticket_hash=?", (ticket_hash,))
        conn.commit()

    user = _row_to_user(row)
    if user is None or not user.is_active:
        return None
    user.auth_method = AUTH_METHOD_OIDC
    return user


def upsert_oidc_user(settings: Settings, *, username: str, role: str) -> AuthUser:
    uname = str(username or "").strip()
    if not uname:
        raise ValueError("oidc username is required")

    role_name = str(role or ROLE_USER).strip().lower()
    if role_name not in VALID_ROLES:
        role_name = ROLE_USER

    now = _utc_now()
    with _connect(settings.auth_db_path) as conn:
        if uname.lower() == SUPER_ADMIN_USERNAME:
            admin_row = conn.execute(
                "SELECT id FROM users WHERE lower(username)=lower(?)",
                (SUPER_ADMIN_USERNAME,),
            ).fetchone()
            if admin_row is None:
                raise ValueError("reserved username")
            if role_name != ROLE_ADMIN:
                raise ValueError("reserved username")
            conn.execute(
                "UPDATE users SET role=?, is_active=1, updated_at=? WHERE id=?",
                (ROLE_ADMIN, now, int(admin_row["id"])),
            )
            conn.commit()
            user_data = get_user(settings, int(admin_row["id"]))
            return AuthUser(
                id=int(user_data["id"]),
                username=str(user_data["username"]),
                role=str(user_data["role"]),
                is_active=bool(user_data["is_active"]),
                source_mode=normalize_source_mode(str(user_data.get("source_mode") or SOURCE_MODE_OUTLINE)),
                auth_method=AUTH_METHOD_OIDC,
            )

        row = conn.execute(
            "SELECT id, username FROM users WHERE lower(username)=lower(?)",
            (uname,),
        ).fetchone()

        if row is None:
            salt = _new_salt()
            password_hash = _hash_password(secrets.token_urlsafe(32), salt)
            cur = conn.execute(
                """
                INSERT INTO users (username, password_hash, salt, role, is_active, source_mode, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?)
                """,
                (uname, password_hash, salt, role_name, SOURCE_MODE_OUTLINE, now, now),
            )
            last_row_id = cur.lastrowid
            if last_row_id is None:
                raise ValueError("failed to upsert oidc user")
            user_id = int(last_row_id)
        else:
            existing_username = str(row["username"] or "").strip()
            if existing_username.lower() == SUPER_ADMIN_USERNAME:
                raise ValueError("reserved username")
            user_id = int(row["id"])
            conn.execute(
                "UPDATE users SET role=?, is_active=1, updated_at=? WHERE id=?",
                (role_name, now, user_id),
            )
        conn.commit()

        out = conn.execute(
            "SELECT id, username, role, is_active, source_mode FROM users WHERE id=?",
            (user_id,),
        ).fetchone()

    user = _row_to_user(out)
    if user is None:
        raise ValueError("failed to upsert oidc user")
    return user
