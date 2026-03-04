import json
import re
from base64 import b64encode, urlsafe_b64decode
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse

from app.acl import (
    effective_source_mode_for_user,
    expand_document_ids_with_descendants,
    globally_enabled_sources,
    get_effective_allowed_document_ids,
    get_notion_tree,
    get_outline_tree,
    is_super_admin,
    merge_children_maps,
    user_enabled_sources,
)
from app.auth import (
    NOTION_CONNECTION_MODE_OAUTH,
    NOTION_CONNECTION_MODE_TOKEN,
    AUTH_METHOD_LOCAL,
    AUTH_METHOD_OIDC,
    ROLE_ADMIN,
    ROLE_USER,
    AuthUser,
    authenticate_user,
    consume_auth_oidc_login_ticket,
    consume_auth_oidc_state,
    consume_notion_oauth_state,
    create_auth_oidc_state,
    create_auth_oidc_login_ticket,
    create_notion_oauth_state,
    create_session,
    create_user,
    delete_user,
    delete_notion_connection,
    get_acl_global_defaults,
    get_acl_user_overrides,
    get_global_source_settings,
    get_oidc_login_enabled,
    get_notion_connection,
    get_user,
    get_user_by_session,
    get_chat_state_meta,
    init_auth_db,
    list_users,
    get_chat_state,
    save_chat_state,
    revoke_session,
    set_acl_global_defaults,
    set_global_source_settings,
    set_oidc_login_enabled,
    set_acl_user_overrides,
    upsert_oidc_user,
    upsert_notion_connection,
    update_user,
)
from app.config import Settings, get_settings
from app.document_ids import SOURCE_NOTION, SOURCE_OUTLINE, enabled_sources_from_mode, make_document_id, split_document_id
from app.document_ids import SOURCE_FILES
from app.files_source import (
    apply_write_operation,
    cleanup_expired_assets,
    create_artifact,
    create_chat_upload,
    create_files_root,
    delete_artifact,
    delete_chat_upload,
    delete_files_root,
    ensure_files_feature_schema,
    files_document_id,
    files_folder_document_id,
    get_artifact,
    get_chat_upload,
    get_files_document,
    get_files_feature_settings,
    get_files_root,
    get_files_tree,
    issue_artifact_download_token,
    list_chat_uploads_by_ids,
    list_files_audit,
    list_files_roots,
    parse_files_document_id,
    preview_write_operation,
    set_files_feature_settings,
    update_files_root,
    verify_artifact_token,
)
from app.llm import LLMGateway
from app.rag import RAGService
from app.schemas import (
    AuthOidcConfigResponse,
    AuthOidcGlobalSettingsRequest,
    AuthOidcGlobalSettingsResponse,
    AuthUserResponse,
    AssistantChatRequest,
    AssistantChatResponse,
    ChatRequest,
    ChatResponse,
    ChangePasswordRequest,
    CreateUserRequest,
    LoginRequest,
    LoginResponse,
    NotionConnectionResponse,
    NotionTokenConnectRequest,
    OllamaModelsResponse,
    ProvidersHealthResponse,
    RewriteRequest,
    SourceGlobalSettingsRequest,
    SourceGlobalSettingsResponse,
    SyncResponse,
    SyncStartResponse,
    SyncStatusResponse,
    TextTaskRequest,
    TextTaskResponse,
    TranslateRequest,
    UpdateUserRequest,
)
from app.sync import run_full_sync
from app.sync_jobs import sync_jobs

app = FastAPI(title="OAssist", version="1.1.0")
CHAT_UI_PATH = Path(__file__).resolve().parent / "static" / "chat.html"
FAVICON_PATH = Path(__file__).resolve().parent / "static" / "favicon.svg"
CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
LATIN_RE = re.compile(r"[A-Za-z]")


@app.on_event("startup")
def startup() -> None:
    settings = get_settings()
    init_auth_db(settings)
    ensure_files_feature_schema(settings)
    cleanup_expired_assets(settings)


def _extract_session_token(request: Request) -> str:
    token = request.cookies.get("oassist_session") or ""
    if token:
        return token
    auth_header = request.headers.get("Authorization", "")
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return ""


def _public_user(user: AuthUser | dict[str, Any]) -> dict[str, Any]:
    if isinstance(user, dict):
        auth_method = str(user.get("auth_method") or AUTH_METHOD_LOCAL).strip().lower()
        if auth_method not in {AUTH_METHOD_LOCAL, AUTH_METHOD_OIDC}:
            auth_method = AUTH_METHOD_LOCAL
        return {
            "id": int(user["id"]),
            "username": str(user["username"]),
            "role": str(user["role"]),
            "is_active": bool(user["is_active"]),
            "source_mode": str(user.get("source_mode") or "outline"),
            "notion_connected": bool(user.get("notion_connected")),
            "auth_method": auth_method,
        }
    auth_method = str(getattr(user, "auth_method", AUTH_METHOD_LOCAL) or AUTH_METHOD_LOCAL).strip().lower()
    if auth_method not in {AUTH_METHOD_LOCAL, AUTH_METHOD_OIDC}:
        auth_method = AUTH_METHOD_LOCAL
    return {
        "id": int(user.id),
        "username": str(user.username),
        "role": str(user.role),
        "is_active": bool(user.is_active),
        "source_mode": str(getattr(user, "source_mode", "outline") or "outline"),
        "notion_connected": False,
        "auth_method": auth_method,
    }


def get_current_user(request: Request, settings: Settings = Depends(get_settings)) -> AuthUser:
    token = _extract_session_token(request)
    user = get_user_by_session(settings, token)
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def require_admin_role(current: AuthUser = Depends(get_current_user)) -> AuthUser:
    if current.role != ROLE_ADMIN:
        raise HTTPException(status_code=403, detail="Admin role required")
    return current


def require_super_admin_role(current: AuthUser = Depends(require_admin_role)) -> AuthUser:
    if not is_super_admin(current):
        raise HTTPException(status_code=403, detail="Super admin required")
    return current


def _is_auth_oidc_configured(settings: Settings) -> bool:
    if not bool(settings.auth_oidc_enabled):
        return False
    required = [
        settings.auth_oidc_client_id,
        settings.auth_oidc_client_secret,
        settings.auth_oidc_auth_uri,
        settings.auth_oidc_token_uri,
        settings.auth_oidc_userinfo_uri,
        settings.auth_oidc_redirect_uri,
    ]
    return all(bool(str(item or "").strip()) for item in required)


def _is_auth_oidc_enabled(settings: Settings) -> bool:
    return _is_auth_oidc_configured(settings) and bool(get_oidc_login_enabled(settings))


def _auth_oidc_unavailable_detail(settings: Settings) -> str:
    if not _is_auth_oidc_configured(settings):
        return "OIDC authentication is not configured"
    return "OIDC login is disabled by administrator"


def _claim_value(data: dict[str, Any], claim_path: str) -> Any:
    if not isinstance(data, dict):
        return None
    path = [segment.strip() for segment in str(claim_path or "").split(".") if segment.strip()]
    if not path:
        return None
    cur: Any = data
    for segment in path:
        if not isinstance(cur, dict) or segment not in cur:
            return None
        cur = cur.get(segment)
    return cur


def _groups_from_claim(value: Any) -> set[str]:
    def _normalize_group(raw: str) -> set[str]:
        text = str(raw or "").strip().lower()
        if not text:
            return set()
        normalized = {text}
        trimmed = text.lstrip("/")
        if trimmed:
            normalized.add(trimmed)
        if "/" in trimmed:
            normalized.add(trimmed.rsplit("/", 1)[-1])
        return {item for item in normalized if item}

    if isinstance(value, list):
        result: set[str] = set()
        for item in value:
            result.update(_normalize_group(str(item)))
        return result
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return set()
        if "," in raw:
            result: set[str] = set()
            for part in raw.split(","):
                result.update(_normalize_group(part))
            return result
        return _normalize_group(raw)
    return set()


def _oidc_role_from_groups(settings: Settings, groups: set[str]) -> str:
    admin_group = str(settings.auth_oidc_admin_group or "outline-admins").strip().lower()
    user_group = str(settings.auth_oidc_user_group or "outline-users").strip().lower()
    if admin_group and admin_group in groups:
        return ROLE_ADMIN
    if user_group and user_group in groups:
        return ROLE_USER
    if not groups:
        return ROLE_USER
    if user_group == "outline-users":
        return ROLE_USER
    if user_group:
        raise HTTPException(status_code=403, detail="OIDC user is not in allowed groups")
    return ROLE_USER


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    raw = str(token or "").strip()
    if not raw:
        return {}
    parts = raw.split(".")
    if len(parts) < 2:
        return {}
    payload_part = parts[1]
    if not payload_part:
        return {}
    padding = "=" * (-len(payload_part) % 4)
    try:
        decoded = urlsafe_b64decode((payload_part + padding).encode("ascii")).decode("utf-8")
        data = json.loads(decoded)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _safe_return_to(value: str | None) -> str:
    target = str(value or "").strip()
    if not target.startswith("/") or target.startswith("//"):
        return "/ui"
    return target


def _auth_error_redirect(detail: str, return_to: str = "/ui") -> RedirectResponse:
    target = _safe_return_to(return_to)
    message = str(detail or "Authentication failed").strip() or "Authentication failed"
    if len(message) > 240:
        message = message[:240].rstrip()
    separator = "&" if "?" in target else "?"
    return RedirectResponse(url=f"{target}{separator}{urlencode({'auth_error': message})}", status_code=302)


def _detect_language(text: str) -> str:
    return "ru" if CYRILLIC_RE.search(text) else "en"


def _source_download_url(document_id: str | None, raw_url: str | None) -> str:
    doc_id = str(document_id or "").strip()
    url = str(raw_url or "").strip()

    if doc_id.startswith("upload:"):
        upload_id = doc_id.split(":", 1)[1].strip()
        if upload_id:
            return f"/chat/uploads/{upload_id}/download"

    parsed = split_document_id(doc_id)
    if parsed is not None:
        source, _native = parsed
        if source == SOURCE_FILES:
            parsed_files = parse_files_document_id(doc_id)
            if parsed_files is not None:
                _root_id, _rel_path, is_folder = parsed_files
                if not is_folder:
                    return f"/files/download?{urlencode({'document_id': doc_id})}"

    return url


def _append_query_param(url: str, key: str, value: str) -> str:
    raw = str(url or "").strip() or "/ui"
    parts = urlsplit(raw)
    query_items = parse_qsl(parts.query, keep_blank_values=True)
    query_items.append((str(key), str(value)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query_items), parts.fragment))


def _filter_tree_nodes_by_allowed(nodes: list[dict[str, Any]], allowed_ids: set[str] | None) -> list[dict[str, Any]]:
    if allowed_ids is None:
        return nodes
    if not allowed_ids:
        return []

    filtered: list[dict[str, Any]] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        doc_id = str(node.get("id") or "").strip()
        children = node.get("children")
        child_nodes = children if isinstance(children, list) else []
        kept_children = _filter_tree_nodes_by_allowed(child_nodes, allowed_ids)
        if doc_id in allowed_ids or kept_children:
            row = dict(node)
            row["children"] = kept_children
            filtered.append(row)
    return filtered


def _count_tree_nodes(nodes: list[dict[str, Any]]) -> int:
    total = 0
    for node in nodes:
        if not isinstance(node, dict):
            continue
        total += 1
        children = node.get("children")
        if isinstance(children, list) and children:
            total += _count_tree_nodes(children)
    return total


def _resolve_notion_access_token(settings: Settings, user: AuthUser | None = None) -> str:
    if user is not None:
        connection = get_notion_connection(settings, user.id)
        if connection and str(connection.get("access_token") or "").strip():
            return str(connection.get("access_token") or "").strip()
    return str(settings.notion_api_token or "").strip()


def _effective_enabled_sources(settings: Settings, user: AuthUser) -> set[str]:
    return user_enabled_sources(user, globally_enabled_sources(settings))


def _is_source_globally_enabled(settings: Settings, source: str) -> bool:
    return str(source or "").strip().lower() in globally_enabled_sources(settings)


def _resolve_allowed_document_ids(settings: Settings, user: AuthUser) -> set[str] | None:
    enabled_sources = _effective_enabled_sources(settings, user)
    if not enabled_sources:
        return set()

    allowed = get_effective_allowed_document_ids(settings, user)
    if allowed is None:
        # Super admin: unrestricted ACL, but still limited by globally enabled sources.
        if enabled_sources == {SOURCE_OUTLINE, SOURCE_NOTION, SOURCE_FILES}:
            return None

        include_outline = SOURCE_OUTLINE in enabled_sources
        include_notion = SOURCE_NOTION in enabled_sources and bool(_resolve_notion_access_token(settings, user))
        include_files = SOURCE_FILES in enabled_sources
        if not include_outline and not include_notion and not include_files:
            return set()
        try:
            known_ids, _children_map = _known_document_ids(
                settings,
                include_outline=include_outline,
                include_notion=include_notion,
                include_files=include_files,
                notion_access_token=_resolve_notion_access_token(settings, user),
            )
            return known_ids
        except Exception:
            return set()

    result = set(allowed)

    result = {
        doc_id
        for doc_id in result
        if (split_document_id(doc_id) or (None, None))[0] in enabled_sources
    }

    if not result:
        return set()

    include_outline = any((split_document_id(doc_id) or (None, None))[0] == SOURCE_OUTLINE for doc_id in result)
    include_files = any((split_document_id(doc_id) or (None, None))[0] == SOURCE_FILES for doc_id in result)
    notion_token = _resolve_notion_access_token(settings, user)
    include_notion = any((split_document_id(doc_id) or (None, None))[0] == SOURCE_NOTION for doc_id in result) and bool(
        notion_token
    )

    try:
        known_ids, children_map = _known_document_ids(
            settings,
            include_outline=include_outline,
            include_notion=include_notion,
            include_files=include_files,
            notion_access_token=notion_token,
        )
        expanded = expand_document_ids_with_descendants(result, children_map)
        result = {doc_id for doc_id in expanded if doc_id in known_ids}
    except Exception:
        pass

    return result


def _known_document_ids(
    settings: Settings,
    *,
    include_outline: bool,
    include_notion: bool,
    include_files: bool,
    notion_access_token: str | None = None,
) -> tuple[set[str], dict[str, list[str]]]:
    known_ids: set[str] = set()
    children_maps: list[dict[str, list[str]]] = []

    if include_outline:
        outline_tree = get_outline_tree(settings)
        known_ids.update({str(item).strip() for item in (outline_tree.get("all_ids") or set()) if str(item).strip()})
        children_maps.append(dict(outline_tree.get("children_map") or {}))

    if include_notion:
        token = str(notion_access_token or "").strip()
        if not token:
            raise HTTPException(status_code=400, detail="Notion is not connected")
        notion_tree = get_notion_tree(settings, token)
        known_ids.update({str(item).strip() for item in (notion_tree.get("all_ids") or set()) if str(item).strip()})
        children_maps.append(dict(notion_tree.get("children_map") or {}))

    if include_files:
        files_tree = get_files_tree(settings)
        known_ids.update({str(item).strip() for item in (files_tree.get("all_ids") or set()) if str(item).strip()})
        children_maps.append(dict(files_tree.get("children_map") or {}))

    return known_ids, merge_children_maps(*children_maps)


def _validate_acl_document_ids(
    settings: Settings,
    document_ids: set[str],
    *,
    notion_access_token: str | None = None,
) -> set[str]:
    if not document_ids:
        return set()

    normalized_ids: set[str] = set()
    notion_requested = False
    outline_requested = False
    files_requested = False
    for raw in document_ids:
        parsed = split_document_id(raw)
        if parsed is None:
            continue
        source, native_id = parsed
        normalized = make_document_id(source, native_id)
        normalized_ids.add(normalized)
        if source == SOURCE_NOTION:
            notion_requested = True
        if source == SOURCE_OUTLINE:
            outline_requested = True
        if source == SOURCE_FILES:
            files_requested = True

    try:
        known_ids, _ = _known_document_ids(
            settings,
            include_outline=outline_requested,
            include_notion=notion_requested,
            include_files=files_requested,
            notion_access_token=notion_access_token,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Failed to validate ACL documents: {exc}") from exc

    unknown = sorted([doc_id for doc_id in normalized_ids if doc_id not in known_ids])
    if unknown:
        preview = ", ".join(unknown[:12])
        suffix = "" if len(unknown) <= 12 else f" (+{len(unknown) - 12} more)"
        raise HTTPException(status_code=400, detail=f"Unknown document_ids: {preview}{suffix}")
    return normalized_ids


def _expand_acl_document_ids(
    settings: Settings,
    document_ids: set[str],
    *,
    notion_access_token: str | None = None,
) -> set[str]:
    if not document_ids:
        return set()

    notion_requested = any((split_document_id(doc_id) or (None, None))[0] == SOURCE_NOTION for doc_id in document_ids)
    outline_requested = any((split_document_id(doc_id) or (None, None))[0] == SOURCE_OUTLINE for doc_id in document_ids)
    files_requested = any((split_document_id(doc_id) or (None, None))[0] == SOURCE_FILES for doc_id in document_ids)
    try:
        _known_ids, children_map = _known_document_ids(
            settings,
            include_outline=outline_requested,
            include_notion=notion_requested,
            include_files=files_requested,
            notion_access_token=notion_access_token,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Failed to expand ACL documents: {exc}") from exc
    return expand_document_ids_with_descendants(document_ids, children_map)


def _expanded_global_acl_scope(
    settings: Settings,
    *,
    notion_access_token: str | None = None,
) -> set[str]:
    defaults = get_acl_global_defaults(settings)
    if not defaults:
        return set()

    enabled_sources = globally_enabled_sources(settings)
    token = str(notion_access_token or "").strip()

    normalized_defaults: set[str] = set()
    include_outline = False
    include_notion = False
    include_files = False

    for raw in defaults:
        parsed = split_document_id(raw)
        if parsed is None:
            continue
        source, native_id = parsed
        if source not in enabled_sources:
            continue
        if source == SOURCE_NOTION and not token:
            continue
        normalized_defaults.add(make_document_id(source, native_id))
        if source == SOURCE_OUTLINE:
            include_outline = True
        if source == SOURCE_NOTION:
            include_notion = True
        if source == SOURCE_FILES:
            include_files = True

    if not normalized_defaults:
        return set()

    try:
        known_ids, children_map = _known_document_ids(
            settings,
            include_outline=include_outline,
            include_notion=include_notion,
            include_files=include_files,
            notion_access_token=token,
        )
    except Exception:
        return set()

    valid_defaults = {doc_id for doc_id in normalized_defaults if doc_id in known_ids}
    if not valid_defaults:
        return set()
    return expand_document_ids_with_descendants(valid_defaults, children_map)


def _acl_scope_for_target_user(settings: Settings, actor: AuthUser, target_user: dict[str, Any]) -> set[str]:
    global_enabled = globally_enabled_sources(settings)
    target_enabled_sources = enabled_sources_from_mode(str(target_user.get("source_mode") or "outline"))
    target_enabled_sources.add(SOURCE_FILES)
    target_enabled_sources = {source for source in target_enabled_sources if source in global_enabled}
    if str(target_user.get("username") or "").strip().lower() == "admin":
        target_enabled_sources = set(global_enabled)

    if not target_enabled_sources:
        return set()

    notion_access_token = _resolve_notion_access_token(settings, actor)

    if not is_super_admin(actor):
        scope_ids = _expanded_global_acl_scope(settings, notion_access_token=notion_access_token)
        return {
            doc_id
            for doc_id in scope_ids
            if (split_document_id(doc_id) or (None, None))[0] in target_enabled_sources
        }

    include_outline = SOURCE_OUTLINE in target_enabled_sources
    include_notion = SOURCE_NOTION in target_enabled_sources and bool(notion_access_token)
    include_files = SOURCE_FILES in target_enabled_sources
    if not include_outline and not include_notion and not include_files:
        return set()
    try:
        known_ids, _children_map = _known_document_ids(
            settings,
            include_outline=include_outline,
            include_notion=include_notion,
            include_files=include_files,
            notion_access_token=notion_access_token,
        )
    except Exception:
        return set()
    return set(known_ids)


def _default_acl_selection_for_target_user(settings: Settings, actor: AuthUser, target_user: dict[str, Any]) -> set[str]:
    global_enabled = globally_enabled_sources(settings)
    target_enabled_sources = enabled_sources_from_mode(str(target_user.get("source_mode") or "outline"))
    target_enabled_sources.add(SOURCE_FILES)
    target_enabled_sources = {source for source in target_enabled_sources if source in global_enabled}
    if str(target_user.get("username") or "").strip().lower() == "admin":
        target_enabled_sources = set(global_enabled)

    if not target_enabled_sources:
        return set()

    notion_access_token = _resolve_notion_access_token(settings, actor)
    defaults = _expanded_global_acl_scope(settings, notion_access_token=notion_access_token)
    return {
        doc_id
        for doc_id in defaults
        if (split_document_id(doc_id) or (None, None))[0] in target_enabled_sources
    }


def _user_can_manage_foreign_assets(user: AuthUser) -> bool:
    return str(getattr(user, "role", "") or "").strip().lower() == ROLE_ADMIN


def _require_files_source_enabled(settings: Settings, user: AuthUser) -> None:
    enabled = _effective_enabled_sources(settings, user)
    if SOURCE_FILES not in enabled:
        raise HTTPException(status_code=400, detail="Files source is disabled")


def _require_document_access(settings: Settings, user: AuthUser, document_id: str) -> None:
    allowed = _resolve_allowed_document_ids(settings, user)
    if allowed is None:
        return
    if document_id not in allowed:
        raise HTTPException(status_code=403, detail="Document is not allowed by ACL")


def _artifact_formats_from_message(text: str) -> list[str]:
    lower = str(text or "").lower()
    ordered = [
        ("docx", ["docx", "word"]),
        ("xlsx", ["xlsx", "excel", "table"]),
        ("pdf", ["pdf"]),
        ("md", ["markdown", "md"]),
        ("csv", ["csv"]),
        ("json", ["json"]),
        ("txt", ["txt", "text file", "текстовый файл"]),
    ]
    result: list[str] = []
    for fmt, keywords in ordered:
        if any(keyword in lower for keyword in keywords):
            result.append(fmt)
    return result


def _language_from_custom_prompt(text: str) -> str | None:
    value = (text or "").strip().lower()
    if not value:
        return None

    ru_patterns = (
        "на русском",
        "русском языке",
        "только русский",
        "отвечай по-русски",
        "отвечай на русском",
        "пиши на русском",
        "reply in russian",
        "answer in russian",
    )
    en_patterns = (
        "in english",
        "answer in english",
        "reply in english",
        "на английском",
        "английском языке",
        "только английский",
    )

    if any(pattern in value for pattern in ru_patterns):
        return "ru"
    if any(pattern in value for pattern in en_patterns):
        return "en"
    return None


def _letter_counts(text: str) -> tuple[int, int]:
    cyr = len(CYRILLIC_RE.findall(text or ""))
    lat = len(LATIN_RE.findall(text or ""))
    return cyr, lat


def _needs_language_fix(answer: str, expected_lang: str) -> bool:
    cyr, lat = _letter_counts(answer)
    if expected_lang == "ru":
        return lat >= 24 and cyr < max(8, int(lat * 0.2))
    if expected_lang == "en":
        return cyr >= 24 and lat < max(8, int(cyr * 0.2))
    return False


def _enforce_answer_language(
    *,
    gateway: LLMGateway,
    answer: str,
    expected_lang: str,
    requested_provider: str,
) -> str:
    text = (answer or "").strip()
    if not text or not _needs_language_fix(text, expected_lang):
        return text

    if expected_lang == "ru":
        instruction = (
            "Перепиши ответ строго на русском языке. "
            "Сохрани структуру markdown, ссылки, кодовые блоки и технические термины без искажений."
        )
    else:
        instruction = (
            "Rewrite the answer strictly in English. "
            "Keep markdown structure, links, code blocks, and technical terms unchanged."
        )

    messages = [
        {"role": "system", "content": "You are a precise technical translator."},
        {"role": "user", "content": f"{instruction}\n\nANSWER:\n{text}"},
    ]

    try:
        fixed, _ = gateway.generate(messages=messages, requested_provider=requested_provider)
        fixed = (fixed or "").strip()
        if fixed:
            return fixed
    except Exception:
        pass
    return text


def _build_assistant_messages(
    payload: AssistantChatRequest,
    settings: Settings,
    user: AuthUser,
) -> tuple[list[dict[str, str]], list[dict[str, Any]], str]:
    custom_prompt = (payload.custom_prompt or "").strip()
    custom_prompt_lang = _language_from_custom_prompt(custom_prompt)

    forced_lang = (payload.chat_language or "").strip().lower()
    if forced_lang in {"ru", "en"}:
        lang = forced_lang
    else:
        lang = _detect_language(payload.message)

    if custom_prompt_lang in {"ru", "en"}:
        lang = custom_prompt_lang

    rag = RAGService(settings)
    allowed_document_ids = _resolve_allowed_document_ids(settings, user)

    context = ""
    sources: list[dict[str, Any]] = []
    full_docs: list[dict[str, Any]] = []
    if payload.use_knowledge:
        context, sources = rag.retrieve(
            question=payload.message,
            top_k=payload.top_k or settings.search_top_k,
            allowed_document_ids=allowed_document_ids,
        )
        for item in sources:
            item_doc_id = str(item.get("document_id") or "").strip()
            item["url"] = _source_download_url(item_doc_id, item.get("url"))
        full_docs = rag.retrieve_full_documents(
            question=payload.message,
            allowed_document_ids=allowed_document_ids,
        )

        known_ids = {str(item.get("document_id") or "") for item in sources}
        for item in full_docs:
            doc_id = str(item.get("document_id") or "")
            if not doc_id or doc_id in known_ids:
                continue
            known_ids.add(doc_id)
            sources.append(
                {
                    "document_id": doc_id,
                    "title": str(item.get("title") or "Untitled"),
                    "url": _source_download_url(doc_id, item.get("url")),
                    "score": float(item.get("score") or 0.0),
                    "excerpt": str(item.get("text") or "")[:280],
                }
            )

    upload_blocks: list[str] = []
    upload_ids = [str(item or "").strip() for item in payload.upload_ids if str(item or "").strip()]
    if upload_ids:
        uploads = list_chat_uploads_by_ids(settings, upload_ids, owner_user_id=user.id)
        for upload in uploads:
            text = str(upload.get("extracted_text") or "").strip()
            excerpt = text[:20000] if text else ""
            content_block = excerpt or "[NO_EXTRACTED_TEXT] Text extraction is unavailable for this file."
            indexed_doc_id = str(upload.get("indexed_document_id") or "").strip()
            source_doc_id = indexed_doc_id if indexed_doc_id else f"upload:{upload['id']}"
            source_url = _source_download_url(source_doc_id, f"/chat/uploads/{upload['id']}/download")
            upload_blocks.append(
                f"[UPLOAD {upload['id']}] {upload.get('filename') or 'upload'}\n"
                f"MIME: {upload.get('mime') or 'application/octet-stream'}\n"
                f"CONTENT:\n{content_block}"
            )
            sources.append(
                {
                    "document_id": source_doc_id,
                    "title": str(upload.get("filename") or "Upload"),
                    "url": source_url,
                    "score": 1.0,
                    "excerpt": text[:280] if text else "Uploaded file attached",
                }
            )

    base_prompt = (
        "You are OAssist, an engineering knowledge-base assistant. "
        "Be concise and practical. "
        "If context is insufficient, state it explicitly. "
        "Do not hallucinate."
    )

    if lang == "ru":
        context_intro = (
            "Используй контекст документов Outline, Notion и Files ниже для фактов и добавляй ссылки на источники, когда это уместно."
        )
    else:
        context_intro = "Use Outline, Notion, and Files document context below for factual statements and cite sources when relevant."

    lang_hint = (payload.chat_language_hint or "").strip()
    if lang_hint:
        system_prompt = (
            f"{base_prompt} "
            "Always reply in the language of the first user message in this chat, "
            "unless the user explicitly asks to switch language. "
            f"First user message sample: {lang_hint[:280]}"
        )
    elif lang == "ru":
        system_prompt = (
            f"{base_prompt} "
            "Всегда отвечай на русском языке в рамках текущего диалога, "
            "если пользователь явно не попросил переключить язык."
        )
    else:
        system_prompt = (
            f"{base_prompt} "
            "Always answer in English for this chat unless the user explicitly asks to switch language."
        )

    if lang == "ru":
        language_guard = (
            "CRITICAL LANGUAGE RULE: Отвечай только на русском языке. "
            "Не переключайся на английский без явной просьбы пользователя."
        )
        output_language_prompt = (
            "OUTPUT FORMAT RULE: Финальный ответ должен быть полностью на русском языке, "
            "включая заголовки и пояснения. Допускаются только технические термины и названия как в оригинале."
        )
    else:
        language_guard = (
            "CRITICAL LANGUAGE RULE: Reply only in English. "
            "Do not switch to Russian unless the user explicitly asks."
        )
        output_language_prompt = (
            "OUTPUT FORMAT RULE: The final answer must be fully in English, including headings and explanations."
        )

    messages: list[dict[str, str]] = [
        {"role": "system", "content": language_guard},
        {"role": "system", "content": system_prompt},
        {"role": "system", "content": output_language_prompt},
    ]

    if custom_prompt:
        messages.append(
            {
                "role": "system",
                "content": (
                    "Apply the following user custom instructions with high priority in this chat. "
                    "These instructions override style/format defaults, while safety restrictions still apply:\n"
                    f"{custom_prompt[:4000]}"
                ),
            }
        )

    if context:
        messages.append(
            {
                "role": "system",
                "content": f"{context_intro}\n\nDocument context:\n{context}",
            }
        )

    if full_docs:
        blocks: list[str] = []
        for idx, item in enumerate(full_docs, start=1):
            doc_id = str(item.get("document_id") or "").strip()
            url = _source_download_url(doc_id, item.get("url"))
            blocks.append(
                f"[DOC {idx}] {item['title']}\nURL: {url}\nCONTENT:\n{item['text']}"
            )
        messages.append(
            {
                "role": "system",
                "content": (
                    "Use full document access below when the user references document names or asks for exact commands. "
                    "Prefer full-document facts over short snippets when both exist.\n\n"
                    + "\n\n".join(blocks)
                ),
            }
        )

    if upload_blocks:
        messages.append(
            {
                "role": "system",
                "content": (
                    "Use uploaded files below as user-provided context. "
                    "If content is incomplete or low confidence, say so explicitly.\n\n"
                    + "\n\n".join(upload_blocks)
                ),
            }
        )

    for item in payload.history[-16:]:
        text = item.content.strip()
        if text:
            messages.append({"role": item.role, "content": text})

    messages.append({"role": "user", "content": payload.message.strip()})
    return messages, sources, lang


def _sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/ui", status_code=302)


@app.get("/ui")
def ui() -> FileResponse:
    return FileResponse(
        CHAT_UI_PATH,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/favicon.svg")
def favicon_svg() -> FileResponse:
    return FileResponse(FAVICON_PATH)


@app.get("/favicon.ico")
def favicon_ico() -> RedirectResponse:
    return RedirectResponse(url="/favicon.svg", status_code=302)


@app.get("/auth/oidc/config", response_model=AuthOidcConfigResponse)
def auth_oidc_config(settings: Settings = Depends(get_settings)):
    configured = _is_auth_oidc_configured(settings)
    enabled = _is_auth_oidc_enabled(settings)
    display_name = str(settings.auth_oidc_display_name or "").strip() or "OIDC"
    return {
        "enabled": enabled,
        "configured": configured,
        "display_name": display_name if configured else None,
    }


@app.get("/auth/oidc/global", response_model=AuthOidcGlobalSettingsResponse)
def auth_oidc_global_get(
    settings: Settings = Depends(get_settings),
    _user: AuthUser = Depends(require_super_admin_role),
):
    configured = _is_auth_oidc_configured(settings)
    display_name = str(settings.auth_oidc_display_name or "").strip() or "OIDC"
    enabled = bool(get_oidc_login_enabled(settings)) if configured else False
    return {
        "configured": configured,
        "enabled": enabled,
        "display_name": display_name if configured else None,
    }


@app.put("/auth/oidc/global", response_model=AuthOidcGlobalSettingsResponse)
def auth_oidc_global_put(
    payload: AuthOidcGlobalSettingsRequest,
    settings: Settings = Depends(get_settings),
    _user: AuthUser = Depends(require_super_admin_role),
):
    configured = _is_auth_oidc_configured(settings)
    if not configured and bool(payload.enabled):
        raise HTTPException(status_code=400, detail="OIDC authentication is not configured")

    enabled = set_oidc_login_enabled(settings, bool(payload.enabled) if configured else False)
    display_name = str(settings.auth_oidc_display_name or "").strip() or "OIDC"
    return {
        "configured": configured,
        "enabled": bool(enabled) if configured else False,
        "display_name": display_name if configured else None,
    }


@app.get("/auth/oidc/start")
@app.get("/auth/oidc/start/")
def auth_oidc_start(
    settings: Settings = Depends(get_settings),
    return_to: str | None = Query(default="/ui"),
):
    if not _is_auth_oidc_enabled(settings):
        raise HTTPException(status_code=400, detail=_auth_oidc_unavailable_detail(settings))

    state = create_auth_oidc_state(settings, return_to=_safe_return_to(return_to))
    params = {
        "client_id": str(settings.auth_oidc_client_id or "").strip(),
        "redirect_uri": str(settings.auth_oidc_redirect_uri or "").strip(),
        "response_type": "code",
        "scope": str(settings.auth_oidc_scopes or "openid profile email").strip(),
        "prompt": "login",
        "max_age": "0",
        "state": state,
    }
    auth_url = f"{str(settings.auth_oidc_auth_uri or '').strip()}?{urlencode(params)}"
    return RedirectResponse(url=auth_url, status_code=302)


@app.get("/auth/oidc.callback")
@app.get("/auth/oidc.callback/")
@app.get("/auth/oidc/callback")
@app.get("/auth/oidc/callback/")
def auth_oidc_callback(
    settings: Settings = Depends(get_settings),
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    error_description: str | None = Query(default=None),
):
    if not _is_auth_oidc_enabled(settings):
        return _auth_error_redirect(_auth_oidc_unavailable_detail(settings))

    if error:
        detail = str(error_description or error).strip()
        return _auth_error_redirect(f"OIDC login failed: {detail}")

    if not code or not state:
        return _auth_error_redirect("OIDC response is incomplete")

    return_to = consume_auth_oidc_state(settings, state)
    if not return_to:
        return _auth_error_redirect("OIDC state is invalid or expired")

    token_url = str(settings.auth_oidc_token_uri or "").strip()
    userinfo_url = str(settings.auth_oidc_userinfo_uri or "").strip()
    redirect_uri = str(settings.auth_oidc_redirect_uri or "").strip()
    client_id = str(settings.auth_oidc_client_id or "").strip()
    client_secret = str(settings.auth_oidc_client_secret or "").strip()
    groups_claim_path = str(settings.auth_oidc_groups_claim or "groups").strip() or "groups"
    username_claim_path = str(settings.auth_oidc_username_claim or "preferred_username").strip() or "preferred_username"
    email_claim_path = str(settings.auth_oidc_email_claim or "email").strip() or "email"

    try:
        with httpx.Client(timeout=settings.request_timeout_seconds) as client:
            token_response = client.post(
                token_url,
                data={
                    "grant_type": "authorization_code",
                    "code": str(code).strip(),
                    "redirect_uri": redirect_uri,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if token_response.status_code >= 400:
                error_detail = ""
                try:
                    payload = token_response.json()
                    if isinstance(payload, dict):
                        err = str(payload.get("error") or "").strip()
                        err_desc = str(payload.get("error_description") or "").strip()
                        error_detail = f"{err}: {err_desc}".strip(": ")
                except Exception:
                    error_detail = ""
                if not error_detail:
                    error_detail = f"HTTP {token_response.status_code}"
                return _auth_error_redirect(f"Failed to exchange OIDC code: {error_detail}", return_to=return_to)
            token_data = token_response.json()
    except Exception as exc:
        return _auth_error_redirect(f"Failed to exchange OIDC code: {exc}", return_to=return_to)

    access_token = str((token_data or {}).get("access_token") or "").strip()
    if not access_token:
        return _auth_error_redirect("OIDC token response did not include access token", return_to=return_to)

    id_token_claims = _decode_jwt_payload(str((token_data or {}).get("id_token") or ""))
    access_token_claims = _decode_jwt_payload(access_token)

    userinfo_data: dict[str, Any] | None = None
    try:
        with httpx.Client(timeout=settings.request_timeout_seconds) as client:
            userinfo_response = client.get(
                userinfo_url,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if userinfo_response.status_code < 400:
                parsed_userinfo = userinfo_response.json()
                if isinstance(parsed_userinfo, dict):
                    userinfo_data = parsed_userinfo
    except Exception:
        userinfo_data = None

    claims: dict[str, Any] = {}
    if isinstance(id_token_claims, dict):
        claims.update(id_token_claims)
    if isinstance(access_token_claims, dict):
        claims.update(access_token_claims)
    if isinstance(userinfo_data, dict):
        claims.update(userinfo_data)

    groups = _groups_from_claim(_claim_value(claims, groups_claim_path))
    if not groups and groups_claim_path == "groups":
        groups = _groups_from_claim(_claim_value(claims, "realm_access.roles"))
    if not groups and groups_claim_path == "groups":
        groups = _groups_from_claim(_claim_value(claims, f"resource_access.{client_id}.roles"))
    if not groups and groups_claim_path == "groups":
        groups = _groups_from_claim(_claim_value(claims, "resource_access.account.roles"))
    try:
        role = _oidc_role_from_groups(settings, groups)
    except HTTPException as exc:
        return _auth_error_redirect(str(exc.detail), return_to=return_to)

    username = ""
    for candidate in (
        _claim_value(claims, username_claim_path),
        _claim_value(claims, email_claim_path),
        _claim_value(claims, "preferred_username"),
        _claim_value(claims, "email"),
        _claim_value(claims, "sub"),
    ):
        if candidate is None:
            continue
        if isinstance(candidate, (dict, list, tuple, set)):
            continue
        value = str(candidate).strip()
        if value:
            username = value
            break

    if not username:
        return _auth_error_redirect("OIDC did not provide a usable username", return_to=return_to)

    try:
        user = upsert_oidc_user(settings, username=username, role=role)
    except ValueError as exc:
        return _auth_error_redirect(f"OIDC login denied: {exc}", return_to=return_to)

    token = create_session(settings, user.id, auth_method=AUTH_METHOD_OIDC)
    login_ticket = create_auth_oidc_login_ticket(settings, user.id)
    max_age = settings.auth_session_ttl_hours * 3600
    redirect_url = _append_query_param(return_to, "auth_ok", "1")
    redirect_url = _append_query_param(redirect_url, "auth_ticket", login_ticket)
    redirect = RedirectResponse(url=redirect_url, status_code=302)
    redirect.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    redirect.headers["Pragma"] = "no-cache"
    redirect.headers["Expires"] = "0"
    redirect.headers["Vary"] = "Cookie"
    redirect.set_cookie(
        key="oassist_session",
        value=token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=max_age,
        path="/",
    )
    return redirect


@app.post("/auth/oidc/exchange", response_model=AuthUserResponse)
def auth_oidc_exchange(
    response: Response,
    ticket: str = Query(..., min_length=16),
    settings: Settings = Depends(get_settings),
):
    user = consume_auth_oidc_login_ticket(settings, ticket)
    if user is None:
        raise HTTPException(status_code=400, detail="OIDC login exchange is invalid or expired")

    token = create_session(settings, user.id, auth_method=AUTH_METHOD_OIDC)
    max_age = settings.auth_session_ttl_hours * 3600
    response.set_cookie(
        key="oassist_session",
        value=token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=max_age,
        path="/",
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["Vary"] = "Cookie"
    payload = _public_user(user)
    payload["notion_connected"] = bool(get_notion_connection(settings, user.id))
    payload["source_mode"] = effective_source_mode_for_user(user)
    return payload


@app.post("/auth/login", response_model=LoginResponse)
def auth_login(payload: LoginRequest, response: Response, settings: Settings = Depends(get_settings)):
    user = authenticate_user(settings, payload.username, payload.password)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    token = create_session(settings, user.id, auth_method=AUTH_METHOD_LOCAL)
    max_age = settings.auth_session_ttl_hours * 3600
    response.set_cookie(
        key="oassist_session",
        value=token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=max_age,
        path="/",
    )
    user_payload = _public_user(user)
    user_payload["notion_connected"] = bool(get_notion_connection(settings, user.id))
    user_payload["source_mode"] = effective_source_mode_for_user(user)
    return {"token": token, "user": user_payload}


@app.post("/auth/logout")
def auth_logout(request: Request, response: Response, settings: Settings = Depends(get_settings)):
    token = _extract_session_token(request)
    if token:
        revoke_session(settings, token)
    response.delete_cookie("oassist_session", path="/")
    return {"ok": True}


@app.get("/auth/me", response_model=AuthUserResponse)
def auth_me(response: Response, current: AuthUser = Depends(get_current_user), settings: Settings = Depends(get_settings)):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["Vary"] = "Cookie"
    payload = _public_user(current)
    payload["notion_connected"] = bool(get_notion_connection(settings, current.id))
    payload["source_mode"] = effective_source_mode_for_user(current)
    return payload


@app.post("/auth/me/change-password")
def auth_change_password(
    payload: ChangePasswordRequest,
    settings: Settings = Depends(get_settings),
    current: AuthUser = Depends(get_current_user),
):
    if str(getattr(current, "auth_method", AUTH_METHOD_LOCAL) or AUTH_METHOD_LOCAL).strip().lower() == AUTH_METHOD_OIDC:
        raise HTTPException(status_code=403, detail="Password is managed by external SSO")

    auth_user = authenticate_user(settings, current.username, payload.current_password)
    if auth_user is None:
        raise HTTPException(status_code=400, detail="Current password is invalid")

    try:
        update_user(settings, current.id, password=payload.new_password)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True}


@app.get("/auth/users", dependencies=[Depends(require_admin_role)])
def auth_users(settings: Settings = Depends(get_settings)):
    return list_users(settings)


@app.post("/auth/users")
def auth_create_user(
    payload: CreateUserRequest,
    settings: Settings = Depends(get_settings),
    current: AuthUser = Depends(require_admin_role),
):
    if str(payload.role or "").strip().lower() == ROLE_ADMIN and not is_super_admin(current):
        raise HTTPException(status_code=403, detail="Only super admin can create admin users")
    try:
        return create_user(settings, payload.username, payload.password, payload.role, payload.source_mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/auth/users/{user_id}")
def auth_update_user(
    user_id: int,
    payload: UpdateUserRequest,
    settings: Settings = Depends(get_settings),
    current: AuthUser = Depends(require_admin_role),
):
    try:
        target_user = get_user(settings, int(user_id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    target_role = str(target_user.get("role") or "").strip().lower()
    desired_role = str(payload.role or "").strip().lower() if payload.role is not None else target_role
    if not is_super_admin(current):
        if target_role == ROLE_ADMIN or desired_role == ROLE_ADMIN:
            raise HTTPException(status_code=403, detail="Only super admin can manage admin users")

    if payload.password is not None:
        if target_role == ROLE_ADMIN and not is_super_admin(current):
            raise HTTPException(status_code=403, detail="Only super admin can change admin passwords")

    try:
        return update_user(
            settings,
            user_id,
            username=payload.username,
            password=payload.password,
            role=payload.role,
            is_active=payload.is_active,
            source_mode=payload.source_mode,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/auth/users/{user_id}")
def auth_delete_user(
    user_id: int,
    settings: Settings = Depends(get_settings),
    current: AuthUser = Depends(require_admin_role),
):
    try:
        target_user = get_user(settings, int(user_id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    target_role = str(target_user.get("role") or "").strip().lower()
    if target_role == ROLE_ADMIN and not is_super_admin(current):
        raise HTTPException(status_code=403, detail="Only super admin can delete admin users")
    try:
        delete_user(settings, user_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True}


@app.get("/health/providers", response_model=ProvidersHealthResponse)
def provider_health(settings: Settings = Depends(get_settings), _user: AuthUser = Depends(get_current_user)):
    gateway = LLMGateway(settings)
    return gateway.provider_health()


@app.get("/models/ollama", response_model=OllamaModelsResponse)
def ollama_models(settings: Settings = Depends(get_settings), _user: AuthUser = Depends(get_current_user)):
    gateway = LLMGateway(settings)
    try:
        models = gateway.list_ollama_models()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"models": models, "default_model": settings.ollama_model}


@app.get("/outline/tree")
def outline_tree(settings: Settings = Depends(get_settings), user: AuthUser = Depends(get_current_user)):
    if SOURCE_OUTLINE not in _effective_enabled_sources(settings, user):
        return {"nodes": [], "count": 0}
    try:
        tree = get_outline_tree(settings)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    allowed_document_ids = None if is_super_admin(user) else _resolve_allowed_document_ids(settings, user)
    visible_nodes = _filter_tree_nodes_by_allowed(list(tree.get("nodes") or []), allowed_document_ids)
    visible_count = int(tree.get("count", 0)) if allowed_document_ids is None else _count_tree_nodes(visible_nodes)
    return {"nodes": visible_nodes, "count": visible_count}


@app.get("/notion/tree")
def notion_tree(settings: Settings = Depends(get_settings), user: AuthUser = Depends(get_current_user)):
    if SOURCE_NOTION not in _effective_enabled_sources(settings, user):
        return {"nodes": [], "count": 0, "disabled": True}

    notion_access_token = _resolve_notion_access_token(settings, user)
    if not notion_access_token:
        raise HTTPException(status_code=400, detail="Notion is not connected")

    try:
        tree = get_notion_tree(settings, notion_access_token)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    allowed_document_ids = None if is_super_admin(user) else _resolve_allowed_document_ids(settings, user)
    visible_nodes = _filter_tree_nodes_by_allowed(list(tree.get("nodes") or []), allowed_document_ids)
    visible_count = int(tree.get("count", 0)) if allowed_document_ids is None else _count_tree_nodes(visible_nodes)
    return {"nodes": visible_nodes, "count": visible_count, "disabled": False}


@app.get("/files/roots")
def files_roots_get(settings: Settings = Depends(get_settings), _user: AuthUser = Depends(require_super_admin_role)):
    return {"items": list_files_roots(settings, include_disabled=True)}


@app.post("/files/roots")
def files_roots_post(
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    _user: AuthUser = Depends(require_super_admin_role),
):
    try:
        root = create_files_root(
            settings,
            name=str((payload or {}).get("name") or "").strip(),
            root_path=str((payload or {}).get("root_path") or "").strip(),
            enabled=bool((payload or {}).get("enabled", True)),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return root


@app.patch("/files/roots/{root_id}")
def files_roots_patch(
    root_id: int,
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    _user: AuthUser = Depends(require_super_admin_role),
):
    try:
        row = update_files_root(
            settings,
            int(root_id),
            name=(payload or {}).get("name"),
            root_path=(payload or {}).get("root_path"),
            enabled=(payload or {}).get("enabled"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return row


@app.delete("/files/roots/{root_id}")
def files_roots_delete(
    root_id: int,
    settings: Settings = Depends(get_settings),
    _user: AuthUser = Depends(require_super_admin_role),
):
    delete_files_root(settings, int(root_id))
    return {"ok": True}


@app.get("/files/settings")
def files_settings_get(settings: Settings = Depends(get_settings), _user: AuthUser = Depends(require_super_admin_role)):
    return get_files_feature_settings(settings)


@app.put("/files/settings")
def files_settings_put(
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    _user: AuthUser = Depends(require_super_admin_role),
):
    try:
        return set_files_feature_settings(settings, payload if isinstance(payload, dict) else {})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/files/tree")
def files_tree(settings: Settings = Depends(get_settings), user: AuthUser = Depends(get_current_user)):
    _require_files_source_enabled(settings, user)
    tree = get_files_tree(settings)
    allowed_document_ids = None if is_super_admin(user) else _resolve_allowed_document_ids(settings, user)
    visible_nodes = _filter_tree_nodes_by_allowed(list(tree.get("nodes") or []), allowed_document_ids)
    visible_count = int(tree.get("count", 0)) if allowed_document_ids is None else _count_tree_nodes(visible_nodes)
    return {"nodes": visible_nodes, "count": visible_count}


@app.get("/files/document")
def files_document_get(
    document_id: str = Query(min_length=1),
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    _require_files_source_enabled(settings, user)
    parsed = parse_files_document_id(document_id)
    if parsed is None:
        raise HTTPException(status_code=400, detail="Invalid files document_id")
    doc_id = document_id.strip()
    if parsed[2]:
        doc_id = files_folder_document_id(parsed[0], parsed[1])
    else:
        doc_id = files_document_id(parsed[0], parsed[1])
    _require_document_access(settings, user, doc_id)
    row = get_files_document(settings, doc_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Document not found")
    return row


@app.get("/files/download")
def files_document_download(
    document_id: str = Query(..., min_length=1),
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    _require_files_source_enabled(settings, user)
    doc_id = str(document_id or "").strip()
    parsed = parse_files_document_id(doc_id)
    if parsed is None:
        raise HTTPException(status_code=400, detail="Invalid files document_id")
    root_id, rel_path, is_folder = parsed
    if is_folder:
        raise HTTPException(status_code=400, detail="Folders cannot be downloaded")

    _require_document_access(settings, user, files_document_id(root_id, rel_path))
    root = get_files_root(settings, root_id)
    if root is None:
        raise HTTPException(status_code=404, detail="Root not found")
    root_path = Path(str(root.get("root_path") or ""))
    file_path = (root_path / rel_path).resolve()
    try:
        file_path.relative_to(root_path.resolve())
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid file path") from exc
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Document file not found")

    return FileResponse(
        file_path,
        media_type="application/octet-stream",
        filename=file_path.name,
    )


@app.post("/chat/uploads")
async def chat_upload_create(
    file: UploadFile = File(...),
    mode: str = Form(default="ephemeral"),
    chat_id: str | None = Form(default=None),
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    _require_files_source_enabled(settings, user)
    cleanup_expired_assets(settings)
    data = await file.read()
    try:
        created = create_chat_upload(
            settings,
            user_id=user.id,
            chat_id=chat_id,
            mode=mode,
            filename=file.filename or "upload.bin",
            content_type=file.content_type,
            data=data,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return created


@app.get("/chat/uploads/{upload_id}")
def chat_upload_get(
    upload_id: str,
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    cleanup_expired_assets(settings)
    row = get_chat_upload(settings, upload_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Upload not found")
    if row["user_id"] != user.id and not _user_can_manage_foreign_assets(user):
        raise HTTPException(status_code=403, detail="Upload is not accessible")
    if bool(row.get("expired")):
        raise HTTPException(status_code=410, detail="Upload has expired")
    return {
        "id": row["id"],
        "mode": row["mode"],
        "mime": row["mime"],
        "size": row["size"],
        "filename": row["filename"],
        "chat_id": row.get("chat_id"),
        "expires_at": row["expires_at"],
        "indexed_document_id": row.get("indexed_document_id"),
        "text_preview": str(row.get("extracted_text") or "")[:4000],
    }


@app.get("/chat/uploads/{upload_id}/download")
def chat_upload_download(
    upload_id: str,
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    cleanup_expired_assets(settings)
    row = get_chat_upload(settings, upload_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Upload not found")
    if row["user_id"] != user.id and not _user_can_manage_foreign_assets(user):
        raise HTTPException(status_code=403, detail="Upload is not accessible")
    if bool(row.get("expired")):
        raise HTTPException(status_code=410, detail="Upload has expired")

    path = Path(str(row.get("storage_path") or ""))
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Upload file is missing")

    return FileResponse(
        path,
        media_type=str(row.get("mime") or "application/octet-stream"),
        filename=str(row.get("filename") or path.name),
    )


@app.delete("/chat/uploads/{upload_id}")
def chat_upload_delete(
    upload_id: str,
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    cleanup_expired_assets(settings)
    row = get_chat_upload(settings, upload_id)
    if row is None:
        return {"ok": True}
    if row["user_id"] != user.id and not _user_can_manage_foreign_assets(user):
        raise HTTPException(status_code=403, detail="Upload is not accessible")
    delete_chat_upload(settings, upload_id)
    return {"ok": True}


@app.get("/notion/connection", response_model=NotionConnectionResponse)
def notion_connection_get(settings: Settings = Depends(get_settings), user: AuthUser = Depends(get_current_user)):
    if not _is_source_globally_enabled(settings, SOURCE_NOTION):
        return {
            "connected": False,
            "mode": None,
            "workspace_id": None,
            "workspace_name": None,
            "oauth_available": False,
        }

    connection = get_notion_connection(settings, user.id)
    oauth_available = bool(
        str(settings.notion_oauth_client_id or "").strip()
        and str(settings.notion_oauth_client_secret or "").strip()
        and str(settings.notion_oauth_redirect_uri or "").strip()
    )
    if connection:
        return {
            "connected": True,
            "mode": connection.get("mode") or NOTION_CONNECTION_MODE_TOKEN,
            "workspace_id": str(connection.get("workspace_id") or "") or None,
            "workspace_name": str(connection.get("workspace_name") or "") or None,
            "oauth_available": oauth_available,
        }

    if str(settings.notion_api_token or "").strip():
        return {
            "connected": True,
            "mode": NOTION_CONNECTION_MODE_TOKEN,
            "workspace_id": None,
            "workspace_name": None,
            "oauth_available": oauth_available,
        }

    return {
        "connected": False,
        "mode": None,
        "workspace_id": None,
        "workspace_name": None,
        "oauth_available": oauth_available,
    }


@app.post("/notion/connection/token", response_model=NotionConnectionResponse)
def notion_connection_token_put(
    payload: NotionTokenConnectRequest,
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    if not _is_source_globally_enabled(settings, SOURCE_NOTION):
        raise HTTPException(status_code=400, detail="Notion source is disabled globally")

    token = str(payload.token or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="Notion token is required")

    try:
        get_notion_tree(settings, token)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Notion token validation failed: {exc}") from exc

    try:
        connection = upsert_notion_connection(
            settings,
            user.id,
            mode=NOTION_CONNECTION_MODE_TOKEN,
            access_token=token,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "connected": True,
        "mode": connection.get("mode") or NOTION_CONNECTION_MODE_TOKEN,
        "workspace_id": str(connection.get("workspace_id") or "") or None,
        "workspace_name": str(connection.get("workspace_name") or "") or None,
        "oauth_available": bool(
            str(settings.notion_oauth_client_id or "").strip()
            and str(settings.notion_oauth_client_secret or "").strip()
            and str(settings.notion_oauth_redirect_uri or "").strip()
        ),
    }


@app.delete("/notion/connection")
def notion_connection_delete(settings: Settings = Depends(get_settings), user: AuthUser = Depends(get_current_user)):
    if not _is_source_globally_enabled(settings, SOURCE_NOTION):
        raise HTTPException(status_code=400, detail="Notion source is disabled globally")
    delete_notion_connection(settings, user.id)
    return {"ok": True}


@app.get("/notion/oauth/start")
def notion_oauth_start(settings: Settings = Depends(get_settings), user: AuthUser = Depends(get_current_user)):
    if not _is_source_globally_enabled(settings, SOURCE_NOTION):
        raise HTTPException(status_code=400, detail="Notion source is disabled globally")

    client_id = str(settings.notion_oauth_client_id or "").strip()
    client_secret = str(settings.notion_oauth_client_secret or "").strip()
    redirect_uri = str(settings.notion_oauth_redirect_uri or "").strip()
    if not client_id or not client_secret or not redirect_uri:
        raise HTTPException(status_code=400, detail="Notion OAuth is not configured")

    state = create_notion_oauth_state(settings, user.id)
    params = {
        "owner": "user",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "state": state,
    }
    auth_url = f"{str(settings.notion_oauth_authorize_url or '').strip()}?{urlencode(params)}"
    return RedirectResponse(url=auth_url, status_code=302)


@app.get("/notion/oauth/callback")
def notion_oauth_callback(
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
):
    if not _is_source_globally_enabled(settings, SOURCE_NOTION):
        raise HTTPException(status_code=400, detail="Notion source is disabled globally")

    if error:
        raise HTTPException(status_code=400, detail=f"Notion OAuth failed: {error}")
    if not code or not state:
        raise HTTPException(status_code=400, detail="Notion OAuth response is incomplete")
    if not consume_notion_oauth_state(settings, user.id, state):
        raise HTTPException(status_code=400, detail="Notion OAuth state is invalid or expired")

    client_id = str(settings.notion_oauth_client_id or "").strip()
    client_secret = str(settings.notion_oauth_client_secret or "").strip()
    redirect_uri = str(settings.notion_oauth_redirect_uri or "").strip()
    if not client_id or not client_secret or not redirect_uri:
        raise HTTPException(status_code=400, detail="Notion OAuth is not configured")

    token_url = str(settings.notion_oauth_token_url or "").strip()
    basic = b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {basic}", "Content-Type": "application/json"}
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }

    try:
        with httpx.Client(timeout=settings.request_timeout_seconds) as client:
            response = client.post(token_url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to exchange Notion OAuth code: {exc}") from exc

    access_token = str((data or {}).get("access_token") or "").strip()
    if not access_token:
        raise HTTPException(status_code=400, detail="Notion OAuth did not return access token")

    workspace_id = str((data or {}).get("workspace_id") or "").strip() or None
    workspace_name = str((data or {}).get("workspace_name") or "").strip() or None
    bot_id = str((data or {}).get("bot_id") or "").strip() or None

    upsert_notion_connection(
        settings,
        user.id,
        mode=NOTION_CONNECTION_MODE_OAUTH,
        access_token=access_token,
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        bot_id=bot_id,
    )

    return RedirectResponse(url="/ui?notion=connected", status_code=302)


@app.get("/sources/global", response_model=SourceGlobalSettingsResponse)
def source_global_get(settings: Settings = Depends(get_settings), _user: AuthUser = Depends(require_super_admin_role)):
    state = get_global_source_settings(settings)
    return {
        "outline_enabled": bool(state.get(SOURCE_OUTLINE, True)),
        "notion_enabled": bool(state.get(SOURCE_NOTION, True)),
        "files_enabled": bool(state.get(SOURCE_FILES, True)),
    }


@app.put("/sources/global", response_model=SourceGlobalSettingsResponse)
def source_global_put(
    payload: SourceGlobalSettingsRequest,
    settings: Settings = Depends(get_settings),
    _user: AuthUser = Depends(require_super_admin_role),
):
    current_state = get_global_source_settings(settings)
    state = set_global_source_settings(
        settings,
        outline_enabled=bool(payload.outline_enabled),
        notion_enabled=bool(payload.notion_enabled),
        files_enabled=(
            bool(payload.files_enabled)
            if payload.files_enabled is not None
            else bool(current_state.get(SOURCE_FILES, True))
        ),
    )
    return {
        "outline_enabled": bool(state.get(SOURCE_OUTLINE, True)),
        "notion_enabled": bool(state.get(SOURCE_NOTION, True)),
        "files_enabled": bool(state.get(SOURCE_FILES, True)),
    }


@app.get("/acl/global")
def acl_global_get(settings: Settings = Depends(get_settings), _user: AuthUser = Depends(require_super_admin_role)):
    return {"document_ids": sorted(get_acl_global_defaults(settings))}


@app.put("/acl/global")
def acl_global_put(
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(require_super_admin_role),
):
    raw_ids = payload.get("document_ids") if isinstance(payload, dict) else None
    if raw_ids is None:
        raw_ids = []
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="document_ids must be an array")
    global_enabled = globally_enabled_sources(settings)
    raw_normalized = {
        make_document_id(parsed[0], parsed[1])
        for item in raw_ids
        if (parsed := split_document_id(str(item).strip()))
    }
    raw_normalized = {
        doc_id
        for doc_id in raw_normalized
        if (split_document_id(doc_id) or (None, None))[0] in global_enabled
    }
    notion_access_token = _resolve_notion_access_token(settings, user)
    doc_ids = _validate_acl_document_ids(
        settings,
        raw_normalized,
        notion_access_token=notion_access_token,
    )
    expanded_doc_ids = _expand_acl_document_ids(
        settings,
        doc_ids,
        notion_access_token=notion_access_token,
    )
    set_acl_global_defaults(settings, expanded_doc_ids)
    return {"ok": True, "document_ids": sorted(expanded_doc_ids)}


@app.get("/acl/users/{user_id}", dependencies=[Depends(require_super_admin_role)])
def acl_user_get(
    user_id: int,
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(require_super_admin_role),
):
    try:
        target_user = get_user(settings, int(user_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    overrides = get_acl_user_overrides(settings, int(user_id))
    is_target_super_admin = str(target_user.get("username") or "").strip().lower() == "admin"
    global_enabled = globally_enabled_sources(settings)
    target_enabled_sources = enabled_sources_from_mode(str(target_user.get("source_mode") or "outline"))
    target_enabled_sources.add(SOURCE_FILES)
    target_enabled_sources = {source for source in target_enabled_sources if source in global_enabled}
    if is_target_super_admin:
        target_enabled_sources = set(global_enabled)
    overrides = {
        doc_id
        for doc_id in overrides
        if (split_document_id(doc_id) or (None, None))[0] in target_enabled_sources
    }
    default_selection = _default_acl_selection_for_target_user(settings, user, target_user)
    scope_ids = _acl_scope_for_target_user(settings, user, target_user)
    overrides = {doc_id for doc_id in overrides if doc_id in scope_ids}
    selected = set(overrides) if overrides else {doc_id for doc_id in default_selection if doc_id in scope_ids}

    return {"user_id": int(user_id), "document_ids": sorted(selected)}


@app.put("/acl/users/{user_id}", dependencies=[Depends(require_super_admin_role)])
def acl_user_put(
    user_id: int,
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(require_super_admin_role),
):
    raw_ids = payload.get("document_ids") if isinstance(payload, dict) else None
    if raw_ids is None:
        raw_ids = []
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="document_ids must be an array")
    try:
        target_user = get_user(settings, int(user_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    is_target_super_admin = str(target_user.get("username") or "").strip().lower() == "admin"
    if is_target_super_admin:
        raise HTTPException(status_code=400, detail="Super admin ACL cannot be changed")

    target_enabled_sources = enabled_sources_from_mode(str(target_user.get("source_mode") or "outline"))
    target_enabled_sources.add(SOURCE_FILES)
    global_enabled = globally_enabled_sources(settings)
    target_enabled_sources = {source for source in target_enabled_sources if source in global_enabled}
    if str(target_user.get("username") or "").strip().lower() == "admin":
        target_enabled_sources = set(global_enabled)

    # allow values only for target user's enabled sources
    raw_normalized = {
        make_document_id(parsed[0], parsed[1])
        for item in raw_ids
        if (parsed := split_document_id(str(item).strip()))
    }
    raw_normalized = {
        doc_id
        for doc_id in raw_normalized
        if (split_document_id(doc_id) or (None, None))[0] in target_enabled_sources
    }

    notion_access_token = _resolve_notion_access_token(settings, user)

    doc_ids = _validate_acl_document_ids(
        settings,
        raw_normalized,
        notion_access_token=notion_access_token,
    )
    expanded_doc_ids = _expand_acl_document_ids(
        settings,
        doc_ids,
        notion_access_token=notion_access_token,
    )

    scope_ids = _acl_scope_for_target_user(settings, user, target_user)
    if scope_ids:
        expanded_doc_ids = {doc_id for doc_id in expanded_doc_ids if doc_id in scope_ids}
    else:
        expanded_doc_ids = set()

    set_acl_user_overrides(settings, int(user_id), expanded_doc_ids)
    return {"ok": True, "user_id": int(user_id), "document_ids": sorted(expanded_doc_ids)}


@app.get("/acl/me")
def acl_me(settings: Settings = Depends(get_settings), user: AuthUser = Depends(get_current_user)):
    allowed_document_ids = None if is_super_admin(user) else _resolve_allowed_document_ids(settings, user)
    notion_connection = get_notion_connection(settings, user.id)
    global_enabled = globally_enabled_sources(settings)
    enabled_for_user = _effective_enabled_sources(settings, user)
    return {
        "is_super_admin": bool(is_super_admin(user)),
        "source_mode": effective_source_mode_for_user(user),
        "enabled_sources": sorted(enabled_for_user),
        "global_enabled_sources": sorted(global_enabled),
        "notion_connected": (
            SOURCE_NOTION in global_enabled
            and (bool(notion_connection) or bool(str(settings.notion_api_token or "").strip()))
        ),
        "allowed_document_ids": None if allowed_document_ids is None else sorted(allowed_document_ids),
    }


@app.get("/chat/state")
def chat_state_get(settings: Settings = Depends(get_settings), user: AuthUser = Depends(get_current_user)):
    state = get_chat_state(settings, user.id)
    meta = get_chat_state_meta(settings, user.id)
    return {"state": state, "updated_at": meta.get("updated_at")}


@app.put("/chat/state")
def chat_state_put(
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    state = payload.get("state") if isinstance(payload, dict) else None
    if not isinstance(state, dict):
        raise HTTPException(status_code=400, detail="state must be an object")
    result = save_chat_state(settings, user.id, state)
    return {"ok": True, **result}


@app.get("/chat/state/meta")
def chat_state_meta(settings: Settings = Depends(get_settings), user: AuthUser = Depends(get_current_user)):
    return get_chat_state_meta(settings, user.id)


@app.post("/chat", response_model=ChatResponse)
def chat(payload: ChatRequest, settings: Settings = Depends(get_settings), user: AuthUser = Depends(get_current_user)):
    rag = RAGService(settings)
    try:
        allowed_document_ids = _resolve_allowed_document_ids(settings, user)
        data = rag.answer(
            question=payload.question,
            provider=payload.provider,
            top_k=payload.top_k or settings.search_top_k,
            allowed_document_ids=allowed_document_ids,
        )
        return data
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/assistant/chat", response_model=AssistantChatResponse)
def assistant_chat(
    payload: AssistantChatRequest,
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    gateway = LLMGateway(settings)
    messages, sources, lang = _build_assistant_messages(payload, settings, user)
    try:
        output, provider = gateway.generate(
            messages=messages,
            requested_provider=payload.provider,
            requested_model=payload.model,
        )
        output = _enforce_answer_language(
            gateway=gateway,
            answer=output,
            expected_lang=lang,
            requested_provider=provider,
        )
        artifacts: list[dict[str, Any]] = []
        requested_formats: list[str] = []
        if payload.request_artifact_format:
            requested_formats = [str(payload.request_artifact_format).strip().lower()]
        else:
            requested_formats = _artifact_formats_from_message(payload.message)
        for format_name in requested_formats[:1]:
            try:
                artifact = create_artifact(
                    settings,
                    user_id=user.id,
                    chat_id=payload.chat_id,
                    format_name=format_name,
                    filename=f"oassist-{format_name}-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}",
                    content=output,
                )
                artifacts.append(
                    {
                        "id": artifact["id"],
                        "filename": artifact["filename"],
                        "format": artifact["format"],
                        "size": int(artifact["size"]),
                        "expires_at": artifact["expires_at"],
                        "download_url": artifact["download_url"],
                    }
                )
            except Exception:
                continue
        return {"provider": provider, "answer": output, "sources": sources, "artifacts": artifacts}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/assistant/chat/stream")
def assistant_chat_stream(
    payload: AssistantChatRequest,
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    gateway = LLMGateway(settings)

    def stream() -> Any:
        try:
            stream_lang = (payload.chat_language or "").strip().lower()
            if stream_lang not in {"ru", "en"}:
                hint = (payload.chat_language_hint or "").strip()
                stream_lang = _detect_language(hint or payload.message)
            yield _sse(
                "status",
                {
                    "stage": "prepare",
                    "message": "Ищу релевантные материалы..." if stream_lang == "ru" else "Collecting relevant context...",
                },
            )

            messages, sources, lang = _build_assistant_messages(payload, settings, user)
            if sources:
                yield _sse("sources", {"items": sources})

            yield _sse(
                "status",
                {
                    "stage": "generate",
                    "message": "Генерирую ответ..." if lang == "ru" else "Generating answer...",
                },
            )

            provider_used = "unknown"
            answer_parts: list[str] = []
            for event in gateway.stream_generate(
                messages=messages,
                requested_provider=payload.provider,
                requested_model=payload.model,
            ):
                kind = event.get("type")
                if kind == "provider":
                    provider_used = event.get("provider", "unknown")
                    yield _sse("provider", {"provider": provider_used})
                    continue
                if kind == "chunk":
                    chunk = event.get("content", "")
                    if chunk:
                        answer_parts.append(chunk)
                        yield _sse("chunk", {"text": chunk})

            answer_text = _enforce_answer_language(
                gateway=gateway,
                answer="".join(answer_parts).strip(),
                expected_lang=lang,
                requested_provider=provider_used if provider_used in {"ollama", "openai", "deepseek"} else payload.provider,
            )
            yield _sse(
                "done",
                {
                    "provider": provider_used,
                    "answer": answer_text,
                    "sources": sources,
                },
            )
            formats = [str(payload.request_artifact_format).strip().lower()] if payload.request_artifact_format else _artifact_formats_from_message(payload.message)
            artifacts: list[dict[str, Any]] = []
            for format_name in formats[:1]:
                try:
                    artifact = create_artifact(
                        settings,
                        user_id=user.id,
                        chat_id=payload.chat_id,
                        format_name=format_name,
                        filename=f"oassist-{format_name}-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}",
                        content=answer_text,
                    )
                    artifacts.append(
                        {
                            "id": artifact["id"],
                            "filename": artifact["filename"],
                            "format": artifact["format"],
                            "size": int(artifact["size"]),
                            "expires_at": artifact["expires_at"],
                            "download_url": artifact["download_url"],
                        }
                    )
                except Exception:
                    continue
            if artifacts:
                yield _sse("artifacts", {"items": artifacts})
        except Exception as exc:
            yield _sse("error", {"detail": str(exc)})

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(stream(), media_type="text/event-stream", headers=headers)


@app.post("/assistant/artifacts/generate")
def assistant_artifact_generate(
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    cleanup_expired_assets(settings)
    format_name = str((payload or {}).get("format") or "").strip().lower()
    content = str((payload or {}).get("content") or "")
    filename = str((payload or {}).get("filename") or "").strip() or None
    chat_id = str((payload or {}).get("chat_id") or "").strip() or None
    if not format_name:
        raise HTTPException(status_code=400, detail="format is required")
    if not content.strip():
        raise HTTPException(status_code=400, detail="content is required")
    try:
        created = create_artifact(
            settings,
            user_id=user.id,
            chat_id=chat_id,
            format_name=format_name,
            filename=filename,
            content=content,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return created


@app.get("/assistant/artifacts/{artifact_id}")
def assistant_artifact_get(
    artifact_id: str,
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    cleanup_expired_assets(settings)
    artifact = get_artifact(settings, artifact_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found")
    if artifact["user_id"] != user.id and not _user_can_manage_foreign_assets(user):
        raise HTTPException(status_code=403, detail="Artifact is not accessible")
    if bool(artifact.get("expired")):
        raise HTTPException(status_code=410, detail="Artifact has expired")
    token = issue_artifact_download_token(settings, artifact_id)
    return {
        "id": artifact["id"],
        "format": artifact["format"],
        "filename": artifact["filename"],
        "size": artifact["size"],
        "sha256": artifact["sha256"],
        "chat_id": artifact.get("chat_id"),
        "expires_at": artifact["expires_at"],
        "download_url": f"/assistant/artifacts/{artifact_id}/download?token={token}",
    }


@app.get("/assistant/artifacts/{artifact_id}/download")
def assistant_artifact_download(
    artifact_id: str,
    token: str | None = Query(default=None),
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    cleanup_expired_assets(settings)
    artifact = get_artifact(settings, artifact_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found")
    if bool(artifact.get("expired")):
        raise HTTPException(status_code=410, detail="Artifact has expired")

    owner_or_admin = artifact["user_id"] == user.id or _user_can_manage_foreign_assets(user)
    token_ok = verify_artifact_token(settings, artifact_id, str(token or "")) if token else False
    if not owner_or_admin and not token_ok:
        raise HTTPException(status_code=403, detail="Artifact download is not allowed")

    path = Path(str(artifact["storage_path"]))
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Artifact file is missing")
    return FileResponse(path, filename=str(artifact["filename"]))


@app.delete("/assistant/artifacts/{artifact_id}")
def assistant_artifact_delete(
    artifact_id: str,
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    cleanup_expired_assets(settings)
    artifact = get_artifact(settings, artifact_id)
    if artifact is None:
        return {"ok": True}
    if artifact["user_id"] != user.id and not _user_can_manage_foreign_assets(user):
        raise HTTPException(status_code=403, detail="Artifact is not accessible")
    delete_artifact(settings, artifact_id)
    return {"ok": True}


@app.post("/files/write/preview")
def files_write_preview(
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    _require_files_source_enabled(settings, user)
    root_id = int((payload or {}).get("root_id") or 0)
    path = str((payload or {}).get("path") or "").strip()
    op = str((payload or {}).get("op") or "").strip().lower()
    new_path = (payload or {}).get("new_path")
    content = (payload or {}).get("content")
    if not root_id:
        raise HTTPException(status_code=400, detail="root_id is required")
    if not path:
        raise HTTPException(status_code=400, detail="path is required")

    rel = path.replace("\\", "/").strip("/")
    root = get_files_root(settings, root_id)
    if root is None:
        raise HTTPException(status_code=404, detail="root not found")

    if op in {"update", "move", "delete"}:
        _require_document_access(settings, user, files_document_id(root_id, rel))
    elif op == "create":
        parent = str(Path(rel).parent).replace("\\", "/")
        if parent == ".":
            parent = ""
        _require_document_access(settings, user, files_folder_document_id(root_id, parent))

    try:
        return preview_write_operation(
            settings,
            user_id=user.id,
            op=op,
            root_id=root_id,
            path=path,
            content=None if content is None else str(content),
            new_path=None if new_path is None else str(new_path),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/files/write/apply")
def files_write_apply(
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    _require_files_source_enabled(settings, user)
    audit_id = str((payload or {}).get("audit_id") or "").strip()
    confirm = bool((payload or {}).get("confirm", False))
    if not audit_id:
        raise HTTPException(status_code=400, detail="audit_id is required")
    if not confirm:
        raise HTTPException(status_code=400, detail="confirm=true is required")
    try:
        return apply_write_operation(settings, audit_id=audit_id, user_id=user.id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/files/create")
def files_create(
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    staged = files_write_preview(
        {
            "op": "create",
            "root_id": (payload or {}).get("root_id"),
            "path": (payload or {}).get("path"),
            "content": (payload or {}).get("content", ""),
        },
        settings,
        user,
    )
    return files_write_apply({"audit_id": staged["audit_id"], "confirm": True}, settings, user)


@app.post("/files/move")
def files_move(
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    staged = files_write_preview(
        {
            "op": "move",
            "root_id": (payload or {}).get("root_id"),
            "path": (payload or {}).get("path"),
            "new_path": (payload or {}).get("new_path"),
        },
        settings,
        user,
    )
    return files_write_apply({"audit_id": staged["audit_id"], "confirm": True}, settings, user)


@app.post("/files/delete")
def files_delete(
    payload: dict[str, Any],
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    staged = files_write_preview(
        {
            "op": "delete",
            "root_id": (payload or {}).get("root_id"),
            "path": (payload or {}).get("path"),
        },
        settings,
        user,
    )
    return files_write_apply({"audit_id": staged["audit_id"], "confirm": True}, settings, user)


@app.get("/files/audit")
def files_audit_get(
    limit: int = Query(default=200, ge=1, le=1000),
    settings: Settings = Depends(get_settings),
    user: AuthUser = Depends(get_current_user),
):
    if _user_can_manage_foreign_assets(user):
        items = list_files_audit(settings, limit=limit)
    else:
        items = list_files_audit(settings, limit=limit, user_id=user.id)
    return {"items": items}


@app.post("/sync/full", response_model=SyncResponse, dependencies=[Depends(require_admin_role)])
def sync_full(settings: Settings = Depends(get_settings)):
    if sync_jobs.is_running():
        raise HTTPException(status_code=409, detail="background sync is running")
    return run_full_sync(settings)


@app.post("/sync/start", response_model=SyncStartResponse, dependencies=[Depends(require_admin_role)])
def sync_start(settings: Settings = Depends(get_settings)):
    try:
        state = sync_jobs.start(settings)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"job_id": state["job_id"], "status": state["status"]}


@app.get("/sync/status", response_model=SyncStatusResponse, dependencies=[Depends(require_admin_role)])
def sync_status():
    return sync_jobs.status()


@app.post("/tasks/summarize", response_model=TextTaskResponse)
def summarize(
    payload: TextTaskRequest,
    settings: Settings = Depends(get_settings),
    _user: AuthUser = Depends(get_current_user),
):
    gateway = LLMGateway(settings)
    messages = [
        {"role": "system", "content": "You summarize technical text into clear bullet points."},
        {
            "role": "user",
            "content": f"Summarize the following text in 5-8 concise bullets:\n\n{payload.text}",
        },
    ]
    try:
        output, provider = gateway.generate(messages=messages, requested_provider=payload.provider)
        return {"provider": provider, "output": output}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/tasks/rewrite", response_model=TextTaskResponse)
def rewrite(
    payload: RewriteRequest,
    settings: Settings = Depends(get_settings),
    _user: AuthUser = Depends(get_current_user),
):
    gateway = LLMGateway(settings)
    messages = [
        {"role": "system", "content": "You rewrite technical text while preserving meaning."},
        {
            "role": "user",
            "content": (
                f"Rewrite the text in this style: {payload.style}. "
                "Keep all important technical details.\n\n"
                f"Text:\n{payload.text}"
            ),
        },
    ]
    try:
        output, provider = gateway.generate(messages=messages, requested_provider=payload.provider)
        return {"provider": provider, "output": output}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/tasks/translate", response_model=TextTaskResponse)
def translate(
    payload: TranslateRequest,
    settings: Settings = Depends(get_settings),
    _user: AuthUser = Depends(get_current_user),
):
    gateway = LLMGateway(settings)
    messages = [
        {
            "role": "system",
            "content": "You are a precise technical translator and preserve original meaning.",
        },
        {
            "role": "user",
            "content": (
                f"Translate the text into {payload.target_language}. "
                "Keep technical names and code blocks intact.\n\n"
                f"Text:\n{payload.text}"
            ),
        },
    ]
    try:
        output, provider = gateway.generate(messages=messages, requested_provider=payload.provider)
        return {"provider": provider, "output": output}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
