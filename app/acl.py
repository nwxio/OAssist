from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from app.auth import AuthUser, get_acl_global_defaults, get_acl_user_overrides, get_global_source_settings
from app.config import Settings
from app.document_ids import (
    SOURCE_FILES,
    SOURCE_NOTION,
    SOURCE_OUTLINE,
    SOURCE_MODE_BOTH,
    enabled_sources_from_mode,
    make_document_id,
    split_document_id,
)
from app.files_source import get_files_tree
from app.notion_client import NotionClient
from app.outline_client import OutlineClient

SUPER_ADMIN_USERNAME = "admin"


def is_super_admin(user: AuthUser) -> bool:
    return str(user.username).strip().lower() == SUPER_ADMIN_USERNAME


def globally_enabled_sources(settings: Settings) -> set[str]:
    data = get_global_source_settings(settings)
    enabled: set[str] = set()
    if bool(data.get(SOURCE_OUTLINE, True)):
        enabled.add(SOURCE_OUTLINE)
    if bool(data.get(SOURCE_NOTION, True)):
        enabled.add(SOURCE_NOTION)
    if bool(data.get(SOURCE_FILES, True)):
        enabled.add(SOURCE_FILES)
    return enabled


def user_enabled_sources(user: AuthUser, globally_enabled: set[str] | None = None) -> set[str]:
    if is_super_admin(user):
        allowed = {SOURCE_OUTLINE, SOURCE_NOTION, SOURCE_FILES}
    else:
        allowed = enabled_sources_from_mode(getattr(user, "source_mode", SOURCE_OUTLINE))
        allowed.add(SOURCE_FILES)
    if globally_enabled is not None:
        return {source for source in allowed if source in globally_enabled}
    return allowed


def _normalize_outline_doc_item(item: dict[str, Any], base_url: str) -> dict[str, Any] | None:
    native_id = str(item.get("id") or "").strip()
    if not native_id:
        return None
    title = str(item.get("title") or "Untitled").strip() or "Untitled"
    parent_native_id = str(item.get("parentDocumentId") or "").strip() or None
    url = str(item.get("url") or "").strip()
    if url.startswith("/"):
        url = f"{base_url.rstrip('/')}{url}"
    return {
        "id": make_document_id(SOURCE_OUTLINE, native_id),
        "source": SOURCE_OUTLINE,
        "title": title,
        "url": url,
        "parentDocumentId": make_document_id(SOURCE_OUTLINE, parent_native_id) if parent_native_id else None,
        "children": [],
    }


def _build_tree_result(rows: list[dict[str, Any]]) -> dict[str, Any]:
    docs_by_id: dict[str, dict[str, Any]] = {}
    children_map: dict[str, list[str]] = defaultdict(list)
    parent_by_id: dict[str, str | None] = {}

    for node in rows:
        doc_id = str(node.get("id") or "").strip()
        if not doc_id:
            continue
        docs_by_id[doc_id] = dict(node)
        docs_by_id[doc_id]["children"] = []
        parent_id = str(node.get("parentDocumentId") or "").strip() or None
        parent_by_id[doc_id] = parent_id
        if parent_id:
            children_map[parent_id].append(doc_id)

    roots: list[dict[str, Any]] = []
    for doc_id, node in docs_by_id.items():
        parent_id = parent_by_id.get(doc_id)
        if parent_id and parent_id in docs_by_id:
            docs_by_id[parent_id]["children"].append(node)
        else:
            roots.append(node)

    def sort_nodes(nodes: list[dict[str, Any]]) -> None:
        nodes.sort(key=lambda row: str(row.get("title") or "").lower())
        for row in nodes:
            kids = row.get("children")
            if isinstance(kids, list) and kids:
                sort_nodes(kids)

    sort_nodes(roots)
    return {
        "nodes": roots,
        "count": len(docs_by_id),
        "all_ids": set(docs_by_id.keys()),
        "children_map": {key: list(values) for key, values in children_map.items()},
    }


def get_outline_tree(settings: Settings) -> dict[str, Any]:
    client = OutlineClient(settings)
    nodes: list[dict[str, Any]] = []
    for raw in client.iter_documents():
        node = _normalize_outline_doc_item(raw, settings.outline_base_url)
        if node:
            nodes.append(node)
    return _build_tree_result(nodes)


def get_notion_tree(settings: Settings, access_token: str) -> dict[str, Any]:
    client = NotionClient(settings, access_token)
    return client.build_tree()


def get_source_tree(settings: Settings, source: str, access_token: str | None = None) -> dict[str, Any]:
    src = str(source or "").strip().lower()
    if src == SOURCE_OUTLINE:
        return get_outline_tree(settings)
    if src == SOURCE_NOTION:
        token = str(access_token or "").strip()
        if not token:
            raise ValueError("Notion access token is required")
        return get_notion_tree(settings, token)
    if src == SOURCE_FILES:
        return get_files_tree(settings)
    raise ValueError("unsupported source")


def merge_children_maps(*maps: dict[str, list[str]]) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = defaultdict(list)
    for mapping in maps:
        for parent_id, child_ids in (mapping or {}).items():
            seen = set(merged[parent_id])
            for child_id in child_ids:
                if child_id not in seen:
                    merged[parent_id].append(child_id)
                    seen.add(child_id)
    return {key: values for key, values in merged.items()}


def _expand_with_descendants(selected_ids: set[str], children_map: dict[str, list[str]]) -> set[str]:
    expanded: set[str] = set()
    queue: deque[str] = deque(selected_ids)
    while queue:
        current = queue.popleft()
        if current in expanded:
            continue
        expanded.add(current)
        for child_id in children_map.get(current, []):
            if child_id not in expanded:
                queue.append(child_id)
    return expanded


def expand_document_ids_with_descendants(selected_ids: set[str], children_map: dict[str, list[str]]) -> set[str]:
    return _expand_with_descendants(selected_ids, children_map)


def filter_document_ids_by_sources(document_ids: set[str], allowed_sources: set[str]) -> set[str]:
    if not document_ids:
        return set()
    result: set[str] = set()
    for raw in document_ids:
        parsed = split_document_id(raw)
        if not parsed:
            continue
        source, native_id = parsed
        if source in allowed_sources:
            result.add(make_document_id(source, native_id))
    return result


def get_effective_allowed_document_ids(settings: Settings, user: AuthUser) -> set[str] | None:
    allowed_sources = user_enabled_sources(user, globally_enabled_sources(settings))
    if not allowed_sources:
        return set()

    if is_super_admin(user):
        return None

    global_defaults = filter_document_ids_by_sources(get_acl_global_defaults(settings), allowed_sources)
    user_overrides = filter_document_ids_by_sources(get_acl_user_overrides(settings, user.id), allowed_sources)

    if not user_overrides:
        return set(global_defaults)
    return set(user_overrides)


def effective_source_mode_for_user(user: AuthUser) -> str:
    if is_super_admin(user):
        return SOURCE_MODE_BOTH
    return str(getattr(user, "source_mode", SOURCE_OUTLINE) or SOURCE_OUTLINE).strip().lower()
