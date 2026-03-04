from __future__ import annotations

from collections import defaultdict
from collections.abc import Generator
from typing import Any

import httpx

from app.config import Settings
from app.document_ids import SOURCE_NOTION, make_document_id


class NotionClient:
    def __init__(self, settings: Settings, access_token: str):
        self.settings = settings
        self.base_url = settings.notion_api_base_url.rstrip("/")
        self.timeout = settings.request_timeout_seconds
        self.access_token = str(access_token or "").strip()
        if not self.access_token:
            raise ValueError("Notion access token is required")

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Notion-Version": self.settings.notion_api_version,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, *, json_payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        with httpx.Client(timeout=self.timeout) as client:
            response = client.request(method=method.upper(), url=url, headers=self._headers(), json=json_payload)
            response.raise_for_status()
            body = response.json()
        return body if isinstance(body, dict) else {}

    @staticmethod
    def _rich_text_plain(items: Any) -> str:
        if not isinstance(items, list):
            return ""
        parts: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            text = str(item.get("plain_text") or "").strip()
            if text:
                parts.append(text)
        return "".join(parts).strip()

    def _title_from_page(self, page: dict[str, Any]) -> str:
        properties = page.get("properties")
        if isinstance(properties, dict):
            for prop in properties.values():
                if not isinstance(prop, dict):
                    continue
                if prop.get("type") != "title":
                    continue
                title = self._rich_text_plain(prop.get("title"))
                if title:
                    return title
        return "Untitled"

    def _title_from_database(self, database: dict[str, Any]) -> str:
        title = self._rich_text_plain(database.get("title"))
        return title or "Untitled"

    @staticmethod
    def _parent_native_id(obj: dict[str, Any]) -> str | None:
        parent = obj.get("parent")
        if not isinstance(parent, dict):
            return None
        for key in ("page_id", "database_id"):
            value = str(parent.get(key) or "").strip()
            if value:
                return value
        return None

    def _search(self, *, object_filter: str | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            payload: dict[str, Any] = {"page_size": 100}
            if object_filter:
                payload["filter"] = {"property": "object", "value": object_filter}
            if cursor:
                payload["start_cursor"] = cursor

            body = self._request("POST", "/search", json_payload=payload)
            items = body.get("results")
            if isinstance(items, list):
                rows.extend(item for item in items if isinstance(item, dict))

            if not body.get("has_more"):
                break
            cursor = str(body.get("next_cursor") or "").strip() or None
            if not cursor:
                break

        return rows

    def _iter_block_children(self, block_id: str) -> Generator[dict[str, Any], None, None]:
        cursor: str | None = None
        while True:
            path = f"/blocks/{block_id}/children?page_size=100"
            if cursor:
                path += f"&start_cursor={cursor}"
            body = self._request("GET", path)
            results = body.get("results")
            if isinstance(results, list):
                for block in results:
                    if isinstance(block, dict):
                        yield block
            if not body.get("has_more"):
                break
            cursor = str(body.get("next_cursor") or "").strip() or None
            if not cursor:
                break

    def _block_text(self, block: dict[str, Any]) -> str:
        block_type = str(block.get("type") or "").strip()
        if not block_type:
            return ""
        content = block.get(block_type)
        if not isinstance(content, dict):
            return ""
        rich_text = content.get("rich_text")
        return self._rich_text_plain(rich_text)

    def _page_text(self, page_id: str, depth: int = 0, max_depth: int = 8) -> str:
        lines: list[str] = []
        for block in self._iter_block_children(page_id):
            text = self._block_text(block)
            if text:
                lines.append(text)
            has_children = bool(block.get("has_children"))
            child_id = str(block.get("id") or "").strip()
            if has_children and child_id and depth < max_depth:
                nested = self._page_text(child_id, depth + 1, max_depth=max_depth)
                if nested:
                    lines.append(nested)
        return "\n".join(lines).strip()

    def build_tree(self) -> dict[str, Any]:
        objects = self._search(object_filter=None)
        docs_by_id: dict[str, dict[str, Any]] = {}
        children_map: dict[str, list[str]] = defaultdict(list)
        parent_by_id: dict[str, str | None] = {}

        for obj in objects:
            obj_type = str(obj.get("object") or "").strip()
            if obj_type not in {"page", "database"}:
                continue

            native_id = str(obj.get("id") or "").strip()
            if not native_id:
                continue

            doc_id = make_document_id(SOURCE_NOTION, native_id)
            parent_native = self._parent_native_id(obj)
            parent_id = make_document_id(SOURCE_NOTION, parent_native) if parent_native else None
            title = self._title_from_page(obj) if obj_type == "page" else self._title_from_database(obj)

            node = {
                "id": doc_id,
                "source": SOURCE_NOTION,
                "title": title,
                "url": str(obj.get("url") or ""),
                "parentDocumentId": parent_id,
                "type": obj_type,
                "children": [],
            }
            docs_by_id[doc_id] = node
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
            nodes.sort(key=lambda item: str(item.get("title") or "").lower())
            for item in nodes:
                kids = item.get("children")
                if isinstance(kids, list) and kids:
                    sort_nodes(kids)

        sort_nodes(roots)
        return {
            "nodes": roots,
            "count": len(docs_by_id),
            "all_ids": set(docs_by_id.keys()),
            "children_map": {key: list(values) for key, values in children_map.items()},
        }

    def iter_documents(self) -> Generator[dict[str, Any], None, None]:
        pages = self._search(object_filter="page")
        for page in pages:
            native_id = str(page.get("id") or "").strip()
            if not native_id:
                continue
            doc_id = make_document_id(SOURCE_NOTION, native_id)
            title = self._title_from_page(page)
            text = self._page_text(native_id)
            if not text:
                continue
            yield {
                "id": doc_id,
                "title": title,
                "text": text,
                "url": str(page.get("url") or ""),
                "source": SOURCE_NOTION,
            }
