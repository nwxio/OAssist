from collections.abc import Generator
from typing import Any

import httpx

from app.config import Settings


class OutlineClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.base_url = settings.outline_base_url.rstrip("/")
        self.timeout = settings.request_timeout_seconds

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        token = self.settings.outline_api_token.strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _post(self, endpoint: str, payload: dict[str, Any]) -> Any:
        url = f"{self.base_url}/api/{endpoint}"
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(url, json=payload, headers=self._headers())
            response.raise_for_status()
            body = response.json()
        if isinstance(body, dict) and "data" in body:
            return body["data"]
        return body

    def iter_documents(self) -> Generator[dict[str, Any], None, None]:
        offset = 0
        page_size = self.settings.sync_page_size
        while True:
            payload = {
                "offset": offset,
                "limit": page_size,
                "sort": "updatedAt",
                "direction": "DESC",
            }
            items = self._post("documents.list", payload)
            if not isinstance(items, list) or not items:
                break
            for item in items:
                if isinstance(item, dict):
                    yield item
            if len(items) < page_size:
                break
            offset += page_size

    def get_document(self, document_id: str) -> dict[str, Any]:
        data = self._post("documents.info", {"id": document_id})
        return data if isinstance(data, dict) else {}

    def search_documents(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        payload = {
            "query": query,
            "limit": max(1, min(limit, 100)),
            "offset": 0,
        }
        data = self._post("documents.search", payload)
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        return []
