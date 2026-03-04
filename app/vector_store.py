from typing import Any

from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse

from app.config import Settings
from app.document_ids import SOURCE_OUTLINE, normalize_document_id, split_document_id


class VectorStore:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = QdrantClient(url=settings.qdrant_url, api_key=settings.qdrant_api_key or None)

    def ensure_collection(self, vector_size: int) -> None:
        existing = {collection.name for collection in self.client.get_collections().collections}
        if self.settings.qdrant_collection in existing:
            return
        try:
            self.client.create_collection(
                collection_name=self.settings.qdrant_collection,
                vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE),
            )
        except UnexpectedResponse as exc:
            if exc.status_code == 409:
                return
            raise

    def upsert(self, points: list[models.PointStruct]) -> None:
        if not points:
            return
        self.client.upsert(collection_name=self.settings.qdrant_collection, points=points)

    def delete_document(self, document_id: str) -> None:
        self.client.delete(
            collection_name=self.settings.qdrant_collection,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="document_id",
                            match=models.MatchValue(value=document_id),
                        )
                    ]
                )
            ),
        )

    def delete_legacy_document_id_points(self) -> int:
        deleted = 0
        offset: Any = None
        while True:
            points, next_offset = self.client.scroll(
                collection_name=self.settings.qdrant_collection,
                offset=offset,
                limit=256,
                with_payload=True,
                with_vectors=False,
            )
            legacy_ids: list[Any] = []
            for point in points:
                payload = point.payload or {}
                document_id = str(payload.get("document_id") or "").strip()
                if not document_id:
                    legacy_ids.append(point.id)
                    continue
                normalized = normalize_document_id(document_id)
                if not normalized or normalized != document_id:
                    legacy_ids.append(point.id)

            if legacy_ids:
                self.client.delete(
                    collection_name=self.settings.qdrant_collection,
                    points_selector=models.PointIdsList(points=legacy_ids),
                )
                deleted += len(legacy_ids)

            if next_offset is None:
                break
            offset = next_offset
        return deleted

    def search(
        self,
        query_vector: list[float],
        limit: int,
        allowed_document_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        if allowed_document_ids is not None and not allowed_document_ids:
            return []

        query_filter = None
        if allowed_document_ids is not None:
            expanded_allowed_ids: set[str] = set()
            for raw in allowed_document_ids:
                doc_id = str(raw or "").strip()
                if not doc_id:
                    continue
                expanded_allowed_ids.add(doc_id)
                parsed = split_document_id(doc_id)
                if parsed is None:
                    continue
                source, native_id = parsed
                if source == SOURCE_OUTLINE and native_id:
                    expanded_allowed_ids.add(native_id)

            query_filter = models.Filter(
                should=[
                    models.FieldCondition(
                        key="document_id",
                        match=models.MatchValue(value=doc_id),
                    )
                    for doc_id in sorted(expanded_allowed_ids)
                ]
            )

        hits = self.client.search(
            collection_name=self.settings.qdrant_collection,
            query_vector=query_vector,
            limit=limit,
            with_payload=True,
            query_filter=query_filter,
        )
        rows: list[dict[str, Any]] = []
        for hit in hits:
            payload = hit.payload or {}
            rows.append(
                {
                    "score": float(hit.score),
                    "document_id": str(payload.get("document_id", "")),
                    "title": str(payload.get("title", "Untitled")),
                    "url": str(payload.get("url", "")),
                    "text": str(payload.get("text", "")),
                }
            )
        return rows
