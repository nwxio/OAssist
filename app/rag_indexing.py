from __future__ import annotations

import hashlib
import uuid
from typing import Any

from qdrant_client import models

from app.config import Settings
from app.document_ids import SOURCE_OUTLINE, split_document_id
from app.embeddings import EmbeddingClient
from app.vector_store import VectorStore


def chunk_text_for_rag(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    cleaned = "\n".join(part.strip() for part in str(text or "").splitlines() if part.strip())
    if not cleaned:
        return []

    chunks: list[str] = []
    start = 0
    step = max(1, int(chunk_size) - int(chunk_overlap))
    while start < len(cleaned):
        end = min(len(cleaned), start + int(chunk_size))
        chunks.append(cleaned[start:end])
        if end == len(cleaned):
            break
        start += step
    return chunks


def upsert_document_to_rag(settings: Settings, normalized: dict[str, Any]) -> dict[str, int]:
    doc_id = str(normalized.get("id") or "").strip()
    text = str(normalized.get("text") or "")
    title = str(normalized.get("title") or "Untitled")
    url = str(normalized.get("url") or "")
    source = str(normalized.get("source") or "").strip() or "outline"

    if not doc_id:
        return {"indexed_documents": 0, "indexed_chunks": 0}

    chunks = chunk_text_for_rag(text, settings.chunk_size, settings.chunk_overlap)
    if not chunks:
        return {"indexed_documents": 0, "indexed_chunks": 0}

    embeddings = EmbeddingClient(settings)
    vectors = embeddings.embed_texts([f"{title}\n\n{chunk}" for chunk in chunks])
    if not vectors:
        return {"indexed_documents": 0, "indexed_chunks": 0}

    store = VectorStore(settings)
    store.ensure_collection(len(vectors[0]))

    store.delete_document(doc_id)
    parsed = split_document_id(doc_id)
    if parsed is not None:
        parsed_source, native_id = parsed
        if parsed_source == SOURCE_OUTLINE and native_id:
            store.delete_document(native_id)

    points: list[models.PointStruct] = []
    for index, (chunk, vector) in enumerate(zip(chunks, vectors)):
        digest = hashlib.sha1(chunk.encode("utf-8")).hexdigest()
        point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}:{index}:{digest}"))
        points.append(
            models.PointStruct(
                id=point_id,
                vector=vector,
                payload={
                    "document_id": doc_id,
                    "source": source,
                    "title": title,
                    "url": url,
                    "chunk_index": index,
                    "text": chunk,
                },
            )
        )

    store.upsert(points)
    return {"indexed_documents": 1, "indexed_chunks": len(points)}


def delete_document_from_rag(settings: Settings, document_id: str) -> None:
    clean_id = str(document_id or "").strip()
    if not clean_id:
        return
    store = VectorStore(settings)
    store.delete_document(clean_id)
