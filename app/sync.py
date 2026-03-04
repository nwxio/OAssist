import hashlib
import uuid
from collections.abc import Callable
from time import monotonic
from typing import Any

from qdrant_client import models

from app.config import Settings
from app.document_ids import SOURCE_FILES, SOURCE_NOTION, SOURCE_OUTLINE, make_document_id, split_document_id
from app.embeddings import EmbeddingClient
from app.files_source import collect_files_documents_for_sync
from app.notion_client import NotionClient
from app.outline_client import OutlineClient
from app.vector_store import VectorStore


def chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    cleaned = "\n".join(part.strip() for part in text.splitlines() if part.strip())
    if not cleaned:
        return []
    chunks: list[str] = []
    start = 0
    step = max(1, chunk_size - chunk_overlap)
    while start < len(cleaned):
        end = min(len(cleaned), start + chunk_size)
        chunks.append(cleaned[start:end])
        if end == len(cleaned):
            break
        start += step
    return chunks


def build_document_url(base_url: str, doc: dict[str, Any]) -> str:
    url = str(doc.get("url") or "").strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return f"{base_url.rstrip('/')}{url}"
    doc_id = doc.get("id")
    return f"{base_url.rstrip('/')}/doc/{doc_id}"


def normalize_outline_document(base_url: str, raw: dict[str, Any]) -> dict[str, str] | None:
    doc_id = str(raw.get("id") or "").strip()
    text = str(raw.get("text") or raw.get("content") or "").strip()
    if not doc_id or not text:
        return None
    return {
        "id": make_document_id(SOURCE_OUTLINE, doc_id),
        "source": SOURCE_OUTLINE,
        "title": str(raw.get("title") or "Untitled"),
        "text": text,
        "url": build_document_url(base_url, raw),
    }


def normalize_notion_document(raw: dict[str, Any]) -> dict[str, str] | None:
    doc_id = str(raw.get("id") or "").strip()
    text = str(raw.get("text") or raw.get("content") or "").strip()
    if not doc_id or not text:
        return None
    return {
        "id": doc_id,
        "source": SOURCE_NOTION,
        "title": str(raw.get("title") or "Untitled"),
        "text": text,
        "url": str(raw.get("url") or "").strip(),
    }


def collect_sync_documents(settings: Settings) -> tuple[list[dict[str, str]], list[str]]:
    documents: list[dict[str, str]] = []
    stale_document_ids: list[str] = []

    outline_token = str(settings.outline_api_token or "").strip()
    if outline_token:
        try:
            outline = OutlineClient(settings)
            for item in outline.iter_documents():
                normalized = normalize_outline_document(settings.outline_base_url, item)
                if normalized:
                    documents.append(normalized)
        except Exception:
            pass

    notion_token = str(settings.notion_api_token or "").strip()
    if notion_token:
        try:
            notion = NotionClient(settings, notion_token)
            for item in notion.iter_documents():
                normalized = normalize_notion_document(item)
                if normalized:
                    documents.append(normalized)
        except Exception:
            pass

    try:
        file_docs, stale_ids = collect_files_documents_for_sync(settings)
        documents.extend(file_docs)
        stale_document_ids.extend(stale_ids)
    except Exception:
        pass

    return documents, stale_document_ids


def run_full_sync(
    settings: Settings,
    progress_callback: Callable[[dict[str, int | float]], None] | None = None,
) -> dict[str, int | float]:
    outline_token = str(settings.outline_api_token or "").strip()

    started = monotonic()
    embeddings = EmbeddingClient(settings)
    store = VectorStore(settings)

    documents, stale_document_ids = collect_sync_documents(settings)
    total_documents = len(documents)

    if outline_token:
        try:
            store.delete_legacy_document_id_points()
        except Exception:
            pass

    for stale_doc_id in stale_document_ids:
        try:
            if (split_document_id(stale_doc_id) or (None, None))[0] == SOURCE_FILES:
                store.delete_document(stale_doc_id)
        except Exception:
            pass

    indexed_documents = 0
    indexed_chunks = 0
    processed_documents = 0
    failed_documents = 0
    collection_ready = False

    if progress_callback:
        progress_callback(
            {
                "total_documents": total_documents,
                "processed_documents": processed_documents,
                "indexed_documents": indexed_documents,
                "indexed_chunks": indexed_chunks,
                "failed_documents": failed_documents,
                "progress_percent": 0.0,
            }
        )

    for normalized in documents:
        doc_id = str(normalized.get("id") or "").strip()
        if not doc_id:
            processed_documents += 1
            continue
        try:
            chunks = chunk_text(normalized["text"], settings.chunk_size, settings.chunk_overlap)
            if not chunks:
                processed_documents += 1
                continue

            embedding_inputs = [f"{normalized['title']}\n\n{chunk}" for chunk in chunks]
            vectors = embeddings.embed_texts(embedding_inputs)
            if not vectors:
                processed_documents += 1
                continue

            if not collection_ready:
                store.ensure_collection(len(vectors[0]))
                collection_ready = True

            store.delete_document(normalized["id"])
            parsed = split_document_id(doc_id)
            if parsed is not None:
                source, native_id = parsed
                if source == SOURCE_OUTLINE and native_id:
                    store.delete_document(native_id)

            points: list[models.PointStruct] = []
            for index, (chunk, vector) in enumerate(zip(chunks, vectors)):
                digest = hashlib.sha1(chunk.encode("utf-8")).hexdigest()
                point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{normalized['id']}:{index}:{digest}"))
                points.append(
                    models.PointStruct(
                        id=point_id,
                        vector=vector,
                        payload={
                            "document_id": normalized["id"],
                            "source": normalized.get("source") or SOURCE_OUTLINE,
                            "title": normalized["title"],
                            "url": normalized["url"],
                            "chunk_index": index,
                            "text": chunk,
                        },
                    )
                )

            store.upsert(points)
            indexed_documents += 1
            indexed_chunks += len(points)
        except Exception:
            failed_documents += 1
        finally:
            processed_documents += 1
            if progress_callback:
                percent = (processed_documents / total_documents * 100.0) if total_documents else 100.0
                progress_callback(
                    {
                        "total_documents": total_documents,
                        "processed_documents": processed_documents,
                        "indexed_documents": indexed_documents,
                        "indexed_chunks": indexed_chunks,
                        "failed_documents": failed_documents,
                        "progress_percent": round(percent, 2),
                    }
                )

    return {
        "indexed_documents": indexed_documents,
        "indexed_chunks": indexed_chunks,
        "total_documents": total_documents,
        "processed_documents": processed_documents,
        "failed_documents": failed_documents,
        "duration_seconds": round(monotonic() - started, 2),
    }
