from typing import Any

import httpx

from app.config import Settings


class EmbeddingError(RuntimeError):
    pass


class EmbeddingClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.timeout = settings.request_timeout_seconds

    def embed_texts(self, texts: list[str], provider: str | None = None) -> list[list[float]]:
        chosen = (provider or self.settings.embedding_provider).lower()
        if chosen == "ollama":
            return self._embed_ollama_many(texts)
        if chosen == "openai":
            return self._embed_openai(texts)
        raise EmbeddingError(f"Unsupported embedding provider: {chosen}")

    def embed_query(self, text: str, provider: str | None = None) -> list[float]:
        return self.embed_texts([text], provider=provider)[0]

    def _embed_ollama_many(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        base = self.settings.ollama_base_url.rstrip("/")
        url = f"{base}/api/embed"
        payload = {"model": self.settings.ollama_embed_model, "input": texts}
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(url, json=payload)
            if response.status_code == 404:
                try:
                    error_text = str(response.json().get("error", "")).strip()
                except Exception:
                    error_text = ""
                if error_text:
                    raise EmbeddingError(error_text)
                vectors: list[list[float]] = []
                for text in texts:
                    legacy = client.post(
                        f"{base}/api/embeddings",
                        json={"model": self.settings.ollama_embed_model, "prompt": text},
                    )
                    legacy.raise_for_status()
                    data = legacy.json()
                    embedding = data.get("embedding")
                    if not isinstance(embedding, list):
                        raise EmbeddingError("Ollama legacy embeddings response missing vector")
                    vectors.append(embedding)
                return vectors
            response.raise_for_status()
            data = response.json()
        embeddings = data.get("embeddings")
        if isinstance(embeddings, list) and len(embeddings) == len(texts) and all(isinstance(item, list) for item in embeddings):
            return embeddings
        embedding = data.get("embedding")
        if isinstance(embedding, list) and len(texts) == 1:
            return [embedding]
        raise EmbeddingError("Ollama embedding response missing vector")

    def _embed_openai(self, texts: list[str]) -> list[list[float]]:
        if not self.settings.openai_api_key:
            raise EmbeddingError("OPENAI_API_KEY is not set")
        url = f"{self.settings.openai_base_url.rstrip('/')}/embeddings"
        payload: dict[str, Any] = {
            "model": self.settings.openai_embedding_model,
            "input": texts,
        }
        headers = {
            "Authorization": f"Bearer {self.settings.openai_api_key}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
        rows = data.get("data", [])
        vectors: list[list[float]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            embedding = row.get("embedding")
            if isinstance(embedding, list):
                vectors.append(embedding)
        if len(vectors) != len(texts):
            raise EmbeddingError("OpenAI embeddings response length mismatch")
        return vectors
