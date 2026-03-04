import json
from collections.abc import Generator
from typing import Any

import httpx

from app.config import Settings


class LLMError(RuntimeError):
    pass


class LLMGateway:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.timeout = settings.request_timeout_seconds

    def provider_health(self) -> dict[str, dict[str, str]]:
        return {
            "ollama": self._health_ollama(),
            "openai": self._health_openai(),
            "deepseek": self._health_deepseek(),
        }

    def generate(
        self,
        messages: list[dict[str, str]],
        requested_provider: str = "auto",
        requested_model: str | None = None,
    ) -> tuple[str, str]:
        providers = self._resolve_order(requested_provider)
        errors: list[str] = []
        for provider in providers:
            try:
                if provider == "ollama":
                    return self._chat_ollama(messages, requested_model), provider
                if provider == "openai":
                    return self._chat_openai(messages), provider
                if provider == "deepseek":
                    return self._chat_deepseek(messages), provider
            except Exception as exc:
                errors.append(f"{provider}: {exc}")
        raise LLMError("All providers failed: " + " | ".join(errors))

    def stream_generate(
        self,
        messages: list[dict[str, str]],
        requested_provider: str = "auto",
        requested_model: str | None = None,
    ) -> Generator[dict[str, str], None, None]:
        providers = self._resolve_order(requested_provider)
        errors: list[str] = []
        for provider in providers:
            try:
                if provider == "ollama":
                    yield {"type": "provider", "provider": provider}
                    for chunk in self._stream_ollama(messages, requested_model):
                        if chunk:
                            yield {"type": "chunk", "content": chunk}
                    yield {"type": "done", "provider": provider}
                    return

                if provider == "openai":
                    yield {"type": "provider", "provider": provider}
                    for chunk in self._stream_openai_compatible(
                        base_url=self.settings.openai_base_url,
                        api_key=self.settings.openai_api_key,
                        model=self.settings.openai_model,
                        messages=messages,
                    ):
                        if chunk:
                            yield {"type": "chunk", "content": chunk}
                    yield {"type": "done", "provider": provider}
                    return

                if provider == "deepseek":
                    yield {"type": "provider", "provider": provider}
                    for chunk in self._stream_openai_compatible(
                        base_url=self.settings.deepseek_base_url,
                        api_key=self.settings.deepseek_api_key,
                        model=self.settings.deepseek_model,
                        messages=messages,
                    ):
                        if chunk:
                            yield {"type": "chunk", "content": chunk}
                    yield {"type": "done", "provider": provider}
                    return
            except Exception as exc:
                errors.append(f"{provider}: {exc}")
        raise LLMError("All providers failed: " + " | ".join(errors))

    def _resolve_order(self, requested_provider: str) -> list[str]:
        req = (requested_provider or "auto").lower()
        if req == "auto":
            return self.settings.provider_order
        return [req]

    def list_ollama_models(self) -> list[str]:
        url = f"{self.settings.ollama_base_url.rstrip('/')}/api/tags"
        with httpx.Client(timeout=10) as client:
            response = client.get(url)
            response.raise_for_status()
            data = response.json()
        models = data.get("models") if isinstance(data, dict) else None
        names = [
            str(item.get("name"))
            for item in (models or [])
            if isinstance(item, dict) and item.get("name")
        ]
        return sorted(set(names), key=str.lower)

    def _chat_ollama(self, messages: list[dict[str, str]], requested_model: str | None = None) -> str:
        url = f"{self.settings.ollama_base_url.rstrip('/')}/api/chat"
        model_name = (requested_model or self.settings.ollama_model).strip()
        if not model_name:
            raise LLMError("Ollama model is not set")
        payload = {
            "model": model_name,
            "messages": messages,
            "stream": False,
            "options": {"temperature": self.settings.llm_temperature},
        }
        with httpx.Client(timeout=httpx.Timeout(connect=10.0, read=max(float(self.timeout), 180.0), write=60.0, pool=60.0)) as client:
            response = client.post(url, json=payload)
            if response.status_code >= 400:
                try:
                    detail = str(response.json().get("error", "")).strip()
                except Exception:
                    detail = response.text.strip()
                if detail:
                    raise LLMError(detail)
                response.raise_for_status()
            data = response.json()
        content = data.get("message", {}).get("content")
        if not content:
            raise LLMError("Ollama returned empty content")
        return content.strip()

    def _stream_ollama(self, messages: list[dict[str, str]], requested_model: str | None = None) -> Generator[str, None, None]:
        url = f"{self.settings.ollama_base_url.rstrip('/')}/api/chat"
        model_name = (requested_model or self.settings.ollama_model).strip()
        if not model_name:
            raise LLMError("Ollama model is not set")
        payload = {
            "model": model_name,
            "messages": messages,
            "stream": True,
            "options": {"temperature": self.settings.llm_temperature},
        }
        stream_timeout = httpx.Timeout(connect=10.0, read=None, write=60.0, pool=60.0)
        with httpx.Client(timeout=stream_timeout) as client:
            with client.stream("POST", url, json=payload) as response:
                if response.status_code >= 400:
                    try:
                        detail = str(response.json().get("error", "")).strip()
                    except Exception:
                        detail = response.text.strip()
                    raise LLMError(detail or f"Ollama HTTP {response.status_code}")

                for line in response.iter_lines():
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    content = data.get("message", {}).get("content")
                    if isinstance(content, str) and content:
                        yield content
                    if data.get("done"):
                        break

    def _chat_openai(self, messages: list[dict[str, str]]) -> str:
        if not self.settings.openai_api_key:
            raise LLMError("OPENAI_API_KEY is not set")
        return self._chat_openai_compatible(
            base_url=self.settings.openai_base_url,
            api_key=self.settings.openai_api_key,
            model=self.settings.openai_model,
            messages=messages,
        )

    def _chat_deepseek(self, messages: list[dict[str, str]]) -> str:
        if not self.settings.deepseek_api_key:
            raise LLMError("DEEPSEEK_API_KEY is not set")
        return self._chat_openai_compatible(
            base_url=self.settings.deepseek_base_url,
            api_key=self.settings.deepseek_api_key,
            model=self.settings.deepseek_model,
            messages=messages,
        )

    def _chat_openai_compatible(self, base_url: str, api_key: str, model: str, messages: list[dict[str, str]]) -> str:
        url = f"{base_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": self.settings.llm_temperature,
        }
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(url, headers=headers, json=payload)
            if response.status_code >= 400:
                try:
                    data = response.json()
                    detail = str(data.get("error", {}).get("message") or data.get("error") or "").strip()
                except Exception:
                    detail = response.text.strip()
                raise LLMError(detail or f"Provider HTTP {response.status_code}")
            data = response.json()
        choices = data.get("choices") or []
        if not choices:
            raise LLMError("Provider returned no choices")
        content = choices[0].get("message", {}).get("content")
        if not content:
            raise LLMError("Provider returned empty content")
        return content.strip()

    def _stream_openai_compatible(
        self,
        base_url: str,
        api_key: str,
        model: str,
        messages: list[dict[str, str]],
    ) -> Generator[str, None, None]:
        url = f"{base_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": self.settings.llm_temperature,
            "stream": True,
        }
        stream_timeout = httpx.Timeout(connect=10.0, read=None, write=60.0, pool=60.0)
        with httpx.Client(timeout=stream_timeout) as client:
            with client.stream("POST", url, headers=headers, json=payload) as response:
                if response.status_code >= 400:
                    try:
                        data = response.json()
                        detail = str(data.get("error", {}).get("message") or data.get("error") or "").strip()
                    except Exception:
                        detail = response.text.strip()
                    raise LLMError(detail or f"Provider HTTP {response.status_code}")

                for line in response.iter_lines():
                    if not line:
                        continue
                    if not line.startswith("data: "):
                        continue
                    data_str = line[6:]
                    if data_str.strip() == "[DONE]":
                        break
                    try:
                        data = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue
                    choices = data.get("choices") or []
                    if not choices:
                        continue
                    delta = choices[0].get("delta") or {}
                    content = delta.get("content")
                    if isinstance(content, str) and content:
                        yield content

    def _health_ollama(self) -> dict[str, str]:
        try:
            url = f"{self.settings.ollama_base_url.rstrip('/')}/api/tags"
            with httpx.Client(timeout=10) as client:
                response = client.get(url)
                response.raise_for_status()
                data = response.json()
            models = data.get("models") if isinstance(data, dict) else None
            names = {
                str(item.get("name"))
                for item in (models or [])
                if isinstance(item, dict) and item.get("name")
            }
            if not self._ollama_model_present(self.settings.ollama_model, names):
                return {
                    "status": "warning",
                    "detail": f"model '{self.settings.ollama_model}' is not pulled",
                }
            if not self._ollama_model_present(self.settings.ollama_embed_model, names):
                return {
                    "status": "warning",
                    "detail": f"embed model '{self.settings.ollama_embed_model}' is not pulled",
                }
            return {"status": "ok"}
        except Exception as exc:
            return {"status": "error", "detail": str(exc)}

    @staticmethod
    def _ollama_model_present(model_name: str, known_names: set[str]) -> bool:
        if model_name in known_names:
            return True
        alias = f"{model_name}:latest"
        if alias in known_names:
            return True
        bare = model_name.split(":", 1)[0]
        return bare in known_names or f"{bare}:latest" in known_names

    def _health_openai(self) -> dict[str, str]:
        if not self.settings.openai_api_key:
            return {"status": "disabled", "detail": "OPENAI_API_KEY is empty"}
        try:
            url = f"{self.settings.openai_base_url.rstrip('/')}/models"
            headers = {"Authorization": f"Bearer {self.settings.openai_api_key}"}
            with httpx.Client(timeout=10) as client:
                response = client.get(url, headers=headers)
                response.raise_for_status()
            return {"status": "ok"}
        except Exception as exc:
            return {"status": "error", "detail": str(exc)}

    def _health_deepseek(self) -> dict[str, str]:
        if not self.settings.deepseek_api_key:
            return {"status": "disabled", "detail": "DEEPSEEK_API_KEY is empty"}
        try:
            url = f"{self.settings.deepseek_base_url.rstrip('/')}/models"
            headers = {"Authorization": f"Bearer {self.settings.deepseek_api_key}"}
            with httpx.Client(timeout=10) as client:
                response = client.get(url, headers=headers)
                response.raise_for_status()
            return {"status": "ok"}
        except Exception as exc:
            return {"status": "error", "detail": str(exc)}
