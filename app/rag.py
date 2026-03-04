import re
from typing import Any

from app.config import Settings
from app.document_ids import SOURCE_OUTLINE, normalize_document_id, split_document_id
from app.embeddings import EmbeddingClient
from app.llm import LLMGateway
from app.outline_client import OutlineClient
from app.vector_store import VectorStore


class RAGService:
    STOPWORDS = {
        "как",
        "что",
        "это",
        "для",
        "или",
        "если",
        "где",
        "когда",
        "какой",
        "какая",
        "какие",
        "кто",
        "про",
        "also",
        "with",
        "what",
        "when",
        "where",
        "how",
        "this",
        "that",
        "your",
        "ours",
        "нашей",
        "наша",
        "наши",
        "wiki",
    }

    TERM_STEMS = {
        "update": ("обнов", "апдейт", "update", "upgrade"),
        "install": ("установ", "инсталл", "install", "setup"),
        "migration": ("мигр", "migration", "migrate"),
        "backup": ("бэкап", "резерв", "backup"),
        "restore": ("восстанов", "restore"),
        "access": ("доступ", "access"),
        "server": ("сервер", "server"),
        "vpn": ("vpn", "впн"),
        "cpanel": ("cpanel",),
        "whm": ("whm",),
    }

    def __init__(self, settings: Settings):
        self.settings = settings
        self.embeddings = EmbeddingClient(settings)
        self.vectors = VectorStore(settings)
        self.outline = OutlineClient(settings)
        self.llm = LLMGateway(settings)

    @staticmethod
    def _tokens(text: str) -> set[str]:
        tokens = {part.lower() for part in re.findall(r"[A-Za-zА-Яа-я0-9_]{3,}", text)}
        return {token for token in tokens if token not in RAGService.STOPWORDS}

    @staticmethod
    def _technical_tokens(text: str) -> set[str]:
        tokens = {part.lower() for part in re.findall(r"[A-Za-z0-9_-]{3,}", text)}
        generic = {
            "with",
            "from",
            "about",
            "this",
            "that",
            "wiki",
            "our",
            "your",
        }
        return {token for token in tokens if token not in generic}

    @staticmethod
    def _normalize_title(text: str) -> str:
        cleaned = re.sub(r"[^A-Za-zА-Яа-я0-9]+", " ", text).strip().lower()
        return re.sub(r"\s+", " ", cleaned)

    @staticmethod
    def _extract_quoted_phrases(text: str) -> list[str]:
        patterns = [r'"([^"]{2,})"', r"'([^']{2,})'", r"«([^»]{2,})»"]
        phrases: list[str] = []
        for pattern in patterns:
            phrases.extend(match.strip() for match in re.findall(pattern, text) if match.strip())
        uniq: list[str] = []
        seen: set[str] = set()
        for phrase in phrases:
            key = phrase.lower()
            if key not in seen:
                seen.add(key)
                uniq.append(phrase)
        return uniq

    @staticmethod
    def _outline_doc_id(raw_doc_id: str | None) -> str:
        normalized = normalize_document_id(raw_doc_id)
        if not normalized:
            return ""
        parsed = split_document_id(normalized)
        if not parsed:
            return ""
        source, _native_id = parsed
        if source != SOURCE_OUTLINE:
            return ""
        return normalized

    @staticmethod
    def _outline_native_doc_id(document_id: str | None) -> str:
        parsed = split_document_id(document_id)
        if not parsed:
            return ""
        source, native_id = parsed
        if source != SOURCE_OUTLINE:
            return ""
        return native_id

    @classmethod
    def _translated_terms(cls, text: str) -> set[str]:
        raw_tokens = cls._tokens(text)
        translated: set[str] = set()
        for token in raw_tokens:
            for target, stems in cls.TERM_STEMS.items():
                if any(token.startswith(stem) for stem in stems):
                    translated.add(target)
        return translated

    @classmethod
    def _query_variants(cls, question: str) -> list[str]:
        base = " ".join(question.strip().split())
        variants: list[str] = [base] if base else []

        quoted = cls._extract_quoted_phrases(question)
        variants.extend(quoted)

        technical = sorted(cls._technical_tokens(question), key=len, reverse=True)
        translated = sorted(
            {term for term in cls._translated_terms(question) if term not in set(technical)},
            key=len,
            reverse=True,
        )

        for token in technical:
            variants.append(token)

        if technical:
            variants.append(" ".join(technical))

        if translated:
            variants.append(" ".join(translated))

        if technical and translated:
            variants.append(" ".join(technical + translated))
            for token in technical[:3]:
                variants.append(f"{token} {' '.join(translated)}".strip())

        deduped: list[str] = []
        seen: set[str] = set()
        for item in variants:
            query = " ".join(str(item).strip().split())
            if len(query) < 2:
                continue
            key = query.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(query)
        return deduped

    def _outline_multi_search(
        self,
        question: str,
        *,
        per_query_limit: int,
        max_queries: int,
        allowed_document_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if allowed_document_ids is not None and not allowed_document_ids:
            return rows
        for query in self._query_variants(question)[:max_queries]:
            try:
                found = self.outline.search_documents(query, limit=per_query_limit)
            except Exception:
                continue
            if allowed_document_ids is not None:
                filtered: list[dict[str, Any]] = []
                for row in found:
                    doc = row.get("document") if isinstance(row, dict) else None
                    doc_id = self._outline_doc_id(str((doc or {}).get("id") or "").strip())
                    if doc_id and doc_id in allowed_document_ids:
                        filtered.append(row)
                found = filtered
            rows.extend(found)
        return rows

    def _title_fallback_rows(
        self,
        question: str,
        limit: int,
        allowed_document_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        if allowed_document_ids is not None and not allowed_document_ids:
            return []
        question_tokens = self._tokens(question).union(self._translated_terms(question))
        question_tech = self._technical_tokens(question)
        if not question_tokens and not question_tech:
            return []

        candidates: list[tuple[float, dict[str, Any]]] = []
        try:
            for item in self.outline.iter_documents():
                doc_id = str(item.get("id") or "").strip()
                if not doc_id:
                    continue
                prefixed_doc_id = self._outline_doc_id(doc_id)
                if allowed_document_ids is not None and prefixed_doc_id not in allowed_document_ids:
                    continue
                if not prefixed_doc_id:
                    continue
                title = str(item.get("title") or "Untitled")
                title_tokens = self._tokens(title).union(self._translated_terms(title))
                title_tech = self._technical_tokens(title)
                overlap = len(question_tokens.intersection(title_tokens))
                tech_overlap = len(question_tech.intersection(title_tech))
                if overlap <= 0 and tech_overlap <= 0:
                    continue

                score = overlap * 0.35 + tech_overlap * 0.7
                url = str(item.get("url") or "")
                if url.startswith("/"):
                    url = f"{self.settings.outline_base_url.rstrip('/')}{url}"

                candidates.append(
                    (
                        score,
                        {
                            "ranking": score,
                            "context": "",
                            "document": {
                                "id": prefixed_doc_id,
                                "title": title,
                                "url": url,
                            },
                        },
                    )
                )
        except Exception:
            return []

        candidates.sort(key=lambda row: row[0], reverse=True)
        return [row for _, row in candidates[:limit]]

    def retrieve_full_documents(
        self,
        question: str,
        allowed_document_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        if allowed_document_ids is not None and not allowed_document_ids:
            return []
        question_norm = self._normalize_title(question)
        question_tokens = self._tokens(question).union(self._translated_terms(question))
        quoted_norm = [self._normalize_title(phrase) for phrase in self._extract_quoted_phrases(question)]
        technical_tokens = self._technical_tokens(question)

        per_query_limit = max(10, min(40, self.settings.full_doc_search_limit))
        rows = self._outline_multi_search(
            question,
            per_query_limit=per_query_limit,
            max_queries=8,
            allowed_document_ids=allowed_document_ids,
        )
        if not rows:
            rows = self._title_fallback_rows(
                question,
                limit=max(10, self.settings.full_doc_search_limit),
                allowed_document_ids=allowed_document_ids,
            )
        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()

        for row in rows:
            if not isinstance(row, dict):
                continue
            doc = row.get("document")
            if not isinstance(doc, dict):
                continue
            doc_id = str(doc.get("id") or "").strip()
            prefixed_doc_id = self._outline_doc_id(doc_id)
            if not prefixed_doc_id or prefixed_doc_id in seen:
                continue
            if allowed_document_ids is not None and prefixed_doc_id not in allowed_document_ids:
                continue
            seen.add(prefixed_doc_id)

            title = str(doc.get("title") or "Untitled")
            title_norm = self._normalize_title(title)
            title_tokens = self._tokens(title).union(self._translated_terms(title))

            ranking = float(row.get("ranking") or 0.0)
            overlap = len(question_tokens.intersection(title_tokens))
            tech_overlap = len(technical_tokens.intersection(self._technical_tokens(title)))
            score = ranking + overlap * 0.2 + tech_overlap * 0.35

            if question_norm and title_norm == question_norm:
                score += 4.0
            if question_norm and question_norm in title_norm:
                score += 1.5

            for phrase in quoted_norm:
                if phrase and phrase in title_norm:
                    score += 5.0

            url = str(doc.get("url") or "")
            if url.startswith("/"):
                url = f"{self.settings.outline_base_url.rstrip('/')}{url}"

            candidates.append(
                {
                    "document_id": prefixed_doc_id,
                    "title": title,
                    "url": url,
                    "score": score,
                }
            )

        candidates.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)

        full_docs: list[dict[str, Any]] = []
        for candidate in candidates[: max(1, self.settings.full_doc_max_docs)]:
            native_doc_id = self._outline_native_doc_id(candidate["document_id"])
            if not native_doc_id:
                continue
            try:
                info = self.outline.get_document(native_doc_id)
            except Exception:
                continue
            text = str(info.get("text") or info.get("content") or "").strip()
            if not text:
                continue
            text = re.sub(r"\n{3,}", "\n\n", text)
            if len(text) > self.settings.full_doc_max_chars:
                text = text[: self.settings.full_doc_max_chars]
            full_docs.append(
                {
                    "document_id": candidate["document_id"],
                    "title": candidate["title"],
                    "url": candidate["url"],
                    "text": text,
                    "score": candidate["score"],
                }
            )

        return full_docs

    def retrieve(
        self,
        question: str,
        top_k: int,
        allowed_document_ids: set[str] | None = None,
    ) -> tuple[str, list[dict]]:
        if allowed_document_ids is not None and not allowed_document_ids:
            return "", []
        hits: list[dict] = []

        try:
            query_vector = self.embeddings.embed_query(question)
            vector_hits = self.vectors.search(
                query_vector=query_vector,
                limit=max(top_k * 40, 200),
                allowed_document_ids=allowed_document_ids,
            )
            for hit in vector_hits:
                item = dict(hit)
                normalized_doc_id = normalize_document_id(str(item.get("document_id") or "").strip())
                if not normalized_doc_id:
                    continue
                if allowed_document_ids is not None and normalized_doc_id not in allowed_document_ids:
                    continue
                item["document_id"] = normalized_doc_id
                item["source"] = "vector"
                item["base_score"] = float(hit.get("score", 0.0))
                hits.append(item)
        except Exception:
            pass

        try:
            search_rows = self._outline_multi_search(
                question,
                per_query_limit=max(top_k * 4, 24),
                max_queries=8,
                allowed_document_ids=allowed_document_ids,
            )
            if not search_rows:
                search_rows = self._title_fallback_rows(
                    question,
                    limit=max(top_k * 8, 24),
                    allowed_document_ids=allowed_document_ids,
                )
            for row in search_rows:
                doc = row.get("document") if isinstance(row, dict) else None
                if not isinstance(doc, dict):
                    continue
                doc_id = self._outline_doc_id(str(doc.get("id") or "").strip())
                if not doc_id:
                    continue
                if allowed_document_ids is not None and doc_id not in allowed_document_ids:
                    continue
                context = str(row.get("context") or "")
                context = re.sub(r"<[^>]+>", "", context).strip()
                text = context or str(doc.get("text") or "")
                if len(text) > 1500:
                    text = text[:1500]
                ranking = float(row.get("ranking") or 0.0)
                base = 0.82 + max(0.0, min(1.0, ranking)) * 0.25
                url = str(doc.get("url") or "")
                if url.startswith("/"):
                    url = f"{self.settings.outline_base_url.rstrip('/')}{url}"
                hits.append(
                    {
                        "document_id": doc_id,
                        "title": str(doc.get("title") or "Untitled"),
                        "url": url,
                        "text": text,
                        "score": base,
                        "source": "outline_search",
                        "base_score": base,
                    }
                )
        except Exception:
            pass

        question_tokens = self._tokens(question).union(self._translated_terms(question))
        technical_tokens = self._technical_tokens(question)
        question_lower = question.lower().strip()

        merged_by_doc: dict[str, dict] = {}
        for hit in hits:
            doc_id = str(hit.get("document_id") or "").strip()
            if not doc_id:
                continue
            current = merged_by_doc.get(doc_id)
            if not current or float(hit.get("base_score", 0.0)) > float(current.get("base_score", 0.0)):
                merged_by_doc[doc_id] = dict(hit)

        ranked_hits: list[dict] = []
        for hit in merged_by_doc.values():
            corpus = f"{hit.get('title', '')} {hit.get('text', '')}"
            corpus_tokens = self._tokens(corpus).union(self._translated_terms(corpus))
            corpus_tech_tokens = self._technical_tokens(corpus)
            overlap = len(question_tokens.intersection(corpus_tokens))
            tech_overlap = len(technical_tokens.intersection(corpus_tech_tokens))
            title_lower = str(hit.get("title") or "").lower()
            title_tokens = self._tokens(title_lower)
            title_exact = 1 if question_lower and question_lower in title_lower else 0
            title_tech_overlap = len(technical_tokens.intersection(title_tokens))

            final_score = float(hit.get("base_score", 0.0))
            final_score += overlap * self.settings.search_keyword_boost
            final_score += tech_overlap * (self.settings.search_keyword_boost * 1.6)
            final_score += title_tech_overlap * (self.settings.search_keyword_boost * 2.0)
            if title_exact:
                final_score += 0.5

            hit["overlap"] = overlap
            hit["tech_overlap"] = tech_overlap
            hit["title_exact"] = title_exact
            hit["final_score"] = final_score
            ranked_hits.append(hit)

        ranked_hits.sort(key=lambda row: float(row.get("final_score", 0.0)), reverse=True)

        technical_hits = [row for row in ranked_hits if int(row.get("tech_overlap", 0)) > 0]
        if technical_tokens and technical_hits:
            ranked_hits = technical_hits

        context_lines: list[str] = []
        sources: list[dict] = []
        seen_docs: set[str] = set()

        title_exact_hits = [row for row in ranked_hits if int(row.get("title_exact", 0)) > 0]
        overlapped_hits = [row for row in ranked_hits if int(row.get("overlap", 0)) > 0]

        if title_exact_hits:
            candidates = title_exact_hits + [row for row in ranked_hits if row not in title_exact_hits]
        elif len(overlapped_hits) >= 2:
            candidates = overlapped_hits
        else:
            candidates = ranked_hits

        selected = 0
        for hit in candidates:
            if (
                float(hit.get("score", 0.0)) < self.settings.search_min_score
                and int(hit.get("overlap", 0)) == 0
                and int(hit.get("title_exact", 0)) == 0
            ):
                continue
            selected += 1
            index = selected
            if selected > top_k:
                break
            context_lines.append(f"[{index}] {hit['title']}\nURL: {hit['url']}\n{hit['text']}")
            doc_id = hit["document_id"]
            if doc_id and doc_id not in seen_docs:
                seen_docs.add(doc_id)
                sources.append(
                    {
                        "document_id": doc_id,
                        "title": hit["title"],
                        "url": hit["url"],
                        "score": hit["score"],
                        "excerpt": hit["text"][:280],
                    }
                )

        return "\n\n".join(context_lines), sources

    def answer(
        self,
        question: str,
        provider: str,
        top_k: int,
        allowed_document_ids: set[str] | None = None,
    ) -> dict:
        context, sources = self.retrieve(
            question=question,
            top_k=top_k,
            allowed_document_ids=allowed_document_ids,
        )
        full_docs = self.retrieve_full_documents(
            question=question,
            allowed_document_ids=allowed_document_ids,
        )

        known = {str(item.get("document_id") or "") for item in sources}
        for item in full_docs:
            doc_id = str(item.get("document_id") or "")
            if not doc_id or doc_id in known:
                continue
            known.add(doc_id)
            sources.append(
                {
                    "document_id": doc_id,
                    "title": str(item.get("title") or "Untitled"),
                    "url": str(item.get("url") or ""),
                    "score": float(item.get("score") or 0.0),
                    "excerpt": str(item.get("text") or "")[:280],
                }
            )

        full_context = "\n\n".join(
            f"[DOC {i}] {item['title']}\nURL: {item['url']}\n{item['text']}"
            for i, item in enumerate(full_docs, start=1)
        )

        if context or full_context:
            user_prompt = (
                "Answer the question using only the context below. "
                "Add source markers like [1], [2] inline for factual statements.\n\n"
                f"Question: {question}\n\n"
                f"Context:\n{context}\n\nFull documents:\n{full_context}"
            )
        else:
            user_prompt = (
                "No indexed knowledge was found for this question. "
                "Say that the answer is not in the current Outline index and suggest syncing.\n\n"
                f"Question: {question}"
            )

        messages = [
            {
                "role": "system",
                "content": "You are OAssist, a knowledge-base assistant for Outline and Notion. Be concise and practical.",
            },
            {"role": "user", "content": user_prompt},
        ]

        answer, used_provider = self.llm.generate(messages=messages, requested_provider=provider)
        return {
            "provider": used_provider,
            "answer": answer,
            "sources": sources,
        }
