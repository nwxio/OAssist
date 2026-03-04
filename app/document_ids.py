from __future__ import annotations

SOURCE_OUTLINE = "outline"
SOURCE_NOTION = "notion"
SOURCE_FILES = "files"
VALID_SOURCES = {SOURCE_OUTLINE, SOURCE_NOTION, SOURCE_FILES}

SOURCE_MODE_OUTLINE = "outline"
SOURCE_MODE_NOTION = "notion"
SOURCE_MODE_BOTH = "both"
VALID_SOURCE_MODES = {SOURCE_MODE_OUTLINE, SOURCE_MODE_NOTION, SOURCE_MODE_BOTH}


def normalize_source_mode(raw: str | None) -> str:
    mode = str(raw or "").strip().lower()
    if mode in VALID_SOURCE_MODES:
        return mode
    return SOURCE_MODE_OUTLINE


def enabled_sources_from_mode(mode: str | None) -> set[str]:
    normalized = normalize_source_mode(mode)
    if normalized == SOURCE_MODE_BOTH:
        return {SOURCE_OUTLINE, SOURCE_NOTION}
    if normalized == SOURCE_MODE_NOTION:
        return {SOURCE_NOTION}
    return {SOURCE_OUTLINE}


def make_document_id(source: str, native_id: str) -> str:
    src = str(source or "").strip().lower()
    raw_id = str(native_id or "").strip()
    if src not in VALID_SOURCES or not raw_id:
        raise ValueError("invalid source document id")

    prefix = f"{src}:"
    while raw_id.lower().startswith(prefix):
        raw_id = raw_id[len(prefix) :].strip()
        if not raw_id:
            raise ValueError("invalid source document id")

    return f"{src}:{raw_id}"


def split_document_id(raw: str | None) -> tuple[str, str] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if ":" not in text:
        return SOURCE_OUTLINE, text
    source, native_id = text.split(":", 1)
    source = source.strip().lower()
    native_id = native_id.strip()
    if source not in VALID_SOURCES or not native_id:
        return None

    repeated_prefix = f"{source}:"
    while native_id.lower().startswith(repeated_prefix):
        native_id = native_id[len(repeated_prefix) :].strip()
        if not native_id:
            return None

    return source, native_id


def normalize_document_id(raw: str | None) -> str:
    parsed = split_document_id(raw)
    if parsed is None:
        return ""
    source, native_id = parsed
    return make_document_id(source, native_id)
