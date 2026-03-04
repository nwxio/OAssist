from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)

    oassist_admin_token: str | None = Field(default=None, alias="OASSIST_ADMIN_TOKEN")

    auth_db_path: str = Field(default="/data/oassist_auth.db", alias="AUTH_DB_PATH")
    auth_session_ttl_hours: int = Field(default=168, alias="AUTH_SESSION_TTL_HOURS")
    auth_bootstrap_admin_username: str = Field(default="admin", alias="AUTH_BOOTSTRAP_ADMIN_USERNAME")
    auth_bootstrap_admin_password: str = Field(default="admin12345", alias="AUTH_BOOTSTRAP_ADMIN_PASSWORD")
    auth_oidc_enabled: bool = Field(default=False, alias="AUTH_OIDC_ENABLED")
    auth_oidc_client_id: str | None = Field(default=None, alias="AUTH_OIDC_CLIENT_ID")
    auth_oidc_client_secret: str | None = Field(default=None, alias="AUTH_OIDC_CLIENT_SECRET")
    auth_oidc_auth_uri: str | None = Field(default=None, alias="AUTH_OIDC_AUTH_URI")
    auth_oidc_token_uri: str | None = Field(default=None, alias="AUTH_OIDC_TOKEN_URI")
    auth_oidc_userinfo_uri: str | None = Field(default=None, alias="AUTH_OIDC_USERINFO_URI")
    auth_oidc_redirect_uri: str | None = Field(default=None, alias="AUTH_OIDC_REDIRECT_URI")
    auth_oidc_scopes: str = Field(default="openid profile email", alias="AUTH_OIDC_SCOPES")
    auth_oidc_groups_claim: str = Field(default="groups", alias="AUTH_OIDC_GROUPS_CLAIM")
    auth_oidc_username_claim: str = Field(default="preferred_username", alias="AUTH_OIDC_USERNAME_CLAIM")
    auth_oidc_email_claim: str = Field(default="email", alias="AUTH_OIDC_EMAIL_CLAIM")
    auth_oidc_admin_group: str = Field(default="outline-admins", alias="AUTH_OIDC_ADMIN_GROUP")
    auth_oidc_user_group: str = Field(default="outline-users", alias="AUTH_OIDC_USER_GROUP")
    auth_oidc_display_name: str = Field(default="Keycloak", alias="AUTH_OIDC_DISPLAY_NAME")

    outline_base_url: str = Field(alias="OUTLINE_BASE_URL")
    outline_api_token: str = Field(alias="OUTLINE_API_TOKEN")

    notion_api_base_url: str = Field(default="https://api.notion.com/v1", alias="NOTION_API_BASE_URL")
    notion_api_version: str = Field(default="2022-06-28", alias="NOTION_API_VERSION")
    notion_api_token: str | None = Field(default=None, alias="NOTION_API_TOKEN")
    notion_oauth_authorize_url: str = Field(
        default="https://api.notion.com/v1/oauth/authorize",
        alias="NOTION_OAUTH_AUTHORIZE_URL",
    )
    notion_oauth_token_url: str = Field(default="https://api.notion.com/v1/oauth/token", alias="NOTION_OAUTH_TOKEN_URL")
    notion_oauth_client_id: str | None = Field(default=None, alias="NOTION_OAUTH_CLIENT_ID")
    notion_oauth_client_secret: str | None = Field(default=None, alias="NOTION_OAUTH_CLIENT_SECRET")
    notion_oauth_redirect_uri: str | None = Field(default=None, alias="NOTION_OAUTH_REDIRECT_URI")

    qdrant_url: str = Field(default="http://qdrant:6333", alias="QDRANT_URL")
    qdrant_api_key: str | None = Field(default=None, alias="QDRANT_API_KEY")
    qdrant_collection: str = Field(default="outline_docs", alias="QDRANT_COLLECTION")

    llm_provider: str = Field(default="auto", alias="LLM_PROVIDER")
    llm_fallback_order: str = Field(default="ollama,openai,deepseek", alias="LLM_FALLBACK_ORDER")
    llm_temperature: float = Field(default=0.2, alias="LLM_TEMPERATURE")

    openai_base_url: str = Field(default="https://api.openai.com/v1", alias="OPENAI_BASE_URL")
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", alias="OPENAI_MODEL")
    openai_embedding_model: str = Field(default="text-embedding-3-small", alias="OPENAI_EMBEDDING_MODEL")

    deepseek_base_url: str = Field(default="https://api.deepseek.com/v1", alias="DEEPSEEK_BASE_URL")
    deepseek_api_key: str | None = Field(default=None, alias="DEEPSEEK_API_KEY")
    deepseek_model: str = Field(default="deepseek-chat", alias="DEEPSEEK_MODEL")

    ollama_base_url: str = Field(default="http://ollama:11434", alias="OLLAMA_BASE_URL")
    ollama_model: str = Field(default="qwen2.5:14b", alias="OLLAMA_MODEL")
    ollama_embed_model: str = Field(default="nomic-embed-text", alias="OLLAMA_EMBED_MODEL")

    embedding_provider: str = Field(default="ollama", alias="EMBEDDING_PROVIDER")

    sync_page_size: int = Field(default=50, alias="SYNC_PAGE_SIZE")
    sync_interval_seconds: int = Field(default=300, alias="SYNC_INTERVAL_SECONDS")

    chunk_size: int = Field(default=1200, alias="CHUNK_SIZE")
    chunk_overlap: int = Field(default=200, alias="CHUNK_OVERLAP")
    search_top_k: int = Field(default=6, alias="SEARCH_TOP_K")
    search_min_score: float = Field(default=0.68, alias="SEARCH_MIN_SCORE")
    search_keyword_boost: float = Field(default=0.08, alias="SEARCH_KEYWORD_BOOST")
    full_doc_search_limit: int = Field(default=60, alias="FULL_DOC_SEARCH_LIMIT")
    full_doc_max_docs: int = Field(default=2, alias="FULL_DOC_MAX_DOCS")
    full_doc_max_chars: int = Field(default=12000, alias="FULL_DOC_MAX_CHARS")

    request_timeout_seconds: int = Field(default=60, alias="REQUEST_TIMEOUT_SECONDS")

    @property
    def provider_order(self) -> list[str]:
        order = [part.strip().lower() for part in self.llm_fallback_order.split(",") if part.strip()]
        valid = [name for name in order if name in {"ollama", "openai", "deepseek"}]
        return valid or ["ollama", "openai", "deepseek"]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
