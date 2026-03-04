from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(min_length=1)
    provider: str = Field(default="auto")
    top_k: int = Field(default=6, ge=1, le=20)


class SourceItem(BaseModel):
    document_id: str
    title: str
    url: str
    score: float
    excerpt: str


class ChatResponse(BaseModel):
    provider: str
    answer: str
    sources: list[SourceItem]


class AssistantMessage(BaseModel):
    role: str = Field(pattern="^(user|assistant)$")
    content: str = Field(min_length=1)


class AssistantChatRequest(BaseModel):
    message: str = Field(min_length=1)
    provider: str = Field(default="auto")
    model: str | None = Field(default=None)
    chat_language: str | None = Field(default=None, pattern="^(ru|en)$")
    chat_language_hint: str | None = Field(default=None, max_length=500)
    custom_prompt: str | None = Field(default=None, max_length=4000)
    use_knowledge: bool = Field(default=True)
    top_k: int = Field(default=6, ge=1, le=20)
    history: list[AssistantMessage] = Field(default_factory=list)
    upload_ids: list[str] = Field(default_factory=list)
    request_artifact_format: str | None = Field(default=None, pattern="^(docx|xlsx|pdf|md|txt|csv|json)$")
    chat_id: str | None = Field(default=None, max_length=128)


class ArtifactItem(BaseModel):
    id: str
    filename: str
    format: str
    size: int
    expires_at: str
    download_url: str


class AssistantChatResponse(BaseModel):
    provider: str
    answer: str
    sources: list[SourceItem] = Field(default_factory=list)
    artifacts: list[ArtifactItem] = Field(default_factory=list)


class SyncResponse(BaseModel):
    indexed_documents: int
    indexed_chunks: int


class ProviderHealth(BaseModel):
    status: str
    detail: str | None = None


class ProvidersHealthResponse(BaseModel):
    ollama: ProviderHealth
    openai: ProviderHealth
    deepseek: ProviderHealth


class OllamaModelsResponse(BaseModel):
    models: list[str] = Field(default_factory=list)
    default_model: str


class SyncStartResponse(BaseModel):
    job_id: str
    status: str


class SyncStatusResponse(BaseModel):
    job_id: str | None = None
    status: str
    started_at: str | None = None
    finished_at: str | None = None
    message: str | None = None
    total_documents: int = 0
    processed_documents: int = 0
    indexed_documents: int = 0
    indexed_chunks: int = 0
    failed_documents: int = 0
    progress_percent: float = 0.0
    duration_seconds: float | None = None


class TextTaskRequest(BaseModel):
    text: str = Field(min_length=1)
    provider: str = Field(default="auto")


class RewriteRequest(TextTaskRequest):
    style: str = Field(default="clear and concise")


class TranslateRequest(TextTaskRequest):
    target_language: str = Field(min_length=2)


class TextTaskResponse(BaseModel):
    provider: str
    output: str


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


class AuthUserResponse(BaseModel):
    id: int
    username: str
    role: str
    is_active: bool
    source_mode: str = Field(default="outline", pattern="^(outline|notion|both)$")
    notion_connected: bool = False
    auth_method: str = Field(default="local", pattern="^(local|oidc)$")


class LoginResponse(BaseModel):
    token: str
    user: AuthUserResponse


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=8)
    role: str = Field(pattern="^(admin|user)$")
    source_mode: str = Field(default="outline", pattern="^(outline|notion|both)$")


class UpdateUserRequest(BaseModel):
    username: str | None = None
    password: str | None = Field(default=None, min_length=8)
    role: str | None = Field(default=None, pattern="^(admin|user)$")
    is_active: bool | None = None
    source_mode: str | None = Field(default=None, pattern="^(outline|notion|both)$")


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(min_length=1)
    new_password: str = Field(min_length=8)


class NotionTokenConnectRequest(BaseModel):
    token: str = Field(min_length=1)


class NotionConnectionResponse(BaseModel):
    connected: bool
    mode: str | None = Field(default=None, pattern="^(token|oauth)$")
    workspace_id: str | None = None
    workspace_name: str | None = None
    oauth_available: bool


class SourceGlobalSettingsResponse(BaseModel):
    outline_enabled: bool
    notion_enabled: bool
    files_enabled: bool


class SourceGlobalSettingsRequest(BaseModel):
    outline_enabled: bool
    notion_enabled: bool
    files_enabled: bool | None = None


class AuthOidcConfigResponse(BaseModel):
    enabled: bool
    display_name: str | None = None
    configured: bool | None = None


class AuthOidcGlobalSettingsResponse(BaseModel):
    configured: bool
    enabled: bool
    display_name: str | None = None


class AuthOidcGlobalSettingsRequest(BaseModel):
    enabled: bool
