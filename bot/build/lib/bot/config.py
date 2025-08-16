from __future__ import annotations

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    discord_token: SecretStr = Field(..., description="Discord bot token")
    openai_api_key: SecretStr | None = Field(None, description="OpenAI (or compatible) API key")
    model_name: str = Field("gpt-4o", description="Primary LLM model identifier")
    embed_model: str = Field("sentence-transformers/all-MiniLM-L6-v2", description="Embedding model for retrieval")
    max_theory_chars: int = Field(1800, description="Max characters in theory responses")
    rate_limit_per_min: int = Field(15, description="Commands per user per minute")
    search_rate_limit_per_min: int = Field(30, description="Search/context commands per user per minute")
    retrieval_top_k: int = Field(4, description="Top K docs for retrieval augmentation")
    data_dir: str = Field("data/bot_store", description="Local path for vector store & logs")
    hash_salt: SecretStr = Field(..., description="Salt used for hashing user identifiers")
    allowed_channel_ids: list[int] | None = Field(None, description="Optional allowlist of channel IDs for ingestion/search")
    audit_log_path: str = Field("data/bot_store/audit.log.jsonl", description="Path for audit JSONL log")
    enable_metrics: bool = Field(True, description="Expose Prometheus metrics server")
    metrics_host: str = Field("0.0.0.0", description="Metrics server host")
    metrics_port: int = Field(9108, description="Metrics server port")
    max_context_results: int = Field(20, description="Max results for LLM context assembly")
    admin_role_ids: list[int] | None = Field(None, description="Role IDs allowed to run admin commands (ingest/purge)")
    lightweight_embeddings: bool = Field(False, description="Use lightweight hash-based embedder (testing)")
    json_logs: bool = Field(False, description="Emit structured JSON logs to stdout")

    model_config = SettingsConfigDict(env_file=".env", env_prefix="SOCIONICS_")

settings = Settings()  # type: ignore[arg-type]
