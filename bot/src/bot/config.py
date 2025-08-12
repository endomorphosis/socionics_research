from __future__ import annotations

from pydantic import BaseSettings, Field, SecretStr

class Settings(BaseSettings):
    discord_token: SecretStr = Field(..., description="Discord bot token")
    openai_api_key: SecretStr | None = Field(None, description="OpenAI (or compatible) API key")
    model_name: str = Field("gpt-4o", description="Primary LLM model identifier")
    embed_model: str = Field("sentence-transformers/all-MiniLM-L6-v2", description="Embedding model for retrieval")
    max_theory_chars: int = Field(1800, description="Max characters in theory responses")
    rate_limit_per_min: int = Field(15, description="Commands per user per minute")
    retrieval_top_k: int = Field(4, description="Top K docs for retrieval augmentation")
    data_dir: str = Field("data/bot_store", description="Local path for vector store & logs")

    class Config:
        env_file = ".env"
        env_prefix = "SOCIONICS_"

settings = Settings()  # type: ignore[arg-type]
