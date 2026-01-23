from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore")

    APP_ENV: str = Field(default="development")
    APP_SECRET: str = Field(default="please-change-me")

    DB_HOST: str = Field(default="localhost")
    DB_PORT: int = Field(default=5432)
    DB_USER: str = Field(default="autodealer")
    DB_PASSWORD: str = Field(default="autodealer")
    DB_NAME: str = Field(default="autodealer")
    DB_SYNC_ECHO: bool = Field(default=False)
    DB_POOL_SIZE: int = Field(default=10)
    DB_MAX_OVERFLOW: int = Field(default=20)
    DB_POOL_TIMEOUT: int = Field(default=30)

    PARSER_USER_AGENT: str = Field(
        default="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )
    PARSER_REQUEST_TIMEOUT_SECONDS: int = Field(default=20)
    PARSER_MIN_DELAY_SECONDS: int = Field(default=1)
    PARSER_MAX_DELAY_SECONDS: int = Field(default=3)
    PARSER_LOG_FILE: str = Field(default="logs/parsing.log")
    MOBILE_DE_HTTP_PROXY: str | None = Field(default=None)
    ENCAR_CARAPIS_API_KEY: str | None = Field(default=None)
    ENCAR_CARAPIS_BASE_URL: str | None = Field(default=None)

    @property
    def sync_database_url(self) -> str:
        return f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def project_root(self) -> Path:
        # backend/app/config.py -> parents[2] == repo root (/app)
        return Path(__file__).resolve().parents[2]


settings = Settings()
