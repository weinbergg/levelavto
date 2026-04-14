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
    SMS_PROVIDER: str = Field(default="log")
    SMS_CODE_LENGTH: int = Field(default=6)
    SMS_CODE_TTL_SEC: int = Field(default=300)
    SMS_RESEND_COOLDOWN_SEC: int = Field(default=60)
    SMS_MAX_ATTEMPTS: int = Field(default=5)
    SMS_SENDER_NAME: str | None = Field(default=None)
    SMS_RU_API_ID: str | None = Field(default=None)
    EMAIL_PROVIDER: str = Field(default="smtp")
    EMAIL_CODE_LENGTH: int = Field(default=6)
    EMAIL_CODE_TTL_SEC: int = Field(default=900)
    EMAIL_RESEND_COOLDOWN_SEC: int = Field(default=60)
    EMAIL_MAX_ATTEMPTS: int = Field(default=5)
    EMAIL_HOST: str | None = Field(default=None)
    EMAIL_PORT: int = Field(default=587)
    EMAIL_HOST_USER: str | None = Field(default=None)
    EMAIL_HOST_PASSWORD: str | None = Field(default=None)
    EMAIL_FROM: str | None = Field(default=None)
    EMAIL_USE_TLS: bool = Field(default=True)
    EMAIL_USE_SSL: bool = Field(default=False)

    @property
    def sync_database_url(self) -> str:
        return f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def project_root(self) -> Path:
        # backend/app/config.py -> parents[2] == repo root (/app)
        return Path(__file__).resolve().parents[2]


settings = Settings()
