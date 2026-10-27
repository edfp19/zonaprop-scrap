from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    data_root: Path = Path("data")
    database_url: SecretStr | None = None
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "zonaprop"
    postgres_user: str = "zonaprop"
    postgres_password: str = "zonaprop"
    selenium_url: str = "http://localhost:4444/wd/hub"
    collection_max_pages: int = Field(default=1, ge=1, le=20)
    collection_max_records: int = Field(default=250, ge=1)
    collection_max_bronze_bytes: int = Field(default=10_000_000, ge=1)
    pipeline_max_silver_bytes: int = Field(default=5_000_000, ge=1)
    collection_delay_seconds: float = Field(default=10, ge=1, le=3600)
    collection_batch_pages: int = Field(default=1, ge=1, le=8)
    collection_batch_cooldown_seconds: float = Field(default=60, ge=1, le=3600)
    collection_page_timeout_seconds: int = Field(default=45, ge=5)
    pipeline_interval_seconds: int = Field(default=86_400, ge=60)
    spark_master: str = "local[2]"
    comparable_min_count: int = Field(default=10, ge=1)
    comparable_area_tolerance: float = Field(default=0.15, gt=0, le=0.5)
    log_level: str = "INFO"

    @property
    def postgres_dsn(self) -> str:
        if self.database_url is not None:
            return self.database_url.get_secret_value().strip()
        return (
            f"postgresql://{quote(self.postgres_user, safe='')}:"
            f"{quote(self.postgres_password, safe='')}"
            f"@{self.postgres_host}:{self.postgres_port}/{quote(self.postgres_db, safe='')}"
        )

    @property
    def postgres_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
