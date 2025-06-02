from pydantic_settings import BaseSettings, SettingsConfigDict


# Класс настроек приложения
class Settings(BaseSettings):
    # Строка подключения к БД для psycopg3
    DATABASE_URL: str = "postgresql+psycopg://user:pasword@localhost:5432/dbname"
    POOL_SIZE: int = 20
    MAX_OVERFLOW: int = 30
    POOL_TIMEOUT: int = 30
    POOL_RECYCLE: int = 3600

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


# Экземпляр настроек
settings = Settings()