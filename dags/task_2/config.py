from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    POSTGRES_DB_NAME: str
    POSTGRES_DB_USER: str
    POSTGRES_DB_PASSWORD: str
    POSTGRES_DB_HOST: str
    POSTGRES_DB_PORT: int

    S3_BUCKET_NAME: str
    S3_ENDPOINT: str
    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str


settings = Settings() # noqa
