from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    KAFKA_BOOTSTRAP: str = "localhost:19092"
    TOPIC_RAW: str = "md.orderbook.raw"
    TOPIC_NORMALIZED: str = "md.orderbook.normalized"
    DEPTH: int = 10


settings = Settings()
