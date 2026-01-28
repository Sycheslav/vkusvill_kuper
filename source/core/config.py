import os
from pydantic_settings import BaseSettings
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()  

class Settings(BaseSettings):
    model_config = {
        "env_file": ".env",       
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }

    TG_BOT_TOKEN: str = ""  # Опционально, если бот парсера не используется
    REDIS_URL: str = "redis://redis:6379/0"

    VKUSVILL_PROXIES: str = ""

    @property
    def VKUSVILL_PROXY_LIST(self) -> List[str]:
        if not self.VKUSVILL_PROXIES:
            return []
        return [p.strip() for p in self.VKUSVILL_PROXIES.split(",") if p.strip()]

    VKUSVILL_CITY_COORDS: Dict[str, tuple[float, float]] = {
        "москва": (55.7558, 37.6173),
        "санкт-петербург": (59.9343, 30.3351),
        "спб": (59.9343, 30.3351),
        "питер": (59.9343, 30.3351),
        "новосибирск": (55.0084, 82.9357),
        "екатеринбург": (56.8389, 60.6057),
        "казань": (55.8304, 49.0661),
    }

    DATA_DIR: str = "source/data"
    
    # Redis Streams
    INPUT_STREAM: str = "food_parse_tasks"
    OUTPUT_STREAM: str = "food_parse_results"
    
    # Новые параметры для worker
    STREAM_MAXLEN: int = 2000  # Максимальная длина стримов
    RESULT_TTL_SEC: int = 900  # TTL ключа результата (15 минут)
    HEARTBEAT_TTL_SEC: int = 60  # TTL heartbeat (1 минута)
    
    # Consumer group
    PARSER_GROUP: str = "food_group"
    PARSER_CONSUMER: str = "worker-1"
    
    # API ключ 2GIS для геокодинга
    DGIS_API_KEY: str = ""

    # HTTP настройки для устойчивых запросов
    HTTP_TIMEOUT_SEC: int = 20
    HTTP_RETRIES: int = 2
    HTTP_RETRY_BACKOFF_SEC: float = 0.7

settings = Settings()