from async_tls_client.session.session import AsyncSession
import logging
import time
import json
import hashlib
from typing import Optional

import redis.asyncio as redis

from source.core.config import settings

logger = logging.getLogger(__name__)

# Дефолтные координаты (Москва)
DEFAULT_COORDS = (55.7558, 37.6173)

# TTL для кэша геокодинга (24 часа)
GEO_CACHE_TTL_SEC = 86400

# In-memory кэш для текущего процесса
_local_geo_cache: dict[str, tuple[float, float]] = {}


def _get_cache_key(city_name: str) -> str:
    """Генерирует ключ кэша для города"""
    normalized = city_name.strip().lower()
    return f"geo:cache:{hashlib.md5(normalized.encode()).hexdigest()[:12]}"


async def get_coords_by_city(
    city_name: str, 
    redis_client: Optional[redis.Redis] = None
) -> tuple[float, float]:
    """
    Получить координаты города через 2GIS API с кэшированием.
    
    Args:
        city_name: Название города или адрес
        redis_client: Опциональный Redis клиент для кэширования
        
    Returns:
        (lat, lon) - координаты
    """
    normalized_city = city_name.strip().lower()
    
    # 1. Проверяем in-memory кэш
    if normalized_city in _local_geo_cache:
        logger.debug("Geo cache hit (local): %s", city_name)
        return _local_geo_cache[normalized_city]
    
    # 2. Проверяем Redis кэш
    cache_key = _get_cache_key(city_name)
    if redis_client:
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                data = json.loads(cached)
                lat, lon = float(data["lat"]), float(data["lon"])
                _local_geo_cache[normalized_city] = (lat, lon)
                logger.info("Geo cache hit (Redis): %s -> %s, %s", city_name, lat, lon)
                return lat, lon
        except Exception as e:
            logger.warning("Ошибка чтения geo кэша из Redis: %s", e)
    
    # 3. Запрашиваем 2GIS API
    if not settings.DGIS_API_KEY:
        logger.warning("DGIS_API_KEY не задан, используем дефолтные координаты для %s", city_name)
        return DEFAULT_COORDS
    
    api_key = settings.DGIS_API_KEY
    
    session = AsyncSession(client_identifier="chrome_120", random_tls_extension_order=True)
    headers = {
        'client-id': 'KuperAndroid',
        'client-ver': '15.1.29',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'client-bundleid': 'ru.instamart',
        'api-version': '2.2',
        'cache-control': 'no-store',
        'content-type': 'application/json',
        'anonymousid': '03d2c216ac476531',
        'backenduseruuid': '',
        'user-uuid': '',
        'screenname': 'MapScreen',
    }
    try:
        start = time.monotonic()
        logger.info("2GIS lookup start | city=%s", city_name)
        resp = await session.get(
            "https://catalog.api.2gis.com/3.0/suggests",
            params={
                "key": api_key,
                "q": city_name,
                "type": "building,street,adm_div.city",
                "suggest_type": "address",
                "fields": "items.full_address_name,items.address,items.adm_div,items.point",
                "location": "37.584212,55.645531"
            },
            headers=headers,
            timeout=settings.HTTP_TIMEOUT_SEC
        )
        data = resp.json()
        elapsed = time.monotonic() - start
        items = data.get("result", {}).get("items", [])
        logger.info("2GIS lookup response | city=%s | items=%d | elapsed=%.2fs", city_name, len(items), elapsed)
        
        if not items:
            logger.warning("2GIS не нашел город: %s, используем дефолтные координаты", city_name)
            return DEFAULT_COORDS
            
        point = items[0].get("point", {})
        lat = float(point.get("lat", DEFAULT_COORDS[0]))
        lon = float(point.get("lon", DEFAULT_COORDS[1]))
        
        # 4. Сохраняем в кэш
        _local_geo_cache[normalized_city] = (lat, lon)
        
        if redis_client:
            try:
                await redis_client.set(
                    cache_key,
                    json.dumps({"lat": lat, "lon": lon}),
                    ex=GEO_CACHE_TTL_SEC
                )
                logger.debug("Geo cached to Redis: %s -> %s, %s", city_name, lat, lon)
            except Exception as e:
                logger.warning("Ошибка записи geo кэша в Redis: %s", e)
        
        logger.info("2GIS: %s -> %s, %s", city_name, lat, lon)
        return lat, lon
        
    except Exception as e:
        logger.error("2GIS ошибка для %s: %s", city_name, e)
        return DEFAULT_COORDS
    finally:
        await session.close()