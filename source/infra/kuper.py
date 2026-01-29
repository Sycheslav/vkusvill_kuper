import logging
from typing import List, Dict, Optional
import asyncio
import time
import random
import os
import pandas as pd

import redis.asyncio as redis

from source.application.parser_interface import BaseParser
from source.core.dto import Task, ParseResult, ProductID, ProductDetail
from source.infra.tls_client import TLSClient
from source.core.config import settings
from source.infra.geo import get_coords_by_city 
from source.utils.parse_coords import parse_city_or_coords
from async_tls_client.session.session import AsyncSession

logger = logging.getLogger("kuper_parser")


class KuperParser(BaseParser):
    BASE_URL = "https://api.kuper.ru/v2"

    HEADERS = {
            'client-id': 'KuperAndroid',
            'client-token': '241f3ea68b8ca03f60c4111b9f39c63d',
            'client-ver': '15.3.40',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            'client-bundleid': 'ru.instamart',
            'api-version': '2.2',
            'cache-control': 'no-store',
            'content-type': 'application/json',
            'anonymousid': '03d2c216ac476531',
            'screenname': 'MultiRetailSearch',
        }
    
    def __init__(self):
        self._session: Optional[AsyncSession] = None
    
    def _create_session(self) -> AsyncSession:
        """Создать новую сессию"""
        return AsyncSession(
            client_identifier="chrome_120",
            random_tls_extension_order=True
        )
    
    async def _close_session(self):
        """Закрыть текущую сессию"""
        if self._session:
            try:
                await self._session.close()
            except Exception as e:
                logger.warning("Ошибка закрытия сессии Kuper: %s", e)
            finally:
                self._session = None
    
    def _get_heavy_csv_path(self, store_name: str) -> str:
        """Получить путь к кэш-файлу для магазина"""
        store = (store_name or "unknown").lower().replace(" ", "_")
        return f"{settings.DATA_DIR}/kuper_heavy_{store}.csv"

    def _summarize_params(self, params: Optional[dict]) -> dict:
        if not params:
            return {}
        keys = {"sid", "tid", "limit", "products_offset", "sort", "lat", "lon", "shipping_method"}
        return {k: params.get(k) for k in keys if k in params}

    async def _get_store_id(self, lat, lon, store_name: str) -> Optional[tuple]:
        params = {'shipping_method': 'by_courier', 'lat': str(lat), 'lon': str(lon), 'include_labels_tree': 'true'}
        data = await self._request_json(f"{self.BASE_URL}/stores", params=params, headers=self.HEADERS)
        stores = data.get("stores", []) if isinstance(data, dict) else []
        if not stores:
            logger.warning("Kuper API не вернул список магазинов для координат %s, %s", lat, lon)
            return None
        store_key = store_name.lower()
        for store in stores:
            if store_key in store.get("name", "").lower():
                logger.info("Найден магазин: %s (id=%s)", store.get("name"), store["id"])
                return (store["id"], store.get("name", ""))
        # Если точное совпадение не найдено, берём первый
        logger.info("Точное совпадение не найдено для '%s', используем первый магазин: %s", store_name, stores[0].get("name"))
        return (stores[0]["id"], stores[0]["name"]) if stores else None

    def _parse_nutrient(self, value: str) -> Optional[float]:
        if not value:
            return None
        try:
            cleaned = str(value).replace(" ккал", "").replace(" г", "").strip()
            cleaned = cleaned.replace(",", ".")
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None

    def _extract_url(self, obj: dict) -> Optional[str]:
        if not isinstance(obj, dict):
            return None
        for key in ("url", "link", "product_url", "web_url", "share_url"):
            value = obj.get(key)
            if value:
                return value
        return None

    async def _request_json(
        self,
        url: str,
        *,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[int] = None,
    ):
        if not self._session:
            raise RuntimeError("Session not initialized. Call parse() first.")
        
        timeout = timeout or settings.HTTP_TIMEOUT_SEC
        retries = max(settings.HTTP_RETRIES, 0)
        backoff = max(settings.HTTP_RETRY_BACKOFF_SEC, 0)

        for attempt in range(retries + 1):
            started = time.monotonic()
            try:
                logger.info(
                    "Kuper HTTP GET start | url=%s | attempt=%s/%s | timeout=%ss | params=%s",
                    url.replace(self.BASE_URL, ""),
                    attempt + 1,
                    retries + 1,
                    timeout,
                    self._summarize_params(params),
                )
                resp = await self._session.get(url, params=params, headers=headers, timeout=timeout)
                status = getattr(resp, "status", None)
                
                # Обработка rate limiting
                if status == 429:
                    retry_after = int(resp.headers.get("Retry-After", backoff * (attempt + 2)))
                    logger.warning("Kuper rate limited (429), waiting %ds...", retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                    
                if status and status != 200:
                    raise ValueError(f"HTTP {status}")
                elapsed = time.monotonic() - started
                logger.info(
                    "Kuper HTTP GET ok | url=%s | status=%s | elapsed=%.2fs",
                    url.replace(self.BASE_URL, ""),
                    status,
                    elapsed,
                )
                return resp.json()
            except Exception as e:
                elapsed = time.monotonic() - started
                if attempt >= retries:
                    logger.warning(
                        "Kuper HTTP GET failed | url=%s | attempt=%s/%s | elapsed=%.2fs | error=%s",
                        url.replace(self.BASE_URL, ""),
                        attempt + 1,
                        retries + 1,
                        elapsed,
                        e,
                    )
                    return None
                logger.info(
                    "Kuper HTTP GET retry | url=%s | attempt=%s/%s | elapsed=%.2fs | error=%s",
                    url.replace(self.BASE_URL, ""),
                    attempt + 1,
                    retries + 1,
                    elapsed,
                    e,
                )
                await asyncio.sleep(backoff * (attempt + 1))
    
    async def parse(self, task: Task, redis_client: Optional[redis.Redis] = None) -> ParseResult:
        if task.mode == "fast":
            return await self.parse_fast(task, redis_client)
        elif task.mode == "heavy":
            return await self.parse_heavy(task, redis_client)
        else:
            raise ValueError(f"Unknown mode: {task.mode}")

    async def parse_fast(self, task: Task, redis_client: Optional[redis.Redis] = None) -> ParseResult:
            start = time.time()
            products = []
            # Локальная переменная вместо атрибута класса (fix race condition)
            current_store = (task.store or "лента").lower().strip()
            logger.info(
                "Kuper fast start | task_id=%s | city=%s | store=%s | limit=%s",
                task.task_id,
                task.city,
                current_store,
                task.limit,
            )
            
            # Создаём сессию в начале
            self._session = self._create_session()
            logger.info("Kuper: сессия создана")
            
            try:
                limit = int(task.limit) if task.limit else 500
            except (TypeError, ValueError):
                limit = 500
            if limit < 1:
                limit = 500
            stop_event = asyncio.Event()
            city = task.city.strip() or "москва"
            city_name, lat, lon = parse_city_or_coords(task.city)
            if lat is not None and lon is not None:
                use_lat, use_lon = lat, lon
                logger.info("Kuper fast | координаты из ввода: %s, %s", use_lat, use_lon)
            else:
                use_lat, use_lon = await get_coords_by_city(city_name, redis_client)
                logger.info("Kuper fast | координаты из геокодинга: %s, %s", use_lat, use_lon)

            logger.info("Kuper fast | city_name=%s | parsed_lat=%s | parsed_lon=%s", city_name, lat, lon)
            
            # Загружаем кэш
            heavy_csv_path = self._get_heavy_csv_path(current_store)
            heavy_df = None
            heavy_dict = None  # Используем dict для O(1) lookup
            if os.path.exists(heavy_csv_path):
                try:
                    heavy_df = pd.read_csv(heavy_csv_path, sep=";", dtype=str, keep_default_na=False)
                    heavy_df["sku"] = heavy_df["sku"].str.strip()
                    # Конвертируем в dict для быстрого поиска
                    heavy_dict = heavy_df.set_index("sku").to_dict("index")
                    logger.info("Kuper fast | кэш загружен: %d товаров", len(heavy_dict))
                except Exception as e:
                    logger.error("Ошибка чтения кэша: %s", e)

            try:
                result = await self._get_store_id(use_lat, use_lon, current_store)
                if not result:
                    logger.warning("Kuper fast | магазин не найден для %s", current_store)
                    raise ValueError("Store not found")
                store_id, store_name = result
                current_store = store_name
                logger.info("Kuper fast | store_id=%s | store_name=%s", store_id, store_name)
                taxon_data = await self._request_json(
                    f"{self.BASE_URL}/taxons",
                    params={"sid": store_id},
                    headers=self.HEADERS
                )
                taxons = taxon_data.get("taxons", []) if isinstance(taxon_data, dict) else []
                logger.info("Kuper fast | taxons всего: %d", len(taxons))

                tasks = []
                for taxon in taxons:
                    name = taxon.get("name", "")
                    if not any(kw in name.lower() for kw in ["готовая еда"]):
                        continue
                    tasks.append(self._fetch_fast(
                        store_id=store_id,
                        tid=taxon["id"],
                        category=name,
                        heavy_dict=heavy_dict,
                        result=products,
                        max_products=limit,
                        stop_event=stop_event,
                        store_name=current_store
                    ))

                if tasks:
                    logger.info("Kuper fast | задач для категорий: %d", len(tasks))
                    await asyncio.gather(*tasks, return_exceptions=True)
                else:
                    logger.warning("Kuper fast | не найдены категории 'готовая еда'")

            except Exception as e:
                logger.error("Kuper fast error: %s", e, exc_info=True)
            finally:
                # Закрываем сессию
                await self._close_session()

            took = round(time.time() - start, 1)
            if limit and len(products) > limit:
                products = products[:limit]
            logger.info("Kuper fast | %d товаров | %.1fс | кэш: %s", len(products), took, "ДА" if heavy_dict is not None else "НЕТ")

            return ParseResult(
                task_id=task.task_id,
                service="kuper",
                mode="fast",
                products=products,
                took_seconds=took,
                user_id=task.user_id,
                chat_id=task.chat_id
            )

    async def _fetch_fast(
        self,
        store_id: str,
        tid: str,
        category: str,
        heavy_dict: Optional[dict],
        result: list,
        max_products: int,
        stop_event: asyncio.Event,
        store_name: str
    ):
        offset = 0
        logger.info("Kuper fast | старт категории=%s id=%s", category, tid)
        while True:
            if stop_event.is_set():
                break
            if max_products and len(result) >= max_products:
                stop_event.set()
                break

            params = {"sid": store_id, "tid": tid, "limit": "24", "products_offset": str(offset), "sort": "popularity"}
            logger.info(
                "Kuper fast | category=%s | offset=%s",
                category,
                offset,
            )
            data = await self._request_json(
                f"{self.BASE_URL}/catalog/entities",
                params=params,
                headers=self.HEADERS
            )
            entities = data.get("entities", []) if isinstance(data, dict) else []
            if not entities:
                logger.info("Kuper fast | category=%s | нет больше entities", category)
                break
            logger.info("Kuper fast | category=%s | entities=%d", category, len(entities))

            for e in entities:
                if stop_event.is_set():
                    break

                sku = str(e.get("sku") or "")
                region_id = str(e["id"])

                name = e.get("name", "Без названия")
                price = e.get("price", 0)
                old_price = e.get("original_price")
                weight = e.get("human_volume") or f"{e.get('grams_per_unit', '')} г"
                photos = [img.get("original_url", "") for img in e.get("images", []) if img.get("original_url")]
                stock = e.get("stock", 0) or e.get("stock_info", {}).get("quantity", 0)
                in_stock = bool(stock > 0)
                url = self._extract_url(e)

                # O(1) lookup в dict вместо O(n) в DataFrame
                calories = proteins = fats = carbs = ingredients = None
                if heavy_dict and sku and sku in heavy_dict:
                    cached = heavy_dict[sku]
                    calories = cached.get("calories")
                    proteins = cached.get("proteins")
                    fats = cached.get("fats")
                    carbs = cached.get("carbs")
                    ingredients = cached.get("ingredients")

                result.append(ProductDetail(
                    product_id=region_id,  
                    name=name,
                    price=price,
                    old_price=old_price,
                    calories=calories,
                    proteins=proteins,
                    fats=fats,
                    carbs=carbs,
                    weight=weight,
                    ingredients=ingredients,
                    photos=photos,
                    category=category,
                    store=store_name.capitalize() if store_name else "Kuper",
                    in_stock=in_stock,
                    url=url
                ))

                if max_products and len(result) >= max_products:
                    stop_event.set()
                    break

            offset += 24

    async def parse_heavy(self, task: Task, redis_client: Optional[redis.Redis] = None) -> ParseResult:
        current_store = (task.store or "лента").lower().strip()
        start = time.time()
        detailed = []
        
        # Создаём сессию
        self._session = self._create_session()
        logger.info("Kuper heavy: сессия создана")
        
        try:
            limit = int(task.limit) if task.limit else 2000
        except (TypeError, ValueError):
            limit = 2000
        if limit < 1:
            limit = 2000

        try:
            city_name, lat, lon = parse_city_or_coords(task.city)
            if lat is not None and lon is not None:
                use_lat, use_lon = lat, lon
            else:
                use_lat, use_lon = await get_coords_by_city(city_name, redis_client)

            result = await self._get_store_id(use_lat, use_lon, current_store)
            if not result:
                logger.warning("Kuper heavy | магазин не найден для %s", current_store)
                raise ValueError("Store not found")
            store_id, store_name = result
            current_store = store_name
            
            taxon_data = await self._request_json(
                f"{self.BASE_URL}/taxons",
                params={"sid": store_id},
                headers=self.HEADERS
            )
            taxons = taxon_data.get("taxons", []) if isinstance(taxon_data, dict) else []
            logger.info("Kuper heavy | taxons: %d", len(taxons))

            cache_rows = []

            for taxon in taxons:
                if limit and len(detailed) >= limit:
                    break
                cat_name = taxon.get("name", "")
                if not any(kw in cat_name.lower() for kw in ["готовая еда"]):
                    continue
                
                logger.info("Kuper heavy | парсим категорию: %s", cat_name)

                offset = 0
                while True:
                    if limit and len(detailed) >= limit:
                        break
                    entities_data = await self._request_json(
                        f"{self.BASE_URL}/catalog/entities",
                        params={
                            "sid": store_id,
                            "tid": taxon["id"],
                            "limit": "24",
                            "products_offset": str(offset),
                            "sort": "popularity"
                        },
                        headers=self.HEADERS
                    )
                    entities = entities_data.get("entities", []) if isinstance(entities_data, dict) else []
                    if not entities:
                        break

                    for e in entities:
                        if limit and len(detailed) >= limit:
                            break
                        if e.get("type") != "product":
                            continue

                        region_id = str(e["id"])
                        sku = str(e.get("sku") or "")
                        if not sku or sku == "None" or sku == "nan":
                            continue 

                        card_data = await self._request_json(
                            f"{self.BASE_URL}/multicards/{region_id}",
                            headers=self.HEADERS
                        )
                        if not isinstance(card_data, dict):
                            continue
                        data = card_data.get("product", {})

                        props = {p["name"]: p["value"] for p in data.get("properties", [])}
                        stock = data.get("stock", 0) or data.get("stock_info", {}).get("quantity", 0)
                        in_stock = bool(stock > 0)

                        url = self._extract_url(data) or self._extract_url(e)

                        product = ProductDetail(
                            product_id=region_id,  
                            name=data.get("name") or e.get("name"),
                            price=(data.get("price") or 0),
                            old_price=(data.get("original_price") or 0) if data.get("original_price") else None,
                            calories=self._parse_nutrient(props.get("energy_value", "")),
                            proteins=self._parse_nutrient(props.get("protein", "")),
                            fats=self._parse_nutrient(props.get("fat", "")),
                            carbs=self._parse_nutrient(props.get("carbohydrate", "")),
                            weight=data.get("human_volume") or e.get("human_volume") or f"{e.get('grams_per_unit', '')} г",
                            ingredients=props.get("ingredients") or data.get("description", ""),
                            photos=[img.get("original_url", "") for img in data.get("images", []) if img.get("original_url")],
                            category=cat_name,
                            store=current_store.capitalize() if current_store else "Kuper",
                            in_stock=in_stock,
                            url=url
                        )
                        detailed.append(product)

                        cache_rows.append({
                            "sku": sku,
                            "calories": product.calories,
                            "proteins": product.proteins,
                            "fats": product.fats,
                            "carbs": product.carbs,
                            "ingredients": product.ingredients,
                        })

                    offset += 24
                    logger.info("Kuper heavy | category=%s | offset=%d | products=%d", cat_name, offset, len(detailed))

            if cache_rows:
                cache_df = pd.DataFrame(cache_rows)
                cache_df.drop_duplicates(subset=["sku"], keep="last", inplace=True)
                os.makedirs(settings.DATA_DIR, exist_ok=True)
                heavy_csv_path = self._get_heavy_csv_path(current_store)
                cache_df.to_csv(heavy_csv_path, sep=";", index=False, encoding="utf-8-sig")
                logger.info("HEAVY кэш сохранён по SKU: %s | %d товаров", heavy_csv_path, len(cache_df))

        except Exception as e:
            logger.error("Kuper heavy fatal error: %s", e, exc_info=True)
        finally:
            # Закрываем сессию
            await self._close_session()

        took = round(time.time() - start, 1)
        logger.info("Kuper heavy завершён | товаров=%d | время=%.1fs", len(detailed), took)
        
        return ParseResult(
            task_id=task.task_id,
            service="kuper",
            mode="heavy",
            products=detailed,
            took_seconds=took,
            user_id=task.user_id,
            chat_id=task.chat_id
        )