
import logging
import asyncio
import time
import os
import pandas as pd
import re
from typing import Optional
from datetime import datetime, date

import redis.asyncio as redis 
from source.application.parser_interface import BaseParser
from source.core.dto import Task, ParseResult, ProductDetail
from source.core.config import settings
from async_tls_client.session.session import AsyncSession

logger = logging.getLogger("vkusvill_parser")


def _get_current_date_str() -> str:
    """Возвращает текущую дату в формате YYYY-MM-DD"""
    return date.today().strftime("%Y-%m-%d")


def _get_current_date_compact() -> str:
    """Возвращает текущую дату в формате YYYYMMDD"""
    return date.today().strftime("%Y%m%d")

class VkusvillParser(BaseParser):
    BASE_URL = "https://mobile.vkusvill.ru/api"
    HEADERS = {
        "X-Vkusvill-Device": "android",
        "X-Vkusvill-Source": "2",
        "X-Vkusvill-Version": "3.11.6 (311006)",
        'X-Vkusvill-Token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJjYXJkIjoiJl81PjUyNyIsInZlcnNpb24iOjJ9.25OjbDKw9yl2NukPuJGQbSNSbzJkSy_tPPDD1VXgep0ckYIPSoBzuu_rDVZHjN2SRC4UkNxkQLlOieUPZ3od3XTef8oI6xViaS9hEH534KubvnFAhZZdcshLm2imk90hyLHnUh1LXF7rNjeppC8diGZVNgobIMioyba0-g7qe1k',
        "User-Agent": "vkusvill/3.11.6 (Android; 28)",
        "Accept": "application/json",
        "Connection": "keep-alive",
        "X-Vkusvill-Model": "vivo V2339A", 
        "Accept-Encoding": "gzip, deflate" 
    }

    HEAVY_CSV_PATH = f"{settings.DATA_DIR}/vkusvill_heavy.csv"

    PROXY_REDIS_KEY = "vkusvill:proxies:free" 

    def _mask_proxy(self, proxy: Optional[str]) -> str:
        if not proxy:
            return "none"
        try:
            scheme, rest = proxy.split("://", 1)
            if "@" in rest:
                _, host = rest.split("@", 1)
                return f"{scheme}://***:***@{host}"
            return f"{scheme}://{rest}"
        except Exception:
            return "<masked>"

    def _summarize_params(self, params):
        if not params:
            return {}
        keys = {"screen", "object_id", "offset", "limit", "sort_id", "data_source"}
        summary = {}
        if isinstance(params, dict):
            for k in keys:
                if k in params:
                    summary[k] = params.get(k)
        elif isinstance(params, list):
            for k, v in params:
                if k in keys:
                    summary[k] = v
        return summary

    async def _checkout_proxy(self, r: redis.Redis) -> Optional[str]:
        proxy = await r.lpop(self.PROXY_REDIS_KEY)
        if proxy:
            return proxy.decode()
        return None

    async def _checkin_proxy(self, r: redis.Redis, proxy: str):
        if proxy:
            await r.rpush(self.PROXY_REDIS_KEY, proxy)
            logger.info("Прокси %s возвращен в очередь.", self._mask_proxy(proxy))

    async def _request_json(
        self,
        session: AsyncSession,
        url: str,
        *,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[int] = None,
    ):
        timeout = timeout or settings.HTTP_TIMEOUT_SEC
        retries = max(settings.HTTP_RETRIES, 0)
        backoff = max(settings.HTTP_RETRY_BACKOFF_SEC, 0)

        for attempt in range(retries + 1):
            try:
                started = time.monotonic()
                logger.info(
                    "VV HTTP GET start | url=%s | attempt=%s/%s | timeout=%ss | params=%s",
                    url.replace(self.BASE_URL, ""),
                    attempt + 1,
                    retries + 1,
                    timeout,
                    self._summarize_params(params),
                )
                resp = await session.get(url, params=params, headers=headers, timeout=timeout)
                status = getattr(resp, "status", None)
                if status and status != 200:
                    raise ValueError(f"HTTP {status}")
                elapsed = time.monotonic() - started
                logger.info(
                    "VV HTTP GET ok | url=%s | status=%s | elapsed=%.2fs",
                    url.replace(self.BASE_URL, ""),
                    status,
                    elapsed,
                )
                return resp.json()
            except Exception as e:
                elapsed = time.monotonic() - started
                if attempt >= retries:
                    logger.warning(
                        "VV HTTP GET failed | url=%s | attempt=%s/%s | elapsed=%.2fs | error=%s",
                        url.replace(self.BASE_URL, ""),
                        attempt + 1,
                        retries + 1,
                        elapsed,
                        e,
                    )
                    return None
                logger.info(
                    "VV HTTP GET retry | url=%s | attempt=%s/%s | elapsed=%.2fs | error=%s",
                    url.replace(self.BASE_URL, ""),
                    attempt + 1,
                    retries + 1,
                    elapsed,
                    e,
                )
                await asyncio.sleep(backoff * (attempt + 1))

    async def _close_session(self, session: Optional[AsyncSession]):
        """Закрыть сессию безопасно"""
        if session:
            try:
                await session.close()
                logger.debug("VkusVill сессия закрыта")
            except Exception as e:
                logger.warning("Ошибка закрытия сессии VkusVill: %s", e)

    async def _get_session_for_city(self, city_input: str, r: redis.Redis) -> tuple[AsyncSession, Optional[str]]:
        key = city_input.strip().lower()
        lat, lon = None, None
        proxy = None

        coord_match = re.match(r"^([-\d.]+)[\s,]+([-\d.]+)$", key)
        if coord_match:
            lat = float(coord_match.group(1))
            lon = float(coord_match.group(2))
        elif key in settings.VKUSVILL_CITY_COORDS:
            lat, lon = settings.VKUSVILL_CITY_COORDS[key]
        
        if lat is None or lon is None:
            default_coords = settings.VKUSVILL_CITY_COORDS.get("москва", (55.7558, 37.6173))
            logger.warning("Не удалось определить координаты для %s, используем дефолтные: %s", city_input, default_coords)
            lat, lon = default_coords

        if settings.VKUSVILL_PROXY_LIST:
            proxy = await self._checkout_proxy(r)
            if not proxy:
                logger.warning("Нет свободных прокси в очереди. Используется прямое подключение (IP может быть занят).")
        logger.info(
            "ВкусВилл Setup | city=%s | geo=%s,%s | proxy=%s",
            city_input,
            lat,
            lon,
            self._mask_proxy(proxy),
        )
        
        session = AsyncSession(
            client_identifier="chrome_120",
            random_tls_extension_order=True
        )
        if proxy:
            session.proxies = {"http": proxy, "https": proxy}

        # Используем динамические даты
        current_date = _get_current_date_str()
        current_date_compact = _get_current_date_compact()
        ts = str(int(time.time() * 1000))

        try:
            params = {
                'number': '&]ё4464',
                'shirota': lat,
                'dolgota': lon,
                'max_distance': '5875',
                'kids_room': '0',
                'project': '0',
                'with_takeaway': '1',
                'with_fresh_juice': '0',
                'with_coffee': '0',
                'with_bakery': '0',
                'shop_status': '0',
                'with_job_interview': '0',
                'with_pandomat': '0',
                'with_butcher': '0',
                'with_cafe': '0',
                'with_goodcaps': '0',
                'with_cardscollect': '0',
                'nopackage': '0',
                'with_cashpoint': '0',
                'fishShowcase': '0',
                'giveFood': '0',
                'with_ice': '0',
                'with_wine': '0',
                'with_help_animals': '0',
                'str_par': f'{{[version]}}{{[311006]}}{{[device_model]}}{{[V2339A]}}{{[screen_id]}}{{[ShopAddressesFragmentV2]}}{{[source]}}{{[2]}}{{[device_id]}}{{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}}{{[def_Date_service]}}{{[{current_date}]}}{{[def_id_service]}}{{[3]}}{{[def_type_service]}}{{[3]}}{{[def_gettype]}}{{[0]}}{{[def_Number_button]}}{{[null]}}{{[def_ShopNo]}}{{[6516]}}{{[def_slot_during]}}{{[null]}}{{[def_slot_since]}}{{[18:00:00]}}{{[def_slot_until]}}{{[20:00:00]}}{{[user_number]}}{{[&]ё4464]}}{{[ts]}}{{[{ts}]}}{{[method]}}{{[/api/stores/getNearbyNew/]}}',
            }
            logger.info("VkusVill: запрос ближайших магазинов...")
            resp = await session.get(
                f"{self.BASE_URL}/stores/getNearbyNew/",
                params=params,
                headers=self.HEADERS,
                timeout=settings.HTTP_TIMEOUT_SEC
            )
            stores_data = resp.json()
            stores = stores_data.get("stores", [])
            if not stores:
                logger.error("VkusVill: не найдены магазины для координат %s, %s", lat, lon)
                return session, proxy
            
            result = stores[0]
            shopno = result.get("ShopNo")
            logger.info("VkusVill: найден магазин ShopNo=%s", shopno)
        
            params = {
                'number': '&]ё4464',
                'shopNo': str(shopno),
                'str_par': f'{{[version]}}{{[311006]}}{{[device_model]}}{{[V2339A]}}{{[screen_id]}}{{[ShopAddressesFragmentV2]}}{{[source]}}{{[2]}}{{[device_id]}}{{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}}{{[def_Date_service]}}{{[{current_date}]}}{{[def_id_service]}}{{[3]}}{{[def_type_service]}}{{[3]}}{{[def_gettype]}}{{[0]}}{{[def_Number_button]}}{{[null]}}{{[def_ShopNo]}}{{[{shopno}]}}{{[def_slot_during]}}{{[null]}}{{[def_slot_since]}}{{[11:00:00]}}{{[def_slot_until]}}{{[13:00:00]}}{{[user_number]}}{{[&]ё4464]}}{{[ts]}}{{[{ts}]}}{{[method]}}{{[/api/takeaway/addPickupAddresses/]}}',
            }

            await session.get(
                f"{self.BASE_URL}/takeaway/addPickupAddresses/",
                params=params,
                headers=self.HEADERS,
                timeout=settings.HTTP_TIMEOUT_SEC
            )
            data = {
                'number': '&]ё4464',
                'shopNo': str(shopno),
                'DateSupply': current_date_compact,
                'number_button_chosen': '1',
                'id_service_chosen': '3',
                'gettype': '0',
                'slot_since': '11:00:00',
                'slot_until': '13:00:00',
                'type_service': '3',
                'price_delivery': '0.0',
                'not_need_slots': '0',
                'package_id': '0',
                'str_par': f'{{[version]}}{{[311006]}}{{[device_model]}}{{[V2339A]}}{{[screen_id]}}{{[AddressesFragmentV2]}}{{[source]}}{{[2]}}{{[device_id]}}{{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}}{{[def_Date_service]}}{{[{current_date}]}}{{[def_id_service]}}{{[3]}}{{[def_type_service]}}{{[3]}}{{[def_gettype]}}{{[0]}}{{[def_Number_button]}}{{[null]}}{{[def_ShopNo]}}{{[{shopno}]}}{{[def_slot_during]}}{{[null]}}{{[def_slot_since]}}{{[10:00:00]}}{{[def_slot_until]}}{{[12:00:00]}}{{[user_number]}}{{[&]ё4464]}}{{[ts]}}{{[{ts}]}}{{[method]}}{{[/api/takeaway/updCartHeader/]}}',
            }
            await session.post(
                f"{self.BASE_URL}/takeaway/updCartHeader/",
                json=data,
                headers=self.HEADERS,
                timeout=settings.HTTP_TIMEOUT_SEC
            )
            logger.info("ВкусВилл: гео настроено | city=%s | shopno=%s | proxy=%s", city_input, shopno, self._mask_proxy(proxy))
        except Exception as e:
            logger.error("Ошибка установки гео ВкусВилл %s: %s", city_input, e, exc_info=True)

        return session, proxy
    
    async def parse(self, task: Task, redis_client: redis.Redis = None) -> ParseResult:
        if not redis_client:
            raise ValueError("Redis client is required for Vkusvill parser")

        if task.mode == "fast":
            return await self.parse_fast(task, redis_client)
        elif task.mode == "heavy":
            return await self.parse_heavy(task, redis_client)
        else:
            raise ValueError(f"Unknown mode {task.mode}")

        

    async def parse_fast(self, task: Task, r: redis.Redis) -> ParseResult:
        start = time.time()
        products = []
        session = None
        current_proxy = None

        logger.info("Vkusvill fast start | task_id=%s | city=%s | limit=%s", task.task_id, task.city, task.limit)
        try:
            limit = int(task.limit) if task.limit else 500
        except (TypeError, ValueError):
            limit = 500
        if limit < 1:
            limit = 500
        stop_event = asyncio.Event()

        city = task.city.strip().lower() or "москва"
        session, current_proxy = await self._get_session_for_city(task.city, r)
        logger.info("Vkusvill fast | лимит=%s | прокси=%s", limit, self._mask_proxy(current_proxy))

        # Загрузка кэша с конвертацией в dict для O(1) lookup
        heavy_dict = None
        if os.path.exists(self.HEAVY_CSV_PATH):
            try:
                heavy_df = pd.read_csv(self.HEAVY_CSV_PATH, sep=";", dtype=str, keep_default_na=False)
                heavy_df["product_id"] = heavy_df["product_id"].str.strip()
                heavy_dict = heavy_df.set_index("product_id").to_dict("index")
                logger.info("Vkusvill fast | кэш загружен: %d товаров", len(heavy_dict))
            except Exception as e:
                logger.error("Ошибка чтения heavy CSV: %s", e)

        # Динамические параметры
        current_date = _get_current_date_str()
        ts = str(int(time.time() * 1000))

        try:
            params = {
                'screen': 'CatalogMain',
                'number': '&_5>527',
                'offline': '0',
                'all_products': 'false',
                'str_par': f'{{[version]}}{{[311006]}}{{[device_model]}}{{[V2339A]}}{{[screen_id]}}{{[CatalogFragment]}}{{[source]}}{{[2]}}{{[device_id]}}{{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}}{{[def_Date_service]}}{{[{current_date}]}}{{[def_id_service]}}{{[32]}}{{[def_type_service]}}{{[1]}}{{[def_gettype]}}{{[56]}}{{[def_Number_button]}}{{[null]}}{{[def_ShopNo]}}{{[6098]}}{{[def_slot_during]}}{{[01:00:00]}}{{[def_slot_since]}}{{[null]}}{{[def_slot_until]}}{{[null]}}{{[user_number]}}{{[&_5>527]}}{{[ts]}}{{[{ts}]}}{{[method]}}{{[/api/bff/get_screen_widgets]}}',
            }

            logger.info("Vkusvill fast | запрашиваем widgets для каталога")
            data = await self._request_json(
                session,
                f"{self.BASE_URL}/bff/get_screen_widgets",
                params=params,
                headers=self.HEADERS,
            )
            widgets = data.get("widgets", []) if isinstance(data, dict) else []
            logger.info("Vkusvill fast | widgets получено: %d", len(widgets))

            tasks = []
            
            for widget in widgets:
                content_items = widget.get("content", [])
                
                if not content_items or not isinstance(content_items, list):
                    continue

                for item in content_items:
                    title = item.get("title", "").lower()
                    
                    if "готовая еда" in title:
                        cat_id = item.get("object_id")
                        if not cat_id:
                            continue
                        
                        logger.info("Vkusvill fast | категория: %s | id=%s", title, cat_id)
                        tasks.append(
                            self._fetch_category_fast(
                                session,
                                str(cat_id),
                                title,
                                heavy_dict,
                                products,
                                limit,
                                stop_event
                            )
                        )
            
            if not tasks:
                logger.warning("Vkusvill fast | не найдена категория 'Готовая еда' в виджетах")

            if tasks:
                logger.info("Vkusvill fast | задач для категорий: %d", len(tasks))
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error("Vkusvill fast fatal error: %s", e, exc_info=True)
        finally:
            # Возвращаем прокси в очередь
            if current_proxy:
                await self._checkin_proxy(r, current_proxy)
            # Закрываем сессию
            await self._close_session(session)

        if limit and len(products) > limit:
            products = products[:limit]

        took = round(time.time() - start, 1)
        logger.info("Vkusvill fast завершён | товаров: %d | время: %.1fс | кэш: %s", len(products), took, "ДА" if heavy_dict is not None else "НЕТ")

        return ParseResult(
            task_id=task.task_id,
            service="vkusvill",
            mode="fast",
            products=products,
            took_seconds=took,
            user_id=task.user_id,
            chat_id=task.chat_id
        )

    async def _fetch_category_fast(
        self,
        session,
        cat_id: str,
        category: str,
        heavy_dict: Optional[dict],
        result_list: list,
        max_products: int,
        stop_event: asyncio.Event
    ):
        page_limit = 24
        page = 1
        logger.info("Vkusvill fast | старт категории=%s id=%s", category, cat_id)
        
        # Динамические параметры
        current_date = _get_current_date_str()
        ts = str(int(time.time() * 1000))
        
        while True:
            if stop_event.is_set():
                break
            if max_products and len(result_list) >= max_products:
                stop_event.set()
                break

            offset = (page - 1) * page_limit
            
            params = [
                ('all_products', 'true'),
                ('data_source', 'Category'),
                ('object_id', str(cat_id)),
                ('number', '&_5>527'),
                ('sort_id', '7'),
                ('offset', str(offset)),
                ('limit', str(page_limit)),
                ('offline', '0'),
                ('all_products', 'false'),
                ('str_par', f'{{[version]}}{{[311006]}}{{[device_model]}}{{[V2339A]}}{{[screen_id]}}{{[CatalogMainFragment]}}{{[source]}}{{[2]}}{{[device_id]}}{{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}}{{[def_Date_service]}}{{[{current_date}]}}{{[def_id_service]}}{{[32]}}{{[def_type_service]}}{{[1]}}{{[def_gettype]}}{{[4]}}{{[def_Number_button]}}{{[1]}}{{[def_ShopNo]}}{{[3700]}}{{[def_slot_during]}}{{[01:00:00]}}{{[def_slot_since]}}{{[null]}}{{[def_slot_until]}}{{[null]}}{{[user_number]}}{{[&_5>527]}}{{[ts]}}{{[{ts}]}}{{[method]}}{{[/api/bff/get_widget_content]}}'),
            ]

            try:
                logger.info(
                    "Vkusvill fast | category=%s | page=%s | offset=%s",
                    category,
                    page,
                    offset,
                )
                data = await self._request_json(
                    session,
                    f"{self.BASE_URL}/bff/get_widget_content",
                    params=params,
                    headers=self.HEADERS,
                )

                if not data or not isinstance(data, list):
                    logger.info("Vkusvill fast | category=%s | нет больше данных", category)
                    break
                logger.info(
                    "Vkusvill fast | category=%s | items=%d",
                    category,
                    len(data),
                )

                for item in data:
                    if stop_event.is_set():
                        break
                    pid = str(item["id"])
                    name = item.get("title", "Без названия")
                    
                    price_obj = item.get("price", {})
                    current_price_cents = price_obj.get("discount_price") or price_obj.get("price", 0)
                    base_price_cents = price_obj.get("price", 0)
                    price = current_price_cents
                    old_price = base_price_cents if base_price_cents > current_price_cents else None
                    weight = item.get("weight_str")
                    url = self._extract_url(item)
                    amount = item.get("amount", 0) or item.get("amount_express", 0) 
                    in_stock = bool(amount > 0)

                    photos = []
                    images_list = item.get("images", [])
                    large_images_obj = next((img_obj for img_obj in images_list if img_obj.get("type") == "Large"), None)
                    if large_images_obj:
                        photos = [img.get("url", "") for img in large_images_obj.get("images", []) if img.get("url")]

                    # O(1) lookup в dict вместо O(n) в DataFrame
                    calories = proteins = fats = carbs = ingredients = None
                    if heavy_dict and pid in heavy_dict:
                        cached = heavy_dict[pid]
                        calories = cached.get("calories")
                        proteins = cached.get("proteins")
                        fats = cached.get("fats")
                        carbs = cached.get("carbs")
                        ingredients = cached.get("ingredients")
                        name = cached.get("name") or name
                        cached_photos = cached.get("photos", "")
                        if cached_photos and isinstance(cached_photos, str):
                            photos = cached_photos.split(" | ")

                    result_list.append(ProductDetail(
                        product_id=pid,
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
                        store="ВкусВилл",
                        in_stock=in_stock,
                        url=url
                    ))

                    if max_products and len(result_list) >= max_products:
                        stop_event.set()
                        break

                page += 1

            except Exception as e:
                logger.warning("Ошибка страницы %d (offset %d) категории %s: %s", page, offset, category, e)
                break
    
    def _parse_nutrient_value(self, match) -> Optional[float]:
        if not match:
            return None
        value = match.group(1).strip()
        value = re.sub(r"[^\d.,]", "", value)  
        value = value.replace(",", ".")   
        if value.endswith("."):
            value = value[:-1]
        if value.startswith("."):
            value = "0" + value
        try:
            return float(value) if value else None
        except ValueError:
            return None

    def _extract_url(self, obj: dict) -> Optional[str]:
        """Пытается извлечь URL товара из ответа API."""
        if not isinstance(obj, dict):
            return None
        for key in ("url", "link", "product_url", "web_url", "share_url"):
            value = obj.get(key)
            if value:
                return value
        slug = obj.get("slug") or obj.get("code") or obj.get("product_slug")
        if slug:
            return f"https://vkusvill.ru/goods/{slug}.html"
        return None

    async def parse_heavy(self, task: Task, r: redis.Redis) -> ParseResult:
        start = time.time()
        detailed = []
        session = None
        current_proxy = None

        session, current_proxy = await self._get_session_for_city(task.city, r)
        
        # Динамические параметры
        current_date = _get_current_date_str()
        ts = str(int(time.time() * 1000))

        try:
            params = {
                'screen': 'CatalogMain',
                'number': '&_5>527',
                'offline': '0',
                'all_products': 'false',
                'str_par': f'{{[version]}}{{[311006]}}{{[device_model]}}{{[V2339A]}}{{[screen_id]}}{{[CatalogFragment]}}{{[source]}}{{[2]}}{{[device_id]}}{{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}}{{[def_Date_service]}}{{[{current_date}]}}{{[def_id_service]}}{{[32]}}{{[def_type_service]}}{{[1]}}{{[def_gettype]}}{{[56]}}{{[def_Number_button]}}{{[null]}}{{[def_ShopNo]}}{{[6098]}}{{[def_slot_during]}}{{[01:00:00]}}{{[def_slot_since]}}{{[null]}}{{[def_slot_until]}}{{[null]}}{{[user_number]}}{{[&_5>527]}}{{[ts]}}{{[{ts}]}}{{[method]}}{{[/api/bff/get_screen_widgets]}}',
            }
            logger.info("Vkusvill heavy | запрашиваем widgets для каталога")
            data = await self._request_json(
                session,
                f"{self.BASE_URL}/bff/get_screen_widgets",
                params=params,
                headers=self.HEADERS,
            )
            widgets = data.get("widgets", []) if isinstance(data, dict) else []
            logger.info("Vkusvill heavy | widgets: %d", len(widgets))
            
            categories_to_parse = []
            for widget in widgets:
                content_items = widget.get("content", [])
                if not content_items or not isinstance(content_items, list):
                    continue
                
                for item in content_items:
                    title = item.get("title", "").lower()
                    if title and "готовая еда" in title:
                        cat_id = item.get("object_id")
                        if cat_id:
                            categories_to_parse.append((str(cat_id), title))

            if not categories_to_parse:
                logger.warning("Не найдена категория 'Готовая еда' для heavy парсинга.")
            else:
                logger.info("Vkusvill heavy | найдено категорий: %d", len(categories_to_parse))
                
            for cat_id, title in categories_to_parse:
                limit = 24
                page = 1
                logger.info("Vkusvill heavy | парсим категорию: %s (id=%s)", title, cat_id)
                while True:
                    offset = (page - 1) * limit
                    
                    params = [
                        ('all_products', 'true'),
                        ('data_source', 'Category'),
                        ('object_id', str(cat_id)),
                        ('number', '&_5>527'),
                        ('sort_id', '7'),
                        ('offset', str(offset)),
                        ('limit', str(limit)),
                        ('offline', '0'),
                        ('all_products', 'false'),
                        ('str_par', f'{{[version]}}{{[311006]}}{{[device_model]}}{{[V2339A]}}{{[screen_id]}}{{[CatalogMainFragment]}}{{[source]}}{{[2]}}{{[device_id]}}{{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}}{{[def_Date_service]}}{{[{current_date}]}}{{[def_id_service]}}{{[32]}}{{[def_type_service]}}{{[1]}}{{[def_gettype]}}{{[4]}}{{[def_Number_button]}}{{[1]}}{{[def_ShopNo]}}{{[3700]}}{{[def_slot_during]}}{{[01:00:00]}}{{[def_slot_since]}}{{[null]}}{{[def_slot_until]}}{{[null]}}{{[user_number]}}{{[&_5>527]}}{{[ts]}}{{[{ts}]}}{{[method]}}{{[/api/bff/get_widget_content]}}'),
                    ]

                    data = await self._request_json(
                        session,
                        f"{self.BASE_URL}/bff/get_widget_content",
                        params=params,
                        headers=self.HEADERS,
                    )

                    if not data:
                        break

                    logger.info("Vkusvill heavy | category=%s | page=%d | items=%d", title, page, len(data))

                    for item in data:
                        if item.get("type") and item.get("type") != "product":
                            continue

                        pid = str(item["id"])
                        try:
                            card_params = {
                                'number': '&_5>527',
                                'source': '2',
                                'version': '311006',
                                'product_id': pid,
                                'shopno': '0',
                                'offline': '0',
                                'str_par': f'{{[version]}}{{[311006]}}{{[device_model]}}{{[V2339A]}}{{[screen_id]}}{{[ProductFragment]}}{{[source]}}{{[2]}}{{[device_id]}}{{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}}{{[def_Date_service]}}{{[{current_date}]}}{{[def_id_service]}}{{[32]}}{{[def_type_service]}}{{[1]}}{{[def_gettype]}}{{[56]}}{{[def_Number_button]}}{{[null]}}{{[def_ShopNo]}}{{[6098]}}{{[def_slot_during]}}{{[01:00:00]}}{{[def_slot_since]}}{{[null]}}{{[def_slot_until]}}{{[null]}}{{[user_number]}}{{[&_5>527]}}{{[ts]}}{{[{ts}]}}{{[method]}}{{[/api/catalog4/product]}}',
                                }
                            card_resp = await session.get(
                                f"{self.BASE_URL}/catalog4/product",
                                params=card_params, 
                                headers=self.HEADERS,
                                timeout=15
                            )
                            if card_resp.status != 200:
                                logger.warning("Карточка %s вернула %s", pid, card_resp.status)
                                continue

                            pr = card_resp.json() 

                            calories = proteins = fats = carbs = None
                            for prop in pr.get("properties", []):
                                if prop.get("property_name") == "Пищевая и энергетическая ценность в 100 г":
                                    text = prop.get("property_value", "")
                                    proteins = self._parse_nutrient_value(re.search(r"белки?\s*([\d.,]+)", text, re.I))
                                    fats     = self._parse_nutrient_value(re.search(r"жиры?\s*([\d.,]+)", text, re.I))
                                    carbs    = self._parse_nutrient_value(re.search(r"углеводы?\s*([\d.,]+)", text, re.I))
                                    calories = self._parse_nutrient_value(re.search(r"(\d+)\s*ккал", text, re.I))
                                    break

                            ingredients = next(
                                (p["property_value"] for p in pr.get("properties", []) if p.get("property_name") == "Состав"),
                                None
                            )

                            photos = []
                            for block in pr.get("images", []):
                                if block.get("type") == "Large":
                                    for img in block.get("images", []):
                                        if url := img.get("url"):
                                            photos.append(url)
                                    break  

                            weight = pr.get("weight_str") or f"{pr.get('weight_kg', 0) * 1000:.0f} г"
                            amount = pr.get("amount", 0) or pr.get("amount_express", 0)
                            in_stock = bool(amount > 0)
                            url = self._extract_url(pr)

                            price_obj = pr.get("price", {})
                            current_price_cents = price_obj.get("discount_price") or price_obj.get("price", 0)
                            base_price_cents = price_obj.get("price", 0)
                            price = current_price_cents
                            old_price = base_price_cents if base_price_cents > current_price_cents else None

                            detailed.append(ProductDetail(
                                product_id=pid,
                                name=pr.get("title", ""),
                                price=price,
                                old_price=old_price,
                                calories=calories,
                                proteins=proteins,
                                fats=fats,
                                carbs=carbs,
                                weight=weight,
                                ingredients=ingredients,
                                photos=photos[:10], 
                                category=title,
                                store="ВкусВилл",
                                in_stock=in_stock,
                                url=url
                            ))


                        except Exception as e:
                            logger.warning(f"Ошибка парсинга карточки {pid}: {e}")

                    page += 1

        except Exception as e:
            logger.error("Vkusvill heavy fatal error: %s", e, exc_info=True)
        finally:
            # Возвращаем прокси в очередь
            if current_proxy:
                await self._checkin_proxy(r, current_proxy)
            # Закрываем сессию
            await self._close_session(session)

        if detailed:
            df = pd.DataFrame([{
                "product_id": p.product_id,
                "name": p.name,
                "price": p.price,
                "old_price": p.old_price,
                "calories": p.calories,
                "proteins": p.proteins,
                "fats": p.fats,
                "carbs": p.carbs,
                "weight": p.weight,
                "ingredients": p.ingredients,
                "photos": " | ".join(p.photos[:5]),
                "category": p.category
            } for p in detailed])

            os.makedirs(settings.DATA_DIR, exist_ok=True)
            df.to_csv(self.HEAVY_CSV_PATH, sep=";", index=False, encoding="utf-8-sig")
            logger.info("Vkusvill HEAVY кэш сохранён: %d товаров", len(df))

        took = round(time.time() - start, 1)
        logger.info("Vkusvill heavy завершён | товаров=%d | время=%.1fs", len(detailed), took)

        return ParseResult(
            task_id=task.task_id,
            service="vkusvill",
            mode="heavy",
            products=detailed,
            took_seconds=took,
            user_id=task.user_id,
            chat_id=task.chat_id
        )