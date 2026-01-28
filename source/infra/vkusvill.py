
import logging
import asyncio
import time
import os
import pandas as pd
import re
from typing import Optional

import redis.asyncio as redis 
from source.application.parser_interface import BaseParser
from source.core.dto import Task, ParseResult, ProductDetail
from source.core.config import settings
from async_tls_client.session.session import AsyncSession

logger = logging.getLogger("vkusvill_parser")

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

    async def _checkout_proxy(self, r: redis.Redis) -> Optional[str]:
        proxy = await r.lpop(self.PROXY_REDIS_KEY)
        if proxy:
            return proxy.decode()
        return None

    async def _checkin_proxy(self, r: redis.Redis, proxy: str):
        if proxy:
            await r.rpush(self.PROXY_REDIS_KEY, proxy)
            logger.info("Прокси %s возвращен в очередь.", proxy)

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
                resp = await session.get(url, params=params, headers=headers, timeout=timeout)
                status = getattr(resp, "status", None)
                if status and status != 200:
                    raise ValueError(f"HTTP {status}")
                return resp.json()
            except Exception as e:
                if attempt >= retries:
                    logger.warning("Vkusvill request failed: %s", e)
                    return None
                await asyncio.sleep(backoff * (attempt + 1))

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
            
        logger.info(f"ВкусВилл Setup | Geo: {lat},{lon} | Proxy: {proxy if proxy else 'Direct'}")
        
        session = AsyncSession(
            client_identifier="chrome_120",
            random_tls_extension_order=True
        )
        if proxy:
            session.proxies = {"http": proxy, "https": proxy}

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
                'str_par': '{[version]}{[311006]}{[device_model]}{[V2339A]}{[screen_id]}{[ShopAddressesFragmentV2]}{[source]}{[2]}{[device_id]}{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}{[def_Date_service]}{[2024-10-23]}{[def_id_service]}{[3]}{[def_type_service]}{[3]}{[def_gettype]}{[0]}{[def_Number_button]}{[null]}{[def_ShopNo]}{[6516]}{[def_slot_during]}{[null]}{[def_slot_since]}{[18:00:00]}{[def_slot_until]}{[20:00:00]}{[user_number]}{[&]ё4464]}{[ts]}{[1729691867108]}{[method]}{[/api/stores/getNearbyNew/]}',
            }
            resp = await session.get(
                f"{self.BASE_URL}/stores/getNearbyNew/",
                params=params,
                headers=self.HEADERS,
                timeout=settings.HTTP_TIMEOUT_SEC
            )
            result = resp.json().get("stores")[0]
            shopno = result.get("ShopNo")
        
            params = {
                'number': '&]ё4464',
                'shopNo': str(shopno),
                'str_par': '{[version]}{[311006]}{[device_model]}{[V2339A]}{[screen_id]}{[ShopAddressesFragmentV2]}{[source]}{[2]}{[device_id]}{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}{[def_Date_service]}{[2024-10-25]}{[def_id_service]}{[3]}{[def_type_service]}{[3]}{[def_gettype]}{[0]}{[def_Number_button]}{[null]}{[def_ShopNo]}{[7660]}{[def_slot_during]}{[null]}{[def_slot_since]}{[11:00:00]}{[def_slot_until]}{[13:00:00]}{[user_number]}{[&]ё4464]}{[ts]}{[1729704763853]}{[method]}{[/api/takeaway/addPickupAddresses/]}',
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
                'DateSupply': '20241025',
                'number_button_chosen': '1',
                'id_service_chosen': '3',
                'gettype': '0',
                'slot_since': '11:00:00',
                'slot_until': '13:00:00',
                'type_service': '3',
                'price_delivery': '0.0',
                'not_need_slots': '0',
                'package_id': '0',
                'str_par': '{[version]}{[311006]}{[device_model]}{[V2339A]}{[screen_id]}{[AddressesFragmentV2]}{[source]}{[2]}{[device_id]}{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}{[def_Date_service]}{[2024-10-25]}{[def_id_service]}{[3]}{[def_type_service]}{[3]}{[def_gettype]}{[0]}{[def_Number_button]}{[null]}{[def_ShopNo]}{[2284]}{[def_slot_during]}{[null]}{[def_slot_since]}{[10:00:00]}{[def_slot_until]}{[12:00:00]}{[user_number]}{[&]ё4464]}{[ts]}{[1729705163318]}{[method]}{[/api/takeaway/updCartHeader/]}',
            }
            await session.post(
                f"{self.BASE_URL}/takeaway/updCartHeader/",
                json=data,
                headers=self.HEADERS,
                timeout=settings.HTTP_TIMEOUT_SEC
            )
            logger.info("ВкусВилл: гео %s | прокси: %s", city_input, "да" if proxy else "нет")
        except Exception as e:
            logger.error(f"Ошибка установки гео ВкусВилл {city_input}: {e}")

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

        try:
            limit = int(task.limit) if task.limit else 500
        except (TypeError, ValueError):
            limit = 500
        if limit < 1:
            limit = 500
        stop_event = asyncio.Event()

        city = task.city.strip().lower() or "москва"
        session, current_proxy = await self._get_session_for_city(task.city, r)

        heavy_df = None
        if os.path.exists(self.HEAVY_CSV_PATH):
            try:
                heavy_df = pd.read_csv(self.HEAVY_CSV_PATH, sep=";", dtype=str, keep_default_na=False)
                heavy_df["product_id"] = heavy_df["product_id"].str.strip()
                logger.info("Vkusvill fast | кэш загружен: %d товаров", len(heavy_df))
            except Exception as e:
                logger.error("Ошибка чтения heavy CSV: %s", e)

        try:
            params = {
                'screen': 'CatalogMain',
                'number': '&_5>527',
                'offline': '0',
                'all_products': 'false',
                'str_par': '{[version]}{[311006]}{[device_model]}{[V2339A]}{[screen_id]}{[CatalogFragment]}{[source]}{[2]}{[device_id]}{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}{[def_Date_service]}{[2024-10-10]}{[def_id_service]}{[32]}{[def_type_service]}{[1]}{[def_gettype]}{[56]}{[def_Number_button]}{[null]}{[def_ShopNo]}{[6098]}{[def_slot_during]}{[01:00:00]}{[def_slot_since]}{[null]}{[def_slot_until]}{[null]}{[user_number]}{[&_5>527]}{[ts]}{[1728539006506]}{[method]}{[/api/bff/get_screen_widgets]}',
            }

            data = await self._request_json(
                session,
                f"{self.BASE_URL}/bff/get_screen_widgets",
                params=params,
                headers=self.HEADERS,
            )
            widgets = data.get("widgets", []) if isinstance(data, dict) else []

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
                        
                        logger.info(f"Найдена категория: {title} (ID: {cat_id})")
                        tasks.append(
                            self._fetch_category_fast(
                                session,
                                str(cat_id),
                                title,
                                heavy_df,
                                products,
                                limit,
                                stop_event
                            )
                        )
            
            if not tasks:
                 logger.warning("Не найдена категория 'Готовая еда' в виджетах")

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error("Vkusvill fast fatal error: %s", e, exc_info=True)
        finally:
            if current_proxy:
                await self._checkin_proxy(r, current_proxy)

        if limit and len(products) > limit:
            products = products[:limit]

        took = round(time.time() - start, 1)
        logger.info("Vkusvill fast завершён | товаров: %d | время: %.1fс | кэш: %s", len(products), took, "ДА" if heavy_df is not None else "НЕТ")

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
        heavy_df,
        result_list: list,
        max_products: int,
        stop_event: asyncio.Event
    ):
        page_limit = 24
        page = 1
        
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
                ('str_par', '{[version]}{[311006]}{[device_model]}{[V2339A]}{[screen_id]}{[CatalogMainFragment]}{[source]}{[2]}{[device_id]}{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}{[def_Date_service]}{[2024-10-18]}{[def_id_service]}{[32]}{[def_type_service]}{[1]}{[def_gettype]}{[4]}{[def_Number_button]}{[1]}{[def_ShopNo]}{[3700]}{[def_slot_during]}{[01:00:00]}{[def_slot_since]}{[null]}{[def_slot_until]}{[null]}{[user_number]}{[&_5>527]}{[ts]}{[1729253880342]}{[method]}{[/api/bff/get_widget_content]}'),
            ]

            try:
                data = await self._request_json(
                    session,
                    f"{self.BASE_URL}/bff/get_widget_content",
                    params=params,
                    headers=self.HEADERS,
                )

                if not data or not isinstance(data, list):
                    break

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

                    calories = proteins = fats = carbs = ingredients = None
                    if heavy_df is not None:
                        match = heavy_df[heavy_df["product_id"] == pid]
                        if not match.empty:
                            r = match.iloc[0]
                            calories = r.get("calories")
                            proteins = r.get("proteins")
                            fats = r.get("fats")
                            carbs = r.get("carbs")
                            ingredients = r.get("ingredients")
                            name = r.get("name", name)
                            photos = r.get("photos", "").split(" | ") if pd.notna(r.get("photos")) else photos

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
                logger.warning(f"Ошибка страницы {page} (offset {offset}) категории {category}: {e}")
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

        try:
            params = {
                'screen': 'CatalogMain',
                'number': '&_5>527',
                'offline': '0',
                'all_products': 'false',
                'str_par': '{[version]}{[311006]}{[device_model]}{[V2339A]}{[screen_id]}{[CatalogFragment]}{[source]}{[2]}{[device_id]}{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}{[def_Date_service]}{[2024-10-10]}{[def_id_service]}{[32]}{[def_type_service]}{[1]}{[def_gettype]}{[56]}{[def_Number_button]}{[null]}{[def_ShopNo]}{[6098]}{[def_slot_during]}{[01:00:00]}{[def_slot_since]}{[null]}{[def_slot_until]}{[null]}{[user_number]}{[&_5>527]}{[ts]}{[1728539006506]}{[method]}{[/api/bff/get_screen_widgets]}',
            }
            data = await self._request_json(
                session,
                f"{self.BASE_URL}/bff/get_screen_widgets",
                params=params,
                headers=self.HEADERS,
            )
            widgets = data.get("widgets", []) if isinstance(data, dict) else []
            
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
            logger.warning(categories_to_parse)
            for cat_id, title in categories_to_parse:
                limit = 24
                page = 1
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
                        ('str_par', '{[version]}{[311006]}{[device_model]}{[V2339A]}{[screen_id]}{[CatalogMainFragment]}{[source]}{[2]}{[device_id]}{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}{[def_Date_service]}{[2024-10-18]}{[def_id_service]}{[32]}{[def_type_service]}{[1]}{[def_gettype]}{[4]}{[def_Number_button]}{[1]}{[def_ShopNo]}{[3700]}{[def_slot_during]}{[01:00:00]}{[def_slot_since]}{[null]}{[def_slot_until]}{[null]}{[user_number]}{[&_5>527]}{[ts]}{[1729253880342]}{[method]}{[/api/bff/get_widget_content]}'),
                    ]

                    data = await self._request_json(
                        session,
                        f"{self.BASE_URL}/bff/get_widget_content",
                        params=params,
                        headers=self.HEADERS,
                    )

                    if not data:
                        break

                    for item in data:
                        if item.get("type") and item.get("type") != "product":
                            continue

                        pid = str(item["id"])
                        try:
                            params = {
                                'number': '&_5>527',
                                'source': '2',
                                'version': '311006',
                                'product_id': pid,
                                'shopno': '0',
                                'offline': '0',
                                'str_par': '{[version]}{[311006]}{[device_model]}{[V2339A]}{[screen_id]}{[ProductFragment]}{[source]}{[2]}{[device_id]}{[15bad36a-71b8-46d9-9c3a-8aaed80bca46]}{[def_Date_service]}{[2024-10-10]}{[def_id_service]}{[32]}{[def_type_service]}{[1]}{[def_gettype]}{[56]}{[def_Number_button]}{[null]}{[def_ShopNo]}{[6098]}{[def_slot_during]}{[01:00:00]}{[def_slot_since]}{[null]}{[def_slot_until]}{[null]}{[user_number]}{[&_5>527]}{[ts]}{[1728539918115]}{[method]}{[/api/catalog4/product]}',
                                }
                            card_resp = await session.get(
                                f"{self.BASE_URL}/catalog4/product",
                                params=params, 
                                headers=self.HEADERS,
                                timeout=15
                            )
                            if card_resp.status != 200:
                                logger.warning(f"Карточка {pid} вернула {card_resp.status}")
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
            if current_proxy:
                await self._checkin_proxy(r, current_proxy)

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

        return ParseResult(
            task_id=task.task_id,
            service="vkusvill",
            mode="heavy",
            products=detailed,
            took_seconds=round(time.time() - start, 1),
            user_id=task.user_id,
            chat_id=task.chat_id
        )