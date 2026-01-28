import logging
from typing import List, Dict, Optional
import asyncio
import time
import random
import os
import pandas as pd

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
    
    session = AsyncSession(
            client_identifier="chrome_120",
            random_tls_extension_order=True
        )
    
    @property
    def heavy_csv_path(self) -> str:
        store = (getattr(self, "current_store", "") or "unknown").lower()
        return f"{settings.DATA_DIR}/kuper_heavy_{store}.csv"

    current_store: str = ""

    async def _get_store_id(self, lat, lon, store_name: str) -> str:
        params = {'shipping_method': 'by_courier', 'lat': str(lat), 'lon': str(lon), 'include_labels_tree': 'true'}
        resp = await self.session.get(f"{self.BASE_URL}/stores", params=params, headers=self.HEADERS)
        stores = resp.json().get("stores", [])
        store_key = store_name.lower()
        for store in stores:
            if store_key in store.get("name", "").lower():
                return (store["id"], store.get("name", ""))
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
    
    async def parse(self, task: Task) -> ParseResult:
        if task.mode == "fast":
            return await self.parse_fast(task)
        elif task.mode == "heavy":
            return await self.parse_heavy(task)
        else:
            raise ValueError(f"Unknown mode: {task.mode}")

    async def parse_fast(self, task: Task) -> ParseResult:
            start = time.time()
            products = []
            self.current_store = (task.store or "лента").lower().strip()
            city = task.city.strip() or "москва"
            city_name, lat, lon = parse_city_or_coords(task.city)
            if lat is not None and lon is not None:
                use_lat, use_lon = lat, lon
            else:
                use_lat, use_lon = await get_coords_by_city(city_name)

            logger.info("%s %s %s", city_name, lat, lon)
            heavy_df = None
            if os.path.exists(self.heavy_csv_path):
                try:
                    heavy_df = pd.read_csv(self.heavy_csv_path, sep=";", dtype=str, keep_default_na=False)
                    heavy_df["sku"] = heavy_df["sku"].str.strip()
                    logger.info("Kuper fast | кэш загружен: %d товаров", len(heavy_df))
                except Exception as e:
                    logger.error("Ошибка чтения кэша: %s", e)

            try:
                result = await self._get_store_id(use_lat, use_lon, self.current_store)
                store_id = result[0]
                self.current_store = result[1]
                taxons = (await self.session.get(f"{self.BASE_URL}/taxons", params={"sid": store_id}, headers=self.HEADERS)).json().get("taxons", [])

                tasks = []
                for taxon in taxons:
                    name = taxon.get("name", "")
                    if not any(kw in name.lower() for kw in ["готовая еда"]):
                        continue
                    tasks.append(self._fetch_fast(
                        store_id=store_id,
                        tid=taxon["id"],
                        category=name,
                        heavy_df=heavy_df,
                        result=products
                    ))

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            except Exception as e:
                logger.error("Kuper fast error: %s", e, exc_info=True)

            took = round(time.time() - start, 1)
            logger.info("Kuper fast | %d товаров | %.1fс | кэш: %s", len(products), took, "ДА" if heavy_df is not None else "НЕТ")

            return ParseResult(
                task_id=task.task_id,
                service="kuper",
                mode="fast",
                products=products,
                took_seconds=took,
                user_id=task.user_id,
                chat_id=task.chat_id
            )

    async def _fetch_fast(self, store_id: str, tid: str, category: str, heavy_df, result: list):
        offset = 0
        while True:
            params = {"sid": store_id, "tid": tid, "limit": "24", "products_offset": str(offset), "sort": "popularity"}
            resp = await self.session.get(f"{self.BASE_URL}/catalog/entities", params=params, headers=self.HEADERS)
            if resp.status != 200 or not resp.json().get("entities"):
                break

            for e in resp.json()["entities"]:

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

                calories = proteins = fats = carbs = ingredients = None
                if heavy_df is not None and sku:
                    match = heavy_df[heavy_df["sku"] == sku]
                    if not match.empty:
                        r = match.iloc[0]
                        calories = r.get("calories")
                        proteins = r.get("proteins")
                        fats = r.get("fats")
                        carbs = r.get("carbs")
                        ingredients = r.get("ingredients")

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
                    store=self.current_store.capitalize(),
                    in_stock=in_stock,
                    url=url
                ))

            offset += 24

    async def parse_heavy(self, task: Task) -> ParseResult:
        self.current_store = (task.store or "лента").lower().strip()
        start = time.time()
        detailed = []

        try:
            city_name, lat, lon = parse_city_or_coords(task.city)
            if lat is not None and lon is not None:
                use_lat, use_lon = lat, lon
            else:
                use_lat, use_lon = await get_coords_by_city(city_name)

            result = await self._get_store_id(use_lat, use_lon, self.current_store)
            store_id = result[0]
            self.current_store = result[1]
            taxons_resp = await self.session.get(f"{self.BASE_URL}/taxons", params={"sid": store_id}, headers=self.HEADERS)
            taxons = taxons_resp.json().get("taxons", [])

            cache_rows = []

            for taxon in taxons:
                cat_name = taxon.get("name", "")
                if not any(kw in cat_name.lower() for kw in ["готовая еда"]):
                    continue

                offset = 0
                while True:
                    entities_resp = await self.session.get(f"{self.BASE_URL}/catalog/entities", params={
                        "sid": store_id,
                        "tid": taxon["id"],
                        "limit": "24",
                        "products_offset": str(offset),
                        "sort": "popularity"
                    }, headers=self.HEADERS)

                    entities = entities_resp.json().get("entities", [])
                    if not entities:
                        break

                    for e in entities:
                        if e.get("type") != "product":
                            continue

                        region_id = str(e["id"])
                        sku = str(e.get("sku") or "")
                        if not sku or sku == "None" or sku == "nan":
                            continue 

                        card_resp = await self.session.get(f"{self.BASE_URL}/multicards/{region_id}", headers=self.HEADERS)
                        if card_resp.status != 200:
                            continue
                        data = card_resp.json().get("product", {})

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
                            store=self.current_store.capitalize(),
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

            if cache_rows:
                cache_df = pd.DataFrame(cache_rows)
                cache_df.drop_duplicates(subset=["sku"], keep="last", inplace=True)
                os.makedirs(settings.DATA_DIR, exist_ok=True)
                cache_df.to_csv(self.heavy_csv_path, sep=";", index=False, encoding="utf-8-sig")
                logger.info("HEAVY кэш сохранён по SKU: %s | %d товаров", self.heavy_csv_path, len(cache_df))

        except Exception as e:
            logger.error("Kuper heavy fatal error: %s", e, exc_info=True)

        took = round(time.time() - start, 1)
        return ParseResult(
            task_id=task.task_id,
            service="kuper",
            mode="heavy",
            products=detailed,
            took_seconds=took,
            user_id=task.user_id,
            chat_id=task.chat_id
        )