from pydantic import BaseModel
from typing import List, Optional, Literal

class Geo(BaseModel):
    lat: float
    lon: float

class Task(BaseModel):
    task_id: str
    service: Literal["vkusvill", "kuper"]
    mode: Literal["fast", "heavy"]
    store: Optional[str] = None
    limit: Optional[int] = 500
    city: str = "москва"      
    user_id: int              
    chat_id: int
    result_key: Optional[str] = None  # Ключ для записи результата в Redis

class ProductID(BaseModel):
    product_id: str
    category: str

class ProductDetail(BaseModel):
    product_id: str
    name: str
    price: float
    old_price: Optional[float] = None
    calories: Optional[float] = None
    proteins: Optional[float] = None
    fats: Optional[float] = None
    carbs: Optional[float] = None
    weight: Optional[str] = None
    ingredients: Optional[str] = None
    photos: List[str] = []
    category: str
    store: Optional[str] = None
    in_stock: bool = True
    url: Optional[str] = None  # Ссылка на продукт

class ParseResult(BaseModel):
    task_id: str
    service: str
    mode: str
    products: List[ProductID | ProductDetail]
    took_seconds: float
    user_id: int
    chat_id: int
    status: str = "success"  # "success" | "error"
    error_message: Optional[str] = None