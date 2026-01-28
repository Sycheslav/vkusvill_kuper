import asyncio
import time
import logging
from datetime import datetime, timezone
import redis.asyncio as redis
from aiogram import Bot

from source.core.config import settings
from source.infra.vkusvill import VkusvillParser
from source.infra.kuper import KuperParser
from source.core.dto import Task, ParseResult

logger = logging.getLogger("redis_worker")
logger.setLevel(logging.INFO)

# Форматирование логов
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
logger.addHandler(handler)

parsers = {"vkusvill": VkusvillParser(), "kuper": KuperParser()}


async def initialize_proxies(r: redis.Redis):
    """Инициализация списка прокси в Redis для VkusVill"""
    key = "vkusvill:proxies:free"
    await r.delete(key)
    proxies = settings.VKUSVILL_PROXY_LIST
    if proxies:
        await r.rpush(key, *proxies)
        logger.info(f"В Redis загружено {len(proxies)} прокси для ВкусВилл")
    else:
        logger.warning("Список прокси пуст! Парсер будет работать с локального IP.")


async def ensure_consumer_group(r: redis.Redis):
    """Создание consumer group если её нет"""
    try:
        await r.xgroup_create(
            settings.INPUT_STREAM,
            settings.PARSER_GROUP,
            id="0",
            mkstream=True
        )
        logger.info(f"Создана consumer group: {settings.PARSER_GROUP}")
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group {settings.PARSER_GROUP} уже существует")
        else:
            raise


async def process_task(task: Task, r: redis.Redis) -> ParseResult:
    """Обработка одной задачи парсинга"""
    logger.info("="*60)
    logger.info("Новая задача | id=%s | %s %s | city=%s | user=%s",
                task.task_id, task.service, task.mode, task.city, task.user_id)

    start = time.time()
    try:
        parser = parsers[task.service]
        
        if task.service == "vkusvill":
            result = await parser.parse(task, redis_client=r)
        else:
            result = await parser.parse(task)

        result.took_seconds = round(time.time() - start, 1)
        result.status = "success"
        
        logger.info("Задача завершена | id=%s | товаров=%d | время=%.1fс",
                    task.task_id, len(result.products), result.took_seconds)
        return result
        
    except Exception as e:
        elapsed = round(time.time() - start, 1)
        logger.error("Ошибка обработки задачи %s: %s", task.task_id, e, exc_info=True)
        
        # Возвращаем результат с ошибкой
        return ParseResult(
            task_id=task.task_id,
            service=task.service,
            mode=task.mode,
            products=[],
            took_seconds=elapsed,
            user_id=task.user_id,
            chat_id=task.chat_id,
            status="error",
            error_message=str(e)
        )


async def update_heartbeat(r: redis.Redis):
    """Обновление heartbeat и статистики"""
    try:
        now = datetime.now(timezone.utc).isoformat()
        await r.set(
            "parser:heartbeat",
            now,
            ex=settings.HEARTBEAT_TTL_SEC
        )
    except Exception as e:
        logger.warning(f"Ошибка обновления heartbeat: {e}")


async def update_stats(r: redis.Redis, success: bool):
    """Обновление статистики"""
    try:
        await r.hincrby("parser:stats", "processed", 1)
        if success:
            await r.hincrby("parser:stats", "success", 1)
        else:
            await r.hincrby("parser:stats", "error", 1)
    except Exception as e:
        logger.warning(f"Ошибка обновления статистики: {e}")


async def trim_streams(r: redis.Redis):
    """Обрезка стримов до максимальной длины"""
    try:
        await r.xtrim(settings.INPUT_STREAM, maxlen=settings.STREAM_MAXLEN)
        await r.xtrim(settings.OUTPUT_STREAM, maxlen=settings.STREAM_MAXLEN)
    except Exception as e:
        logger.warning(f"Ошибка обрезки стримов: {e}")


async def main():
    logger.info("="*60)
    logger.info("Redis Worker запущен")
    logger.info("="*60)
    logger.info(f"INPUT_STREAM: {settings.INPUT_STREAM}")
    logger.info(f"OUTPUT_STREAM: {settings.OUTPUT_STREAM}")
    logger.info(f"PARSER_GROUP: {settings.PARSER_GROUP}")
    logger.info(f"PARSER_CONSUMER: {settings.PARSER_CONSUMER}")
    
    r = redis.from_url(settings.REDIS_URL, decode_responses=False)
    await r.ping()
    logger.info("Подключено к Redis")

    # Инициализация
    await initialize_proxies(r)
    await ensure_consumer_group(r)

    last_heartbeat = time.time()
    last_trim = time.time()
    
    logger.info("Ожидание задач...")

    while True:
        try:
            # Обновляем heartbeat каждые 30 секунд
            if time.time() - last_heartbeat > 30:
                await update_heartbeat(r)
                last_heartbeat = time.time()
            
            # Обрезаем стримы каждые 5 минут
            if time.time() - last_trim > 300:
                await trim_streams(r)
                last_trim = time.time()
            
            # Читаем из стрима через consumer group
            msgs = await r.xreadgroup(
                settings.PARSER_GROUP,
                settings.PARSER_CONSUMER,
                {settings.INPUT_STREAM: ">"},
                count=1,
                block=5000
            )
            
            if not msgs:
                continue

            for stream_name, messages in msgs:
                for msg_id, fields in messages:
                    raw = fields.get(b"data")
                    if not raw:
                        # Пустое сообщение - подтверждаем и пропускаем
                        await r.xack(settings.INPUT_STREAM, settings.PARSER_GROUP, msg_id)
                        continue

                    try:
                        task = Task.model_validate_json(raw.decode())
                    except Exception as e:
                        logger.error(f"Ошибка парсинга задачи: {e}")
                        await r.xack(settings.INPUT_STREAM, settings.PARSER_GROUP, msg_id)
                        continue
                    
                    # Обрабатываем задачу
                    result = await process_task(task, r)
                    
                    # Определяем ключ для записи результата
                    result_key = task.result_key or f"parse:result:{task.task_id}"
                    
                    # Записываем результат в ключ с TTL
                    await r.set(
                        result_key,
                        result.model_dump_json(),
                        ex=settings.RESULT_TTL_SEC
                    )
                    logger.info(f"Результат записан в {result_key}")
                    
                    # Также публикуем в output stream (для мониторинга)
                    await r.xadd(
                        settings.OUTPUT_STREAM,
                        {"data": result.model_dump_json()},
                        maxlen=settings.STREAM_MAXLEN
                    )
                    
                    # Подтверждаем обработку
                    await r.xack(settings.INPUT_STREAM, settings.PARSER_GROUP, msg_id)
                    
                    # Обновляем статистику
                    await update_stats(r, result.status == "success")
                    
                    logger.info(f"Задача {task.task_id} обработана, статус: {result.status}")

        except redis.ConnectionError as e:
            logger.error(f"Потеряно соединение с Redis: {e}")
            await asyncio.sleep(5)
            try:
                r = redis.from_url(settings.REDIS_URL, decode_responses=False)
                await r.ping()
                logger.info("Переподключение к Redis успешно")
            except Exception:
                pass
                
        except Exception as e:
            logger.error("Критическая ошибка в worker: %s", e, exc_info=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
