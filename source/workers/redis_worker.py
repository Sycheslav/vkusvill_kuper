import asyncio
import time
import logging
import signal
import sys
from datetime import datetime, timezone
import redis.asyncio as redis
from aiogram import Bot

from source.core.config import settings
from source.infra.vkusvill import VkusvillParser
from source.infra.kuper import KuperParser
from source.core.dto import Task, ParseResult

# Настройка ROOT логгера для всех модулей
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
    force=True  # Перезаписать существующую конфигурацию
)

# Установить уровень для всех логгеров парсера
for logger_name in ["redis_worker", "vkusvill_parser", "kuper_parser", "source.infra.geo"]:
    logging.getLogger(logger_name).setLevel(logging.INFO)

logger = logging.getLogger("redis_worker")

# Глобальный флаг для graceful shutdown
shutdown_event = asyncio.Event()

parsers = {"vkusvill": VkusvillParser(), "kuper": KuperParser()}


def _safe_decode(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _decode_obj(obj):
    if isinstance(obj, dict):
        return {_safe_decode(k): _decode_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decode_obj(x) for x in obj]
    if isinstance(obj, tuple):
        return tuple(_decode_obj(x) for x in obj)
    return _safe_decode(obj)


async def log_stream_info(r: redis.Redis, when: str):
    try:
        info = await r.xinfo_stream(settings.INPUT_STREAM)
        info = _decode_obj(info)
        logger.info(
            "XINFO STREAM (%s): length=%s last_id=%s",
            when,
            info.get("length"),
            info.get("last-generated-id"),
        )
    except Exception as e:
        logger.debug("Не удалось получить XINFO STREAM (%s): %s", when, e)

    try:
        groups = await r.xinfo_groups(settings.INPUT_STREAM)
        groups = _decode_obj(groups)
        if not groups:
            logger.info("XINFO GROUPS (%s): нет групп", when)
        else:
            for group in groups:
                logger.info(
                    "XINFO GROUP (%s): name=%s pending=%s consumers=%s last_id=%s",
                    when,
                    group.get("name"),
                    group.get("pending"),
                    group.get("consumers"),
                    group.get("last-delivered-id"),
                )
    except Exception as e:
        logger.debug("Не удалось получить XINFO GROUPS (%s): %s", when, e)

    try:
        pending = await r.xpending(settings.INPUT_STREAM, settings.PARSER_GROUP)
        pending = _decode_obj(pending)
        logger.info(
            "XPENDING (%s): pending=%s min_id=%s max_id=%s consumers=%s",
            when,
            pending.get("pending"),
            pending.get("min"),
            pending.get("max"),
            pending.get("consumers"),
        )
    except Exception as e:
        logger.debug("Не удалось получить XPENDING (%s): %s", when, e)


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


# Максимальное время на одну задачу (5 минут)
MAX_TASK_TIMEOUT_SEC = 300


async def _do_parse(task: Task, r: redis.Redis) -> ParseResult:
    """Внутренняя функция парсинга"""
    parser = parsers[task.service]
    logger.info("Выбран парсер: %s", task.service)
    
    # Оба парсера теперь принимают redis_client для кэширования геокодинга
    return await parser.parse(task, redis_client=r)


async def process_task(task: Task, r: redis.Redis) -> ParseResult:
    """Обработка одной задачи парсинга с timeout"""
    logger.info("=" * 60)
    logger.info(
        "Новая задача | id=%s | service=%s | mode=%s | city=%s | store=%s | limit=%s | user=%s | chat=%s",
        task.task_id,
        task.service,
        task.mode,
        task.city,
        task.store,
        task.limit,
        task.user_id,
        task.chat_id,
    )

    start = time.time()
    parse_started = time.monotonic()
    try:
        # Добавляем timeout на всю задачу
        result = await asyncio.wait_for(
            _do_parse(task, r),
            timeout=MAX_TASK_TIMEOUT_SEC
        )

        result.took_seconds = round(time.time() - start, 1)
        result.status = "success"
        parse_elapsed = time.monotonic() - parse_started
        
        logger.info(
            "Задача завершена | id=%s | товаров=%d | время=%.1fс | parse_elapsed=%.2fs",
            task.task_id,
            len(result.products),
            result.took_seconds,
            parse_elapsed,
        )
        return result
    
    except asyncio.TimeoutError:
        elapsed = round(time.time() - start, 1)
        logger.error(
            "TIMEOUT задачи %s после %ds (лимит: %ds)",
            task.task_id, elapsed, MAX_TASK_TIMEOUT_SEC
        )
        return ParseResult(
            task_id=task.task_id,
            service=task.service,
            mode=task.mode,
            products=[],
            took_seconds=elapsed,
            user_id=task.user_id,
            chat_id=task.chat_id,
            status="error",
            error_message=f"Timeout after {elapsed}s"
        )
        
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


def setup_signal_handlers():
    """Настройка обработчиков сигналов для graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info("Получен сигнал %s, инициируем graceful shutdown...", signum)
        shutdown_event.set()
    
    # На Windows signal.SIGTERM может не работать, используем SIGINT
    signal.signal(signal.SIGINT, signal_handler)
    try:
        signal.signal(signal.SIGTERM, signal_handler)
    except (ValueError, OSError):
        # SIGTERM не поддерживается на Windows в asyncio
        pass


async def main():
    # Выводим сразу при старте
    print("=" * 60, flush=True)
    print("Parser Worker Starting...", flush=True)
    print("=" * 60, flush=True)
    
    logger.info("=" * 60)
    logger.info("Redis Worker запущен")
    logger.info("=" * 60)
    logger.info("REDIS_URL: %s", settings.REDIS_URL[:50] + "..." if len(settings.REDIS_URL) > 50 else settings.REDIS_URL)
    logger.info("INPUT_STREAM: %s", settings.INPUT_STREAM)
    logger.info("OUTPUT_STREAM: %s", settings.OUTPUT_STREAM)
    logger.info("PARSER_GROUP: %s", settings.PARSER_GROUP)
    logger.info("PARSER_CONSUMER: %s", settings.PARSER_CONSUMER)
    
    setup_signal_handlers()
    
    logger.info("Подключаемся к Redis...")
    r = redis.from_url(settings.REDIS_URL, decode_responses=False)
    try:
        await r.ping()
        logger.info("Подключено к Redis успешно!")
    except Exception as e:
        logger.error("Не удалось подключиться к Redis: %s", e)
        return

    # Инициализация
    await initialize_proxies(r)
    await ensure_consumer_group(r)
    await log_stream_info(r, "startup")

    last_heartbeat = time.time()
    last_trim = time.time()
    
    logger.info("Ожидание задач...")
    
    # Сразу обновим heartbeat при старте
    await update_heartbeat(r)
    logger.info("Heartbeat обновлён при старте")

    while not shutdown_event.is_set():
        try:
            # Обновляем heartbeat каждые 30 секунд
            if time.time() - last_heartbeat > 30:
                await update_heartbeat(r)
                last_heartbeat = time.time()
                logger.debug("Heartbeat обновлён")
            
            # Обрезаем стримы каждые 5 минут
            if time.time() - last_trim > 300:
                await trim_streams(r)
                await log_stream_info(r, "periodic")
                last_trim = time.time()
            
            # Читаем из стрима через consumer group
            logger.debug("Ожидание сообщений из стрима %s...", settings.INPUT_STREAM)
            msgs = await r.xreadgroup(
                settings.PARSER_GROUP,
                settings.PARSER_CONSUMER,
                {settings.INPUT_STREAM: ">"},
                count=1,
                block=5000
            )
            
            if not msgs:
                logger.debug("Нет новых сообщений, продолжаем ожидание...")
                continue

            for stream_name, messages in msgs:
                for msg_id, fields in messages:
                    stream_name_s = _safe_decode(stream_name)
                    msg_id_s = _safe_decode(msg_id)
                    raw = fields.get(b"data")
                    if not raw:
                        # Пустое сообщение - подтверждаем и пропускаем
                        await r.xack(settings.INPUT_STREAM, settings.PARSER_GROUP, msg_id)
                        continue
                    logger.info(
                        "Получено сообщение | stream=%s | msg_id=%s | payload_bytes=%s",
                        stream_name_s,
                        msg_id_s,
                        len(raw),
                    )

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
                    result_json = result.model_dump_json()
                    write_started = time.monotonic()
                    await r.set(
                        result_key,
                        result_json,
                        ex=settings.RESULT_TTL_SEC
                    )
                    write_elapsed = time.monotonic() - write_started
                    logger.info(
                        "Результат записан | key=%s | bytes=%s | ttl=%ss | write_elapsed=%.2fs",
                        result_key,
                        len(result_json),
                        settings.RESULT_TTL_SEC,
                        write_elapsed,
                    )
                    
                    # Также публикуем в output stream (для мониторинга)
                    stream_started = time.monotonic()
                    await r.xadd(
                        settings.OUTPUT_STREAM,
                        {"data": result_json},
                        maxlen=settings.STREAM_MAXLEN
                    )
                    stream_elapsed = time.monotonic() - stream_started
                    logger.info(
                        "Результат опубликован в output stream | stream=%s | elapsed=%.2fs",
                        settings.OUTPUT_STREAM,
                        stream_elapsed,
                    )
                    
                    # Подтверждаем обработку
                    ack_started = time.monotonic()
                    await r.xack(settings.INPUT_STREAM, settings.PARSER_GROUP, msg_id)
                    ack_elapsed = time.monotonic() - ack_started
                    logger.info(
                        "XACK выполнен | stream=%s | group=%s | msg_id=%s | elapsed=%.2fs",
                        settings.INPUT_STREAM,
                        settings.PARSER_GROUP,
                        msg_id_s,
                        ack_elapsed,
                    )
                    
                    # Обновляем статистику
                    await update_stats(r, result.status == "success")
                    
                    logger.info(f"Задача {task.task_id} обработана, статус: {result.status}")

        except redis.ConnectionError as e:
            logger.error("Потеряно соединение с Redis: %s", e)
            if shutdown_event.is_set():
                break
            await asyncio.sleep(5)
            try:
                r = redis.from_url(settings.REDIS_URL, decode_responses=False)
                await r.ping()
                logger.info("Переподключение к Redis успешно")
            except Exception as reconnect_err:
                logger.error("Не удалось переподключиться: %s", reconnect_err)
                
        except Exception as e:
            logger.error("Критическая ошибка в worker: %s", e, exc_info=True)
            if shutdown_event.is_set():
                break
            await asyncio.sleep(5)
    
    # Graceful shutdown
    logger.info("Завершение работы воркера...")
    try:
        await r.close()
        logger.info("Соединение с Redis закрыто")
    except Exception as e:
        logger.warning("Ошибка при закрытии соединения: %s", e)
    logger.info("Воркер завершён")


if __name__ == "__main__":
    print("Starting parser worker...", flush=True)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user", flush=True)
    except Exception as e:
        print(f"Fatal error: {e}", flush=True)
        raise
