import json
import logging
import os
import queue
import ssl
import threading
import time

from datetime import datetime
from random import randint
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt
import redis

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine, text


# ==================================================
# LOGGING
# ==================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


# ==================================================
# TIMEZONE
# ==================================================
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


# ==================================================
# INDEX LIST
# ==================================================
INDEX_LIST = [
    "VNINDEX",
    "VN30",
    "HNX",
    "HNX30",
    "UPCOM",
    "VNXAllShare",
    "VN100",
]


# ==================================================
# CONFIG
# ==================================================
USERNAME = os.getenv("DNSE_USERNAME", "064CCS7GUK")
PASSWORD = os.getenv("DNSE_PASSWORD", "199204@Vie")

DB_URL = os.getenv(
    "DB_URL",
    "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl",
)
SCHEMA = os.getenv("DB_SCHEMA", "ohlcv")

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://root:Dnl_123456@tanhungsoft.com:6379",
)
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "ohlcv_1d")

RESOLUTION = "1D"
LATEST_DNSE_KEY_PREFIX = os.getenv(
    "LATEST_DNSE_KEY_PREFIX",
    "latest_dnse_streaming_message_1D"
)


# ==================================================
# HTTP SESSION
# ==================================================
http = Session()

retry = Retry(
    total=3,
    backoff_factor=0.3,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)

adapter = HTTPAdapter(
    pool_connections=5,
    pool_maxsize=5,
    max_retries=retry,
)

http.mount("https://", adapter)
http.mount("http://", adapter)


# ==================================================
# POSTGRES
# ==================================================
engine = create_engine(
    DB_URL,
    pool_size=5,
    max_overflow=5,
    pool_timeout=20,
    pool_recycle=1800,
    pool_pre_ping=True,
)


# ==================================================
# REDIS CONNECTION
# ==================================================
redis_pool = None
redis_client = None
redis_lock = threading.Lock()


def create_redis():
    global redis_pool

    if redis_pool is not None:
        try:
            redis_pool.disconnect()
        except Exception:
            pass

    redis_pool = redis.BlockingConnectionPool.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,
        max_connections=30,
        timeout=1.0,
    )

    return redis.Redis(
        connection_pool=redis_pool
    )


def reconnect_redis() -> bool:
    global redis_client

    with redis_lock:
        logging.warning("[REDIS] Reconnecting...")

        try:
            redis_client = create_redis()
            redis_client.ping()

            logging.info("[REDIS] Reconnect OK")
            return True

        except Exception as e:
            logging.error(
                "[REDIS] Reconnect failed: %s",
                e,
            )
            return False


def publish_redis(
    payload: dict,
    channel: str = REDIS_CHANNEL,
) -> bool:
    data = json.dumps(
        payload,
        ensure_ascii=False,
    )

    for attempt in range(1, 4):
        try:
            redis_client.publish(
                channel,
                data,
            )
            return True

        except Exception as e:
            logging.warning(
                "[REDIS PUBLISH] Failed | "
                "channel=%s | attempt=%d/3 | error=%s",
                channel,
                attempt,
                e,
            )

            reconnect_redis()

            if attempt < 3:
                time.sleep(1)

    logging.error(
        "[REDIS PUBLISH] Give up | "
        "channel=%s | restarting process...",
        channel,
    )

    os._exit(1)


redis_client = create_redis()

try:
    redis_client.ping()

    logging.info(
        "Connected Redis | channel=%s",
        REDIS_CHANNEL,
    )

except Exception as e:
    logging.error(
        "Initial Redis connection failed: %s",
        e,
    )

    if not reconnect_redis():
        raise

# ==================================================
# SYMBOL MAPPING
# ==================================================
SPECIAL_MAP = {
    "HNX": "HNXINDEX",
    "UPCOM": "UPCOMINDEX",
    "VNINDEX": "VNINDEX",
    "VNXALLSHARE": "VNXALLSHARE",
    "VNALLSHARE": "VNXALLSHARE",
    "HNX30": "HNX30",
}


def normalize_symbol(symbol):
    value = str(symbol or "").strip().upper().replace(" ", "")
    return SPECIAL_MAP.get(value, value)


# ==================================================
# UPSERT 1D
# ==================================================
def upsert_1d(symbol, data):
    ts = int(data.get("time") or data.get("timestamp"))

    if ts > 10_000_000_000:
        ts //= 1000

    time_vn = datetime.fromtimestamp(
        ts,
        tz=VN_TZ,
    ).replace(
        hour=15,
        minute=0,
        second=0,
        microsecond=0,
    )

    table = f'"{SCHEMA}"."{symbol.upper()}_1D"'

    with engine.begin() as conn:
        conn.execute(
            text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";')
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol TEXT,
                    time TIMESTAMP WITH TIME ZONE PRIMARY KEY,
                    open DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    volume BIGINT
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                INSERT INTO {table}
                    (symbol, time, open, close, high, low, volume)
                VALUES
                    (:symbol, :time, :open, :close, :high, :low, :volume)
                ON CONFLICT (time) DO UPDATE SET
                    open = EXCLUDED.open,
                    close = EXCLUDED.close,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    volume = EXCLUDED.volume;
                """
            ),
            {
                "symbol": symbol.upper(),
                "time": time_vn,
                "open": float(data.get("open") or 0),
                "close": float(data.get("close") or 0),
                "high": float(data.get("high") or 0),
                "low": float(data.get("low") or 0),
                "volume": int(data.get("volume") or 0),
            },
        )


# ==================================================
# DB WORKER
# ==================================================
db_queue = queue.Queue(maxsize=10_000)


def db_worker():
    while True:
        symbol, data = db_queue.get()

        try:
            upsert_1d(symbol, data)
        except Exception as e:
            logging.error("[DB err] %s: %s", symbol, e)
        finally:
            db_queue.task_done()


threading.Thread(
    target=db_worker,
    daemon=True,
    name="db-worker",
).start()


# ==================================================
# Latest DNSE Message Saver
# ==================================================

def save_latest_dnse_message(
    payload: dict,
) -> bool:
    symbol = str(
        payload.get("symbol") or ""
    ).strip().upper()

    if not symbol:
        logging.warning(
            "[REDIS SET] Missing symbol | payload=%s",
            payload,
        )
        return False

    key = (
        f"{LATEST_DNSE_KEY_PREFIX}:"
        f"{symbol}"
    )

    value = json.dumps(
        payload,
        ensure_ascii=False,
    )

    for attempt in range(1, 4):
        try:
            redis_client.set(
                key,
                value,
            )
            return True

        except Exception as e:
            logging.warning(
                "[REDIS SET] Failed | "
                "key=%s | attempt=%d/3 | error=%s",
                key,
                attempt,
                e,
            )

            reconnect_redis()

            if attempt < 3:
                time.sleep(1)

    logging.error(
        "[REDIS SET] Give up | "
        "key=%s | restarting process...",
        key,
    )

    os._exit(1)


# ==================================================
# AUTH DNSE
# ==================================================
def authenticate(username, password):
    response = http.post(
        "https://api.dnse.com.vn/user-service/api/auth",
        json={
            "username": username,
            "password": password,
        },
        timeout=10,
    )

    response.raise_for_status()
    return response.json()["token"]


def get_investor_id(token):
    response = http.get(
        "https://api.dnse.com.vn/user-service/api/me",
        headers={
            "authorization": f"Bearer {token}",
        },
        timeout=10,
    )

    response.raise_for_status()
    return response.json()["investorId"]


token = authenticate(USERNAME, PASSWORD)
investor_id = get_investor_id(token)


# ==================================================
# MQTT
# ==================================================
client = mqtt.Client(
    client_id=f"dnse-ohlc-index-1d-{randint(1000, 9999)}",
    protocol=mqtt.MQTTv311,
    transport="websockets",
    clean_session=True,
)

client.username_pw_set(investor_id, token)
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.ws_set_options(path="/wss")


def on_connect(client, userdata, flags, rc, properties=None):
    if rc != 0:
        logging.error("MQTT connect failed: %s", rc)
        return

    logging.info("Connected MQTT")

    for symbol in INDEX_LIST:
        topic_symbol = symbol.replace(" ", "")

        client.subscribe(
            (
                "plaintext/quotes/krx/mdds/v2/ohlc/"
                f"index/{RESOLUTION}/{topic_symbol}"
            ),
            qos=1,
        )

    logging.info("Subscribed: %s", ", ".join(INDEX_LIST))


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        symbol = data.get("symbol")

        if not symbol:
            return

        norm_symbol = normalize_symbol(symbol)

        # Giữ time của nến 1D ở 15:00
        ts = int(data.get("time") or data.get("timestamp"))

        if ts > 10_000_000_000:
            ts //= 1000

        time_vn = datetime.fromtimestamp(
            ts,
            tz=VN_TZ,
        ).replace(
            hour=15,
            minute=0,
            second=0,
            microsecond=0,
        )

        time_vn_str = time_vn.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        latest_time_str = datetime.now(
            VN_TZ
        ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        payload = {
            "function": "chart_1d",
            "symbol": norm_symbol,
            "time": time_vn_str,
            "latest_time": latest_time_str,
            "open": float(data.get("open") or 0),
            "close": float(data.get("close") or 0),
            "high": float(data.get("high") or 0),
            "low": float(data.get("low") or 0),
            "volume": float(data.get("volume") or 0),
            "exchange": "INDEX",
        }

        try:
            db_queue.put_nowait(
                (norm_symbol, data)
            )
        except queue.Full:
            logging.warning(
                "[db-queue-full] dropped %s",
                norm_symbol,
            )

        publish_redis(
            payload,
            REDIS_CHANNEL,
        )
        
        save_latest_dnse_message(payload)

    except Exception:
        logging.exception(
            "on_message error | topic=%s",
            msg.topic,
        )


client.on_connect = on_connect
client.on_message = on_message

client.connect_async(
    "datafeed-lts-krx.dnse.com.vn",
    443,
    keepalive=60,
)

client.loop_start()

while True:
    time.sleep(1)