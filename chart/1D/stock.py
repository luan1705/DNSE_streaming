import json
import ssl
import threading
import time
import queue
import os
import importlib
import logging

import paho.mqtt.client as mqtt
import redis
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from random import randint
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from config import REDIS_URL, POSTGRES_URL

# ==================================================
# LOGGING
# ==================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ==================================================
# TIMEZONE
# ==================================================
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# ==================================================
# LOAD SYMBOL LIST FROM ENV
# ==================================================
SYMBOL_MODULE = os.getenv("SYMBOL_MODULE", "List.exchange")
SYMBOL_NAME = os.getenv("SYMBOL_NAME")

if not SYMBOL_NAME:
    raise RuntimeError("SYMBOL_NAME chua duoc set (vd: HNX1, HOSE3, UPCOM4)")

try:
    module = importlib.import_module(SYMBOL_MODULE)
    SYMBOLS = getattr(module, SYMBOL_NAME)
except Exception as e:
    raise RuntimeError(f"Khong load duoc list {SYMBOL_MODULE}.{SYMBOL_NAME}: {e}")

if not isinstance(SYMBOLS, (list, tuple)) or not SYMBOLS:
    raise RuntimeError(f"SYMBOL_NAME {SYMBOL_NAME} khong hop le hoac rong")

logging.info(f"Loaded {len(SYMBOLS)} symbols from {SYMBOL_MODULE}.{SYMBOL_NAME}")

# ==================================================
# DERIVE EXCHANGE NAME
# ==================================================
if SYMBOL_NAME.startswith("HNX"):
    EXCHANGE = "HNX"
elif SYMBOL_NAME.startswith("HOSE"):
    EXCHANGE = "HOSE"
elif SYMBOL_NAME.startswith("UPCOM"):
    EXCHANGE = "UPCOM"
else:
    EXCHANGE = "DERIVATIVES"

# ==================================================
# CONFIG
# ==================================================
USERNAME = os.getenv("DNSE_USERNAME", "064CCS7GUK")
PASSWORD = os.getenv("DNSE_PASSWORD", "199204@Vie")
SCHEMA = os.getenv("DB_SCHEMA", "ohlcv")
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
    allowed_methods=["GET", "POST"]
    )
adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=retry)
http.mount("https://", adapter)
http.mount("http://", adapter)

# ==================================================
# POSTGRES
# ==================================================
engine = create_engine(POSTGRES_URL, pool_size=5, max_overflow=5, pool_timeout=20, pool_recycle=1800, pool_pre_ping=True)

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

# ==================================================
# TRADING TIME
# ==================================================
def is_trading_time_vn():
    now = datetime.now(VN_TZ)
    hm = now.hour + now.minute / 60
    return not (hm < 9 or 11.5 <= hm < 13 or hm > 14.75)

# ==================================================
# UPSERT 1D
# ==================================================
def upsert_1d(symbol, data):
    ts = int(data.get("time") or data.get("timestamp"))
    if ts > 10_000_000_000:
        ts //= 1000
    time_vn = datetime.fromtimestamp(ts, tz=VN_TZ).replace(hour=15, minute=0, second=0, microsecond=0)
    table = f'"{SCHEMA}"."{symbol.upper()}_1D"'
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";'))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                symbol TEXT, time TIMESTAMP WITH TIME ZONE PRIMARY KEY,
                open DOUBLE PRECISION, close DOUBLE PRECISION,
                high DOUBLE PRECISION, low DOUBLE PRECISION, volume BIGINT
            );
        """))
        conn.execute(text(f"""
            INSERT INTO {table} (symbol, time, open, close, high, low, volume)
            VALUES (:symbol, :time, :open, :close, :high, :low, :volume)
            ON CONFLICT (time) DO UPDATE SET
                open=EXCLUDED.open, close=EXCLUDED.close,
                high=EXCLUDED.high, low=EXCLUDED.low, volume=EXCLUDED.volume;
        """), {
            "symbol": symbol.upper(), "time": time_vn,
            "open": float(data.get("open", 0)), "close": float(data.get("close", 0)),
            "high": float(data.get("high", 0)), "low": float(data.get("low", 0)),
            "volume": int(data.get("volume", 0))
        })

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
            logging.error(f"[DB err] {symbol}: {e}")
        finally:
            db_queue.task_done()

threading.Thread(target=db_worker, daemon=True, name="db-worker").start()

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
    r = http.post("https://api.dnse.com.vn/user-service/api/auth", json={"username": username, "password": password}, timeout=10)
    r.raise_for_status()
    return r.json()["token"]

def get_investor_id(token):
    r = http.get("https://api.dnse.com.vn/user-service/api/me", headers={"authorization": f"Bearer {token}"}, timeout=10)
    r.raise_for_status()
    return r.json()["investorId"]

token = authenticate(USERNAME, PASSWORD)
investor_id = get_investor_id(token)

# ==================================================
# MQTT
# ==================================================
client = mqtt.Client(client_id=f"dnse-ohlc-1d-{randint(1000,9999)}", protocol=mqtt.MQTTv311, transport="websockets", clean_session=True)
client.username_pw_set(investor_id, token)
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.ws_set_options(path="/wss")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("Connected MQTT")
        for sym in SYMBOLS:
            client.subscribe(f"plaintext/quotes/krx/mdds/v2/ohlc/stock/{RESOLUTION}/{sym}", qos=1)
        logging.info(f"Subscribed: {', '.join(SYMBOLS[:5])}{'...' if len(SYMBOLS) > 5 else ''} ({len(SYMBOLS)} symbols)")
    else:
        logging.error(f"MQTT connect failed: {rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        symbol = data.get("symbol")

        if not symbol or not is_trading_time_vn():
            return

        # Thời gian cây nến 1D: giữ nguyên 15:00 như code cũ
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

        # Thời điểm message mới nhất được nhận
        latest_time_str = datetime.now(
            VN_TZ
        ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        payload = {
            "function": "ohlcv_1d",
            "symbol": symbol.upper(),

            # Giữ nguyên time của code cũ
            "time": time_vn_str,

            # Bổ sung thời gian cập nhật realtime
            "latest_time": latest_time_str,

            "open": float(data.get("open") or 0),
            "close": float(data.get("close") or 0),
            "high": float(data.get("high") or 0),
            "low": float(data.get("low") or 0),
            "volume": float(data.get("volume") or 0),
            "exchange": EXCHANGE,
        }

        # PostgreSQL vẫn ghi bất đồng bộ như hiện tại
        try:
            db_queue.put_nowait(
                (symbol, data)
            )
        except queue.Full:
            logging.warning(
                "[db-queue-full] dropped %s",
                symbol,
            )

        publish_redis(
            payload,
            REDIS_CHANNEL,
        )

        # Redis SET latest theo từng mã
        save_latest_dnse_message(payload)

    except Exception:
        logging.exception(
            "on_message error | topic=%s",
            msg.topic,
        )

client.on_connect = on_connect
client.on_message = on_message

client.connect_async("datafeed-lts-krx.dnse.com.vn", 443, keepalive=60)
client.loop_start()

while True:
    time.sleep(1)