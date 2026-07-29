import json
import ssl
import threading
import time
import logging
import queue
import os

import paho.mqtt.client as mqtt
import redis

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from random import randint
from datetime import datetime
from zoneinfo import ZoneInfo

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
# DERIVATIVES
# ==================================================
derivatives = [
    "VN30F1M",
    "VN30F2M",
    "VN30F1Q",
    "VN30F2Q",
    "V100F1M",
    "V100F2M",
    "V100F1Q",
    "V100F2Q",
]

map_derivative = {
    "VN30F1M": "41I1FB000",
    "VN30F2M": "VN30F2512",
    "VN30F1Q": "41I1G3000",
    "VN30F2Q": "41I1G6000",
    "V100F1M": "41I2FB000",
    "V100F2M": "41I2FC000",
    "V100F1Q": "41I2G3000",
    "V100F2Q": "41I2G6000",
}


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

REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "ohlcv_1")

RESOLUTION = "1"


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

    return redis.Redis(connection_pool=redis_pool)


def reconnect_redis():
    global redis_client

    with redis_lock:
        logging.warning("[REDIS] Reconnecting...")

        try:
            redis_client = create_redis()
            redis_client.ping()

            logging.info("[REDIS] Reconnect OK")
            return True

        except Exception as e:
            logging.error("[REDIS] Reconnect failed: %s", e)
            return False


def publish_redis(payload, channel=REDIS_CHANNEL):
    data = json.dumps(payload, ensure_ascii=False)

    for attempt in range(1, 4):
        try:
            redis_client.publish(channel, data)
            return True

        except Exception as e:
            logging.warning(
                "[REDIS PUBLISH] Failed | channel=%s | attempt=%d/3 | error=%s",
                channel,
                attempt,
                e,
            )

            reconnect_redis()

            if attempt < 3:
                time.sleep(1)

    logging.error(
        "[REDIS PUBLISH] Give up | restarting process..."
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
# TRADING TIME
# ==================================================
def is_trading_time_vn():
    now = datetime.now(VN_TZ)
    hm = now.hour + now.minute / 60

    if hm < 9 or 11.5 <= hm < 13 or hm > 14.75:
        return False

    return True


# ==================================================
# UPSERT 1 MINUTE
# ==================================================
def upsert_1m(mapped_symbol, data):
    ts = int(
        data.get("time")
        or data.get("timestamp")
    )

    if ts > 10_000_000_000:
        ts //= 1000

    time_vn = datetime.fromtimestamp(
        ts,
        tz=VN_TZ,
    ).replace(
        second=0,
        microsecond=0,
    )

    table = f'"{SCHEMA}"."{mapped_symbol.upper()}_1"'

    with engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";'
            )
        )

        conn.execute(
            text(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol TEXT,
                    time TIMESTAMPTZ PRIMARY KEY,
                    open DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    volume BIGINT
                );
            """)
        )

        conn.execute(
            text(f"""
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
            """),
            {
                "symbol": mapped_symbol.upper(),
                "time": time_vn,
                "open": float(data.get("open") or 0),
                "close": float(data.get("close") or 0),
                "high": float(data.get("high") or 0),
                "low": float(data.get("low") or 0),
                "volume": int(data.get("volume") or 0),
            },
        )


# ==================================================
# DATABASE WORKER
# ==================================================
db_queue = queue.Queue(
    maxsize=10_000,
)


def db_worker():
    while True:
        mapped_symbol, data = db_queue.get()

        try:
            upsert_1m(
                mapped_symbol,
                data,
            )
        except Exception as e:
            logging.error(
                "[DB err] %s: %s",
                mapped_symbol,
                e,
            )
        finally:
            db_queue.task_done()


threading.Thread(
    target=db_worker,
    daemon=True,
    name="db-worker",
).start()


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


token = authenticate(
    USERNAME,
    PASSWORD,
)

investor_id = get_investor_id(
    token,
)


# ==================================================
# MQTT
# ==================================================
BROKER_HOST = "datafeed-lts-krx.dnse.com.vn"
BROKER_PORT = 443

client = mqtt.Client(
    client_id=(
        f"dnse-ohlc-derivatives-1m-"
        f"{randint(1000, 9999)}"
    ),
    protocol=mqtt.MQTTv311,
    transport="websockets",
    clean_session=True,
)

client.username_pw_set(
    investor_id,
    token,
)

client.tls_set(
    cert_reqs=ssl.CERT_NONE,
)

client.tls_insecure_set(True)
client.ws_set_options(path="/wss")


def on_connect(
    client,
    userdata,
    flags,
    rc,
    properties=None,
):
    if rc == 0:
        logging.info("Connected MQTT")

        for sym in derivatives:
            client.subscribe(
                (
                    "plaintext/quotes/krx/mdds/v2/ohlc/"
                    f"derivative/{RESOLUTION}/{sym}"
                ),
                qos=1,
            )

        logging.info(
            "Subscribed: %s",
            ", ".join(derivatives),
        )

    else:
        logging.error(
            "MQTT connect failed: %s",
            rc,
        )


def on_message(
    client,
    userdata,
    msg,
):
    try:
        data = json.loads(
            msg.payload.decode()
        )

        raw_symbol = data.get("symbol")

        if not raw_symbol or not is_trading_time_vn():
            return

        mapped_symbol = map_derivative.get(
            raw_symbol
        )

        if not mapped_symbol:
            logging.warning(
                "No mapped derivative for symbol %s",
                raw_symbol,
            )
            return

        ts = int(
            data.get("time")
            or data.get("timestamp")
        )

        if ts > 10_000_000_000:
            ts //= 1000

        time_vn = datetime.fromtimestamp(
            ts,
            tz=VN_TZ,
        ).replace(
            second=0,
            microsecond=0,
        )

        payload = {
            "function": "chart_1m",
            "symbol": mapped_symbol.upper(),
            "time": time_vn.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "open": float(
                data.get("open") or 0
            ),
            "close": float(
                data.get("close") or 0
            ),
            "high": float(
                data.get("high") or 0
            ),
            "low": float(
                data.get("low") or 0
            ),
            "volume": float(
                data.get("volume") or 0
            ),
            "exchange": "DERIVATIVE",
        }

        # Ghi PostgreSQL bất đồng bộ
        try:
            db_queue.put_nowait(
                (
                    mapped_symbol,
                    data,
                )
            )
        except queue.Full:
            logging.warning(
                "[db-queue-full] dropped %s",
                mapped_symbol,
            )

        # Gửi realtime Pub/Sub
        publish_redis(
            payload,
            REDIS_CHANNEL,
        )

    except Exception:
        logging.exception(
            "on_message error | topic=%s",
            msg.topic,
        )


client.on_connect = on_connect
client.on_message = on_message

client.connect_async(
    BROKER_HOST,
    BROKER_PORT,
    keepalive=60,
)

client.loop_start()

while True:
    time.sleep(1)