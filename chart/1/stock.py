import json
import ssl
import threading
import time
import os
import importlib

import paho.mqtt.client as mqtt
import redis
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from random import randint
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

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
    raise RuntimeError("SYMBOL_NAME chưa được set (vd: HNX1, HOSE3, UPCOM4)")

try:
    module = importlib.import_module(SYMBOL_MODULE)
    SYMBOLS = getattr(module, SYMBOL_NAME)
except Exception as e:
    raise RuntimeError(f"Không load được list {SYMBOL_MODULE}.{SYMBOL_NAME}: {e}")

if not isinstance(SYMBOLS, (list, tuple)) or not SYMBOLS:
    raise RuntimeError(f"SYMBOL_NAME {SYMBOL_NAME} không hợp lệ hoặc rỗng")

print(f"Loaded {len(SYMBOLS)} symbols from {SYMBOL_MODULE}.{SYMBOL_NAME}")

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

DB_URL = os.getenv(
    "DB_URL",
    "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"
)
SCHEMA = os.getenv("DB_SCHEMA", "ohlcv")

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://root:Dnl_123456@tanhungsoft.com:6379"
)

REDIS_CHANNEL = os.getenv(
    "REDIS_CHANNEL",
    f"ohlcv_1"
)

RESOLUTION = "1"

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

adapter = HTTPAdapter(
    pool_connections=5,
    pool_maxsize=5,
    max_retries=retry
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
    pool_pre_ping=True
)

# ==================================================
# REDIS
# ==================================================

redis_pool = redis.ConnectionPool.from_url(
    REDIS_URL,
    max_connections=30,
    decode_responses=False
)

redis_client = redis.Redis(connection_pool=redis_pool)

redis_client.ping()
print("Connected Redis")

# ==================================================
# TRADING TIME (VIETNAM TIME)
# ==================================================

def is_trading_time_vn():
    now = datetime.now(VN_TZ)
    hm = now.hour + now.minute / 60

    if (
        hm < 9 or
        11.5 <= hm < 13 or
        hm > 14.75
    ):
        return False
    return True

# ==================================================
# UPSERT 1M (VN TIMEZONE) - FIXED
# ==================================================

def upsert_1m(symbol, data):
    try:
        ts = int(data.get("time") or data.get("timestamp"))

        if ts > 10000000000:
            ts = ts / 1000

        time_vn = datetime.fromtimestamp(ts)
        time_vn = time_vn.replace(tzinfo=VN_TZ)
        time_vn = time_vn.replace(second=0, microsecond=0)

        time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")

        table = f'"{SCHEMA}"."{symbol.upper()}_1"'

        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";'))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol TEXT,
                    time TIMESTAMPTZ PRIMARY KEY,
                    open DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    volume BIGINT
                );
            """))

            conn.execute(text(f"""
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
            """), {
                "symbol": symbol.upper(),
                "time": time_vn,
                "open": float(data.get("open", 0)),
                "close": float(data.get("close", 0)),
                "high": float(data.get("high", 0)),
                "low": float(data.get("low", 0)),
                "volume": int(data.get("volume", 0))
            })

        print(f"[1M][DB] {symbol} @ {time_vn_str}")

    except Exception as e:
        print(f"DB error {symbol}: {e}")



# ==================================================
# AUTH DNSE
# ==================================================

def authenticate(username, password):
    r = http.post(
        "https://api.dnse.com.vn/user-service/api/auth",
        json={"username": username, "password": password},
        timeout=10
    )
    r.raise_for_status()
    return r.json()["token"]

def get_investor_id(token):
    r = http.get(
        "https://api.dnse.com.vn/user-service/api/me",
        headers={"authorization": f"Bearer {token}"},
        timeout=10
    )
    r.raise_for_status()
    return r.json()["investorId"]

token = authenticate(USERNAME, PASSWORD)
investor_id = get_investor_id(token)

# ==================================================
# MQTT
# ==================================================

client = mqtt.Client(
    client_id=f"dnse-ohlc-1m-{randint(1000,9999)}",
    protocol=mqtt.MQTTv311,
    transport="websockets",
    clean_session=True
)

client.username_pw_set(investor_id, token)

client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.ws_set_options(path="/wss")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected MQTT")
        for sym in SYMBOLS:
            client.subscribe(
                f"plaintext/quotes/krx/mdds/v2/ohlc/stock/{RESOLUTION}/{sym}",
                qos=1
            )
    else:
        print("MQTT connect failed", rc)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        symbol = data.get("symbol")

        if not symbol or not is_trading_time_vn():
            return

        upsert_1m(symbol, data)

        ts = int(data["time"])
        
        # Kiểm tra milliseconds
        if ts > 10000000000:
            ts = ts / 1000
        
        # FIX: Timestamp từ DNSE đã là VN time
        time_vn = datetime.fromtimestamp(ts)
        time_vn = time_vn.replace(tzinfo=VN_TZ)
        time_vn = time_vn.replace(second=0, microsecond=0)
        time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")

        redis_client.publish(
    REDIS_CHANNEL,
    json.dumps({
        "function": "chart_1m",
        "symbol": symbol.upper(),
        "time": time_vn_str,
        "open": float(data.get("open", 0)),
        "close": float(data.get("close", 0)),
        "high": float(data.get("high", 0)),
        "low": float(data.get("low", 0)),
        "volume": float(data.get("volume", 0)),
        "exchange": EXCHANGE
    })
) 
    except Exception as e:
        print("on_message error:", e)

client.on_connect = on_connect
client.on_message = on_message

# ==================================================
# REDIS SUB TEST
# ==================================================

def redis_subscriber_test():
    sub = redis_client.pubsub()
    sub.subscribe(REDIS_CHANNEL)
    for msg in sub.listen():
        if msg["type"] == "message":
            print("Redis:", msg["data"].decode())

threading.Thread(target=redis_subscriber_test, daemon=True).start()

# ==================================================
# START
# ==================================================

client.connect_async("datafeed-lts-krx.dnse.com.vn", 443, keepalive=60)
client.loop_start()

while True:
    time.sleep(1)