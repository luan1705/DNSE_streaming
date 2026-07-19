import json
import ssl
import threading
import time
import queue
import os
import importlib
import collections
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
DB_URL = os.getenv("DB_URL", "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl")
SCHEMA = os.getenv("DB_SCHEMA", "ohlcv")
REDIS_URL = os.getenv("REDIS_URL", "redis://root:Dnl_123456@tanhungsoft.com:6379")
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "ohlcv_1d")
RESOLUTION = "1D"

# ==================================================
# HTTP SESSION
# ==================================================
http = Session()
retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET", "POST"])
adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=retry)
http.mount("https://", adapter)
http.mount("http://", adapter)

# ==================================================
# POSTGRES
# ==================================================
engine = create_engine(DB_URL, pool_size=5, max_overflow=5, pool_timeout=20, pool_recycle=1800, pool_pre_ping=True)

# ==================================================
# REDIS
# ==================================================
redis_pool = redis.ConnectionPool.from_url(REDIS_URL, max_connections=30, decode_responses=False)
redis_client = redis.Redis(connection_pool=redis_pool)
redis_client.ping()
logging.info(f"Connected Redis | channel = {REDIS_CHANNEL}")

# ==================================================
# LATENCY TRACKER
# ==================================================
class LatencyTracker:
    PRINT_EVERY = 1
    WINDOW = 200
    WARN_E2E_MS = 500

    def __init__(self):
        self._lock = threading.Lock()
        self._counter = 0
        self._samples = {
            "broker": collections.deque(maxlen=self.WINDOW),
            "redis": collections.deque(maxlen=self.WINDOW),
            "e2e": collections.deque(maxlen=self.WINDOW),
            "db": collections.deque(maxlen=self.WINDOW),
        }

    @staticmethod
    def _pct(data, p):
        if not data:
            return 0.0
        s = sorted(data)
        idx = int(len(s) * p / 100)
        return s[min(idx, len(s) - 1)]

    @staticmethod
    def _fmt(ms):
        if ms < 50:
            return f"\033[32m{ms:7.1f}ms\033[0m"
        if ms < 200:
            return f"\033[33m{ms:7.1f}ms\033[0m"
        return f"\033[31m{ms:7.1f}ms\033[0m"

    def record_mqtt(self, symbol, exchange_ts_ms, recv_ts_ms, redis_done_ts_ms):
        broker_ms = recv_ts_ms - exchange_ts_ms
        redis_ms = redis_done_ts_ms - recv_ts_ms
        e2e_ms = redis_done_ts_ms - exchange_ts_ms
        with self._lock:
            self._samples["broker"].append(broker_ms)
            self._samples["redis"].append(redis_ms)
            self._samples["e2e"].append(e2e_ms)
            self._counter += 1
            counter = self._counter
            snap = {k: list(v) for k, v in self._samples.items()}
        if counter % self.PRINT_EVERY == 0:
            now_str = datetime.now(VN_TZ).strftime("%H:%M:%S.%f")[:-3]
            warn = f"  \033[31m⚠ E2E > {self.WARN_E2E_MS}ms!\033[0m" if e2e_ms > self.WARN_E2E_MS else ""
            print(f"[{now_str}] {symbol:<8} | broker={self._fmt(broker_ms)} | redis={self._fmt(redis_ms)} | e2e={self._fmt(e2e_ms)}{warn}")

    def record_db(self, symbol, enqueue_ts_ms, db_done_ts_ms):
        db_ms = db_done_ts_ms - enqueue_ts_ms
        with self._lock:
            self._samples["db"].append(db_ms)
        print(f"  [db] {symbol:<8} | write={self._fmt(db_ms)}")

latency = LatencyTracker()

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
    time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")
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
    return time_vn_str

# ==================================================
# DB WORKER
# ==================================================
db_queue = queue.Queue(maxsize=10_000)

def db_worker():
    while True:
        symbol, data, enqueue_ts_ms = db_queue.get()
        try:
            time_vn_str = upsert_1d(symbol, data)
            db_done_ts_ms = time.time() * 1000
            latency.record_db(symbol, enqueue_ts_ms, db_done_ts_ms)
            logging.info(f"[DB] {symbol} @ {time_vn_str}")
        except Exception as e:
            logging.error(f"[DB err] {symbol}: {e}")
        finally:
            db_queue.task_done()

threading.Thread(target=db_worker, daemon=True, name="db-worker").start()

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
    recv_ts_ms = time.time() * 1000
    try:
        data = json.loads(msg.payload.decode())
        symbol = data.get("symbol")
        if not symbol or not is_trading_time_vn():
            return
        exchange_ts_ms = recv_ts_ms
        ts = int(data["time"])
        if ts > 10_000_000_000:
            ts //= 1000
        time_vn = datetime.fromtimestamp(ts, tz=VN_TZ).replace(hour=15, minute=0, second=0, microsecond=0)
        time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")
        redis_client.publish(REDIS_CHANNEL, json.dumps({
            "function": "chart_1d", "symbol": symbol.upper(), "time": time_vn_str,
            "open": float(data.get("open", 0)), "close": float(data.get("close", 0)),
            "high": float(data.get("high", 0)), "low": float(data.get("low", 0)),
            "volume": float(data.get("volume", 0)), "exchange": EXCHANGE
        }))
        redis_done_ts_ms = time.time() * 1000
        latency.record_mqtt(symbol=symbol, exchange_ts_ms=exchange_ts_ms, recv_ts_ms=recv_ts_ms, redis_done_ts_ms=redis_done_ts_ms)
        try:
            db_queue.put_nowait((symbol, data, redis_done_ts_ms))
        except queue.Full:
            logging.warning(f"[db-queue-full] dropped {symbol}")
    except Exception as e:
        logging.error(f"on_message error: {e}")

client.on_connect = on_connect
client.on_message = on_message

client.connect_async("datafeed-lts-krx.dnse.com.vn", 443, keepalive=60)
client.loop_start()

while True:
    time.sleep(1)