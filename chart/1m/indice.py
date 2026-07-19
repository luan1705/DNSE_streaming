import json
import ssl
import threading
import time
import logging

import paho.mqtt.client as mqtt
import redis
from requests import post, get
from random import randint
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

# ==================================================
# LOGGING
# ==================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

INDEX_LIST = ["VNINDEX", "VN30", "HNX", "HNX30", "UPCOM", "VNXAllShare", "VN100"]

username = "064CCS7GUK"
password = "199204@Vie"
DB_URL = "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"
SCHEMA = "ohlcv"
RESOLUTION = "1"
REDIS_URL = "redis://root:Dnl_123456@tanhungsoft.com:6379"
REDIS_CHANNEL = "ohlcv_1"

engine = create_engine(DB_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

try:
    redis_client.ping()
    logging.info(f"Connected Redis | channel = {REDIS_CHANNEL}")
except Exception as e:
    logging.error(f"Redis connection failed: {e}")
    exit(1)

special_map = {
    "HNX": "HNXINDEX", "hnx": "HNXINDEX", "UPCOM": "UPCOMINDEX",
    "vnindex": "VNINDEX", "VNXAllShare": "VNXALLSHARE", "vnallshare": "VNXALLSHARE",
    "HNX30": "HNX30", "hnx30": "HNX30"
}

def normalize_symbol(symbol):
    s = symbol.strip().upper().replace(" ", "")
    return special_map.get(s, s)

def is_trading_time_vn():
    now = datetime.now(VN_TZ)
    hm = now.hour + now.minute / 60
    if hm < 9 + 15/60 or 11.5 <= hm < 13 or hm > 14.75:
        return False
    return True

def upsert_1m(symbol, data):
    try:
        symbol = normalize_symbol(symbol)
        ts = int(data.get("time") or data.get("timestamp"))
        if ts > 10000000000:
            ts = ts / 1000
        time_vn = datetime.fromtimestamp(ts).replace(tzinfo=VN_TZ).replace(second=0, microsecond=0)
        time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")
        table = f'"{SCHEMA}"."{symbol}_1"'
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
                "symbol": symbol, "time": time_vn,
                "open": float(data.get("open", 0)), "close": float(data.get("close", 0)),
                "high": float(data.get("high", 0)), "low": float(data.get("low", 0)),
                "volume": int(data.get("volume", 0))
            })
        logging.info(f"[DB] {symbol} @ {time_vn_str}")
    except Exception as e:
        logging.error(f"DB error {symbol}: {e}")

def authenticate(username, password):
    url = "https://api.dnse.com.vn/user-service/api/auth"
    response = post(url, json={"username": username, "password": password})
    response.raise_for_status()
    return response.json().get("token")

def get_investor_info(token):
    url = "https://api.dnse.com.vn/user-service/api/me"
    headers = {"authorization": f"Bearer {token}"}
    response = get(url, headers=headers)
    response.raise_for_status()
    return response.json()

token = authenticate(username, password)
investor_id = get_investor_info(token)["investorId"]

BROKER_HOST = "datafeed-lts-krx.dnse.com.vn"
BROKER_PORT = 443
client = mqtt.Client(client_id=f"dnse-ohlc-index-1m-{randint(1000,9999)}", protocol=mqtt.MQTTv311, transport="websockets")
client.username_pw_set(investor_id, token)
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.ws_set_options(path="/wss")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("Connected MQTT")
        for sym in INDEX_LIST:
            client.subscribe(f"plaintext/quotes/krx/mdds/v2/ohlc/index/{RESOLUTION}/{sym.replace(' ', '')}", qos=1)
        logging.info(f"Subscribed: {', '.join(INDEX_LIST)}")
    else:
        logging.error(f"MQTT connect failed: {rc}")

def on_message(client, userdata, msg):
    try:
        if not is_trading_time_vn():
            return
        data = json.loads(msg.payload.decode())
        symbol = data.get("symbol")
        if not symbol:
            return
        norm_symbol = normalize_symbol(symbol)
        ts = int(data["time"])
        if ts > 10000000000:
            ts = ts / 1000
        time_vn = datetime.fromtimestamp(ts).replace(tzinfo=VN_TZ).replace(second=0, microsecond=0)
        time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")
        upsert_1m(norm_symbol, data)
        redis_client.publish(REDIS_CHANNEL, json.dumps({
            "function": "chart_1m", "symbol": norm_symbol, "time": time_vn_str,
            "open": data.get("open"), "close": data.get("close"),
            "high": data.get("high"), "low": data.get("low"),
            "volume": float(data.get("volume", 0))
        }))
    except Exception as e:
        logging.error(f"on_message error: {e}")

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
client.loop_start()

while True:
    time.sleep(1)