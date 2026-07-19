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

derivatives = ["VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q", "V100F1M", "V100F2M", "V100F1Q", "V100F2Q"]
map_derivative = {
    "VN30F1M": "41I1FB000", "VN30F2M": "VN30F2512", "VN30F1Q": "41I1G3000", "VN30F2Q": "41I1G6000",
    "V100F1M": "41I2FB000", "V100F2M": "41I2FC000", "V100F1Q": "41I2G3000", "V100F2Q": "41I2G6000"
}

username = "064CCS7GUK"
password = "199204@Vie"
DB_URL = "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"
SCHEMA = "ohlcv"
RESOLUTION = "1D"
REDIS_URL = "redis://root:Dnl_123456@tanhungsoft.com:6379"
REDIS_CHANNEL = "ohlcv_1d"

engine = create_engine(DB_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

try:
    redis_client.ping()
    logging.info(f"Connected Redis | channel = {REDIS_CHANNEL}")
except Exception as e:
    logging.error(f"Redis connection failed: {e}")
    exit(1)

def is_trading_time_vn():
    now = datetime.now(VN_TZ)
    hm = now.hour + now.minute / 60
    if hm < 9 or 11.5 <= hm < 13 or hm > 14.75:
        return False
    return True

def upsert_1d(symbol, data):
    try:
        ts = int(data.get("time") or data.get("timestamp"))
        if ts > 10000000000:
            ts = ts / 1000
        time_vn = datetime.fromtimestamp(ts).replace(tzinfo=VN_TZ).replace(hour=15, minute=0, second=0, microsecond=0)
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
client = mqtt.Client(client_id=f"dnse-ohlc-derivatives-1d-{randint(1000,9999)}", protocol=mqtt.MQTTv311, transport="websockets")
client.username_pw_set(investor_id, token)
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.ws_set_options(path="/wss")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("Connected MQTT")
        for sym in derivatives:
            client.subscribe(f"plaintext/quotes/krx/mdds/v2/ohlc/derivative/{RESOLUTION}/{sym}", qos=1)
        logging.info(f"Subscribed: {', '.join(derivatives)}")
    else:
        logging.error(f"MQTT connect failed: {rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        raw_symbol = data.get("symbol")
        if not raw_symbol:
            return
        mapped_symbol = map_derivative.get(raw_symbol)
        if not mapped_symbol:
            logging.warning(f"No mapped derivative for symbol {raw_symbol}")
            return
        ts = int(data["time"])
        if ts > 10000000000:
            ts = ts / 1000
        time_vn = datetime.fromtimestamp(ts).replace(tzinfo=VN_TZ).replace(hour=15, minute=0, second=0, microsecond=0)
        time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")
        upsert_1d(mapped_symbol, data)
        redis_client.publish(REDIS_CHANNEL, json.dumps({
            "function": "chart_1d", "symbol": mapped_symbol.upper(), "time": time_vn_str,
            "open": float(data.get("open", 0)), "close": float(data.get("close", 0)),
            "high": float(data.get("high", 0)), "low": float(data.get("low", 0)),
            "volume": float(data.get("volume", 0)), "exchange": "DERIVATIVE"
        }))
    except Exception as e:
        logging.error(f"on_message error: {e}")

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
client.loop_start()

while True:
    time.sleep(1)