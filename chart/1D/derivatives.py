import json
import ssl
import threading
import time
import paho.mqtt.client as mqtt
import redis
from requests import post, get
from random import randint
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

# ==================================================
# TIMEZONE
# ==================================================

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# ==================== MAP SYMBOL ====================
derivatives = [
    "VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q",
    "V100F1M", "V100F2M", "V100F1Q", "V100F2Q"
]

map_derivative = {
    "VN30F1M": "41I1FB000",
    "VN30F2M": "VN30F2512",
    "VN30F1Q": "41I1G3000",
    "VN30F2Q": "41I1G6000",
    "V100F1M": "41I2FB000",
    "V100F2M": "41I2FC000",
    "V100F1Q": "41I2G3000",
    "V100F2Q": "41I2G6000"
}

# ==================== CONFIG ====================
username = "064CCS7GUK"
password = "199204@Vie"

DB_URL = "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"
SCHEMA = "ohlcv"
RESOLUTION = "1D"

REDIS_URL = "redis://root:Dnl_123456@tanhungsoft.com:6379"
REDIS_CHANNEL = "ohlcv_1d"

engine = create_engine(DB_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

# ==================== TEST REDIS ====================
try:
    redis_client.ping()
    print("✅ Connected to Redis!")
except Exception as e:
    print("❌ Redis connection failed:", e)
    exit(1)

# ==================== HÀM KIỂM TRA GIỜ GIAO DỊCH (VN TIMEZONE) ====================
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

# ==================== UPSERT 1D (VN TIMEZONE) - TIME SET TO 00:00:00 ====================
def upsert_1d(symbol, data):
    try:
        ts = int(data.get("time") or data.get("timestamp"))

        if ts > 10000000000:
            ts = ts / 1000

        time_vn = datetime.fromtimestamp(ts)
        time_vn = time_vn.replace(tzinfo=VN_TZ)
        time_vn = time_vn.replace(hour=15, minute=0, second=0, microsecond=0)

        time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")

        table = f'"{SCHEMA}"."{symbol.upper()}_1D"'

        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";'))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol TEXT,
                    time TIMESTAMP WITH TIME ZONE PRIMARY KEY,
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
                "volume": int(data.get("volume", 0)),
            })

        print(f"[1d] {symbol} @ {time_vn_str} VN")

    except Exception as e:
        print(f"DB error {symbol}: {e}")


# ==================== AUTH ====================
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

# ==================== MQTT ====================
BROKER_HOST = "datafeed-lts-krx.dnse.com.vn"
BROKER_PORT = 443
client_id = f"dnse-ohlc-derivatives-1d-{randint(1000,9999)}"

client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311, transport="websockets")
client.username_pw_set(investor_id, token)
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.ws_set_options(path="/wss")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ Connected to MQTT (1D)!")
        for sym in derivatives:
            topic = f"plaintext/quotes/krx/mdds/v2/ohlc/derivative/{RESOLUTION}/{sym}"
            client.subscribe(topic, qos=1)
            print("📡 Subscribed:", topic)
    else:
        print("❌ MQTT connect failed:", rc)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        raw_symbol = data.get("symbol")
        if not raw_symbol:
            print(f"⚠️ Unknown payload: {data}")
            return

        # # ====== LỌC NGOÀI GIỜ (VN TIME) ======
        # if not is_trading_time_vn():
        #     print(f"⏭️ [SKIP] {raw_symbol} | {datetime.now(VN_TZ).strftime('%H:%M:%S')} VN | Ngoài giờ giao dịch")
        #     return

        # Map sang mã thực tế
        mapped_symbol = map_derivative.get(raw_symbol)
        if not mapped_symbol:
            print(f"⚠️ No mapped derivative found for symbol {raw_symbol}")
            print("🔍 Known keys:", list(map_derivative.keys()))
            return

        ts = int(data["time"])
        
        # Kiểm tra milliseconds
        if ts > 10000000000:
            ts = ts / 1000
        
        # Timestamp từ DNSE đã là VN time
        time_vn = datetime.fromtimestamp(ts)
        time_vn = time_vn.replace(tzinfo=VN_TZ)
        
        # Set giờ phút giây về 00:00:00
        time_vn = time_vn.replace(hour=15, minute=0, second=0, microsecond=0)
        time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")

        print(f"📥 Received [{raw_symbol}] -> [{mapped_symbol}] {time_vn_str} VN -> open={data.get('open')} close={data.get('close')}")

        # Lưu DB và publish Redis dùng mapped_symbol
        upsert_1d(mapped_symbol, data)

        redis_payload = {
    "function": "chart_1d",
    "symbol": mapped_symbol.upper(),
    "time": time_vn_str,
    "open": float(data.get("open", 0)),
    "close": float(data.get("close", 0)),
    "high": float(data.get("high", 0)),
    "low": float(data.get("low", 0)),
    "volume": float(data.get("volume", 0)),
    "exchange": "DERIVATIVE"
}
        redis_client.publish(REDIS_CHANNEL, json.dumps(redis_payload))
        print(f"📤 Published to Redis channel '{REDIS_CHANNEL}'")

    except Exception as e:
        print(f"⚠️ Error in on_message: {e}")

client.on_connect = on_connect
client.on_message = on_message

# ==================== REDIS SUB TEST (DEBUG) ====================
def redis_subscriber_test():
    sub = redis_client.pubsub()
    sub.subscribe(REDIS_CHANNEL)
    print(f"👂 Redis subscriber listening on channel: {REDIS_CHANNEL}")
    for message in sub.listen():
        if message["type"] == "message":
            print(f"🔄 [Redis Received]: {message['data'].decode()}")

threading.Thread(target=redis_subscriber_test, daemon=True).start()

# ==================== MQTT LOOP ====================
client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
client.loop_start()

while True:
    time.sleep(1)