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

# ==================== IMPORT DANH SÁCH SYMBOL ====================
# ✅ Giả sử file List/exchange.py có sẵn các list: HNXBOND, INDEX_LIST

# Thêm các chỉ số cần subscribe
INDEX_LIST = ["VNINDEX", "VN30", "HNX", "HNX30", "UPCOM", "VNXAllShare", "VN100"]

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

last_data = {}
last_update_minute = {}

# ==================== SPECIAL NAME MAPPING ====================
special_map = {
    "HNX": "HNXINDEX",
    "hnx": "HNXINDEX",
    "UPCOM": "UPCOMINDEX",
    "vnindex": "VNINDEX",
    "VNXAllShare": "VNXALLSHARE",
    "vnallshare": "VNXALLSHARE",
    "HNX30": "HNX30",
    "hnx30": "HNX30"
}

def normalize_symbol(symbol: str) -> str:
    """
    Chuẩn hóa tên symbol:
    - Strip whitespace, uppercase
    - Áp dụng special_map nếu có
    """
    s = symbol.strip().upper().replace(" ", "")
    return special_map.get(s, s)


# ==================== UPSERT 1D (VN TIMEZONE) - TIME SET TO 00:00:00 ====================
def upsert_1d(symbol, data):
    try:
        print(f"🧩 UPSERT CALLED for {symbol}: {data}")

        ts = int(data.get("time") or data.get("timestamp"))
        
        # Kiểm tra nếu là milliseconds (> 10 tỷ)
        if ts > 10000000000:
            ts = ts / 1000
        
        # Timestamp từ DNSE đã tính theo giờ local server (VN)
        # Tạo datetime và gán timezone VN
        time_vn = datetime.fromtimestamp(ts)
        time_vn = time_vn.replace(tzinfo=VN_TZ)
        
        # Set giờ phút giây về 00:00:00 cho daily data
        time_vn = time_vn.replace(hour=15, minute=0, second=0, microsecond=0)
        
        # String để log
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

            base = {
                "symbol": symbol.upper(),
                "time": time_vn,  # TIMESTAMP WITH TIME ZONE at 00:00:00
                "open": float(data.get("open", 0)),
                "close": float(data.get("close", 0)),
                "high": float(data.get("high", 0)),
                "low": float(data.get("low", 0)),
                "volume": int(data.get("volume", 0)),
                # "exchange": "INDEX"
            }

            conn.execute(text(f"""
                INSERT INTO {table} (symbol, time, open, close, high, low, volume)
                VALUES (:symbol, :time, :open, :close, :high, :low, :volume)
                ON CONFLICT (time) DO UPDATE SET
                    open = EXCLUDED.open,
                    close = EXCLUDED.close,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    volume = EXCLUDED.volume;
            """), base)

        print(f"✅ [1D] Upserted {symbol.upper()} ({time_vn_str} VN)")

    except Exception as e:
        print(f"⚠️ [1D] DB error for {symbol}: {e}")


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
client_id = f"dnse-ohlc-index-1d-{randint(1000,9999)}"

client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311, transport="websockets")
client.username_pw_set(investor_id, token)
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.ws_set_options(path="/wss")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ Connected to MQTT (1D)!")

        # ✅ Sub tất cả chỉ số
        for sym in INDEX_LIST:
            topic = f"plaintext/quotes/krx/mdds/v2/ohlc/index/{RESOLUTION}/{sym.replace(' ', '')}"
            client.subscribe(topic, qos=1)
            print("📡 Subscribed:", topic)
    else:
        print("❌ MQTT connect failed:", rc)

def on_message(client, userdata, msg):
    global last_data, last_update_minute
    try:
        data = json.loads(msg.payload.decode())
        symbol = data.get("symbol")
        if not symbol:
            print(f"⚠️ Unknown payload on topic {msg.topic}: {data}")
            return

        norm_symbol = normalize_symbol(symbol)
        
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

        print(f"📥 Received [{norm_symbol}] {time_vn_str} VN -> open={data.get('open')} close={data.get('close')}")
        upsert_1d(norm_symbol, data)

        redis_payload = {
    "function": "chart_1d",
    "symbol": norm_symbol,
    "time": time_vn_str,
    "open": float(data.get("open", 0)),
    "close": float(data.get("close", 0)),
    "high": float(data.get("high", 0)),
    "low": float(data.get("low", 0)),
    "volume": float(data.get("volume", 0))
    # "exchange": "INDEX"
}

        result = redis_client.publish(REDIS_CHANNEL, json.dumps(redis_payload))
        print(f"📤 Published to Redis channel '{REDIS_CHANNEL}' (subscribers={result})")

        last_data[norm_symbol] = data
        last_update_minute[norm_symbol] = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M")

    except Exception as e:
        print(f"⚠️ Error in on_message: {e}")


client.on_connect = on_connect
client.on_message = on_message

# ==================== REDIS SUB TEST ====================
def redis_subscriber_test():
    sub = redis_client.pubsub()
    sub.subscribe(REDIS_CHANNEL)
    print(f"👂 Redis subscriber test listening on channel: {REDIS_CHANNEL}")
    for message in sub.listen():
        if message["type"] == "message":
            print(f"🔄 [Redis Received]: {message['data'].decode()}")

threading.Thread(target=redis_subscriber_test, daemon=True).start()

# ==================== TIMER KIỂM TRA DỮ LIỆU ====================
def check_missing_data():
    global last_data, last_update_minute
    while True:
        now_vn = datetime.now(VN_TZ)
        print(f"🕒 check_missing_data running... {now_vn.strftime('%H:%M:%S')} VN")
        time.sleep(60)
        current_min = now_vn.strftime("%Y-%m-%d %H:%M")
        for sym in INDEX_LIST:
            norm_sym = normalize_symbol(sym)
            if norm_sym in last_data and last_update_minute.get(norm_sym) != current_min:
                print(f"⚠️ No new data for {norm_sym} ({current_min}), reusing last tick")
                new_data = last_data[norm_sym].copy()
                new_data["time"] = int(datetime.strptime(current_min, "%Y-%m-%d %H:%M").timestamp())
                upsert_1d(norm_sym, new_data)
                last_update_minute[norm_sym] = current_min

# ==================== MQTT LOOP ====================
client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
client.loop_start()

while True:
    time.sleep(1)