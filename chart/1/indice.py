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

# ==================== IMPORT DANH SÁCH INDEX ====================
INDEX_LIST = ["VNINDEX", "VN30", "HNX", "HNX30", "UPCOM", "VNXAllShare", "VN100"]

# ==================== CONFIG ====================
username = "064CCS7GUK"
password = "199204@Vie"
DB_URL = "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"
SCHEMA = "ohlcv"
RESOLUTION = "1"
REDIS_URL = "redis://root:Dnl_123456@tanhungsoft.com:6379"
REDIS_CHANNEL = "ohlcv_1"

engine = create_engine(DB_URL)
redis_client = redis.Redis.from_url(REDIS_URL)

# ==================== KIỂM TRA REDIS ====================
try:
    redis_client.ping()
    print("✅ Connected to Redis!")
except Exception as e:
    print("❌ Redis connection failed:", e)
    exit(1)

# ==================== MAP SYMBOL ĐẶC BIỆT ====================
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

# ==================== GIỚI HẠN GIỜ GIAO DỊCH (VN TIMEZONE) ====================
def is_trading_time_vn():
    """
    Trả về True nếu đang trong giờ giao dịch hợp lệ (VN),
    False nếu là trước 9h15 VN, nghỉ trưa hoặc sau ATC.
    """
    now = datetime.now(VN_TZ)
    hm = now.hour + now.minute / 60

    # Trước 9h15
    if hm < 9 + 15/60:
        return False

    # Nghỉ trưa: 11h30–13h00
    if 11.5 <= hm < 13:
        return False

    # Sau 14h45
    if hm > 14.75:
        return False

    return True


# ==================== UPSERT 1M (VN TIMEZONE) ====================
def upsert_1m(symbol, data):
    try:
        symbol = normalize_symbol(symbol)
        ts = int(data.get("time") or data.get("timestamp"))
        
        # Kiểm tra nếu là milliseconds (> 10 tỷ)
        if ts > 10000000000:
            ts = ts / 1000
        
        # Timestamp từ DNSE đã tính theo giờ local server (VN)
        # Tạo datetime và gán timezone VN
        time_vn = datetime.fromtimestamp(ts)
        time_vn = time_vn.replace(tzinfo=VN_TZ)
        
        # Làm tròn xuống phút
        time_vn = time_vn.replace(second=0, microsecond=0)
        
        # String để log
        time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")
        
        table = f'"{SCHEMA}"."{symbol}_1"'

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
                "symbol": symbol,
                "time": time_vn,  # TIMESTAMP WITH TIME ZONE (UTC+7)
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

        print(f"✅ [1M] Upserted {symbol} ({time_vn_str} VN)")
    except Exception as e:
        print(f"⚠️ [1M] DB error for {symbol}: {e}")

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
client_id = f"dnse-ohlc-index-1m-{randint(1000,9999)}"

client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311, transport="websockets")
client.username_pw_set(investor_id, token)
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.ws_set_options(path="/wss")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ Connected to MQTT (1M)!")
        for sym in INDEX_LIST:
            topic = f"plaintext/quotes/krx/mdds/v2/ohlc/index/{RESOLUTION}/{sym.replace(' ', '')}"
            client.subscribe(topic, qos=1)
            print("📡 Subscribed:", topic)
    else:
        print("❌ MQTT connect failed:", rc)

def on_message(client, userdata, msg):
    try:
        # Skip ngoài giờ hoặc trước 9h15
        if not is_trading_time_vn():
            print(f"⏭️ [SKIP] {datetime.now(VN_TZ).strftime('%H:%M:%S')} VN | Ngoài giờ giao dịch, bỏ qua dữ liệu.")
            return

        data = json.loads(msg.payload.decode())
        symbol = data.get("symbol")
        if not symbol:
            return

        norm_symbol = normalize_symbol(symbol)
        
        ts = int(data["time"])
        
        # Kiểm tra milliseconds
        if ts > 10000000000:
            ts = ts / 1000
        
        # Timestamp từ DNSE đã là VN time
        time_vn = datetime.fromtimestamp(ts)
        time_vn = time_vn.replace(tzinfo=VN_TZ)
        time_vn = time_vn.replace(second=0, microsecond=0)
        time_vn_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")

        print(f"📥 Received [{norm_symbol}] {time_vn_str} VN -> open={data.get('open')} close={data.get('close')}")
        upsert_1m(norm_symbol, data)

        redis_payload = {
            "function": "chart_1m",
            "symbol": norm_symbol,
            "time": time_vn_str,
            "open": data.get("open"),
            "close": data.get("close"),
            "high": data.get("high"),
            "low": data.get("low"),
            "volume": float(data.get("volume", 0)),
            # "exchange": "INDEX"
        }
        redis_client.publish(REDIS_CHANNEL, json.dumps(redis_payload))
        print(f"📤 Published to Redis channel '{REDIS_CHANNEL}'")

    except Exception as e:
        print(f"⚠️ Error in on_message: {e}")

client.on_connect = on_connect
client.on_message = on_message

# ==================== MQTT LOOP ====================
client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
client.loop_start()

# Giữ chương trình chạy
while True:
    time.sleep(1)