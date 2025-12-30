# producer_x_keep_result.py
import os, json, time, logging, signal, sys, redis
import paho.mqtt.client as mqtt
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from random import randint
import ssl
from requests import post, get

# ====== IMPORTS THEO DỰ ÁN CỦA BẠN ======
from List.exchange import x10_list,still_list
# =========================================

# ---------- Cấu hình qua ENV ----------
REDIS_URL   = "redis://default:%40Vns123456@videv.cloud:6379/1"
CHANNEL = "DNSE_streaming"
# --------------------------------------

#------------------------------Cấu hình redis----------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=2.5,           # timeout đọc/ghi
    socket_connect_timeout=2.0,   # timeout connect
    health_check_interval=30,     # ping định kỳ 30s
    max_connections=3,            # Mỗi container chỉ tối đa 3 socket tới Redis
    timeout=1.0,                  # Khi pool bận, chờ tối đa 1s để lấy connection (không drop)
)
r = redis.Redis(connection_pool=POOL)

def publish(payload: dict):
    global r
    if "source" not in payload:
        payload["source"] = CHANNEL
    try:
        r.publish(CHANNEL, json.dumps(payload, ensure_ascii=False))
    except Exception as e:
        logging.warning("Redis publish fail (%s): %s", CHANNEL, e)
        try:
            # reconnect Redis
            r = redis.Redis(connection_pool=POOL)
            r.publish(CHANNEL, json.dumps(payload, ensure_ascii=False))
            logging.info("Redis reconnected and published successfully")
        except Exception as e2:
            logging.error("Redis retry failed: %s", e2)

#----------------------------------------------------------------------------------------------------------

#==============================================DNSE streaming===============================================
username = "064C26054V"
password = "@Vns123456"

def authenticate(username, password):
    try:
        url = "https://api.dnse.com.vn/user-service/api/auth"
        response = post(url, json={"username": username, "password": password})
        response.raise_for_status()
        logging.info("✅ Authentication successful!")
        return response.json().get("token")
    except Exception as e:
        logging.error(f"❌ Authentication failed: {e}")
        return None

def get_investor_info(token):
    try:
        url = "https://api.dnse.com.vn/user-service/api/me"
        headers = {"authorization": f"Bearer {token}"}
        response = get(url, headers=headers)
        response.raise_for_status()
        logging.info("✅ Get investor info successful!")
        return response.json()
    except Exception as e:
        logging.error(f"❌ Failed to get investor info: {e}")
        return None

token = authenticate(username, password)
if not token:
    exit()

investor_info = get_investor_info(token)
if not investor_info:
    exit()

investor_id = investor_info["investorId"]
logging.info("InvestorID: %s", investor_id)

BROKER_HOST = "datafeed-lts-krx.dnse.com.vn"
BROKER_PORT = 443
CLIENT_ID_PREFIX = "dnse-price-json-mqtt-ws-sub-"
client_id = f"{CLIENT_ID_PREFIX}{randint(1000, 9999)}"

client = mqtt.Client(
    client_id=client_id,
    protocol=mqtt.MQTTv311,
    transport="websockets"
)
client.username_pw_set(investor_id, token)
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.ws_set_options(path="/wss")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("✅ Connected to MQTT Broker!")
        symbols = x10_list + still_list

        for sym in symbols:
            topic = f"plaintext/quotes/krx/mdds/stockinfo/v1/roundlot/symbol/{sym}"
            client.subscribe(topic, qos=1)
            logging.info(f"📡 Subscribed: {sym}")
    else:
        logging.info(f"❌ Failed to connect, return code {rc}")

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
def fmt_time_z_to_minute(ts: str) -> str | None:
    if not ts:
        return None
    ts = ts.replace("Z", "+00:00")
    # nếu có >6 chữ số sau dấu chấm thì cắt bớt
    if "." in ts:
        head, tail = ts.split(".", 1)
        frac = tail.split("+", 1)[0].split("-", 1)[0]
        if len(frac) > 6:
            ts = head + "." + frac[:6] + tail[len(frac):]
    dt = datetime.fromisoformat(ts).astimezone(VN_TZ)
    dt = dt.replace(second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def to_float(x):
    # thiếu/None/"" -> None, còn lại -> float
    if x is None:
        return None
    try:
        if isinstance(x, str):
            x = x.strip()
            if x == "":
                return None
        return float(x)
    except Exception:
        return None

def to_int(x):
    v = to_float(x)
    return None if v is None else int(v)

def round2(x):
    return None if x is None else round(x, 2)

def mul(x, k):
    return None if x is None else x * k


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
        mp = data.get("matchPrice", None)
        if mp is None:
            return

        sym = data.get('symbol')
        time= fmt_time_z_to_minute(data.get("tradingTime") or "")
        k = 10

        result = {
            "function": "dnse_asset",
            "content": {
                "symbol": sym,
                "time":  time,
                "ceiling":  round2(to_float(data.get("highLimitPrice"))),
                "floor":    round2(to_float(data.get("lowLimitPrice"))),
                "refPrice": round2(to_float(data.get("referencePrice"))),
            
                "matchPrice": round2(mp),
                "matchVol":   round2(mul(to_float(data.get("matchQuantity")), k)),
                "matchChange": round2(to_float(data.get("changedValue"))),
                "matchRatioChange": round2(to_float(data.get("changedRatio"))),
            
                "totalVol": round2(mul(to_float(data.get("totalVolumeTraded")), k)),
                "totalVal": round2(mul(to_float(data.get("grossTradeAmount")), 1_000_000_000)),
            
                "high":  round2(to_float(data.get("highestPrice"))),
                "low":   round2(to_float(data.get("lowestPrice"))),
                "open":  round2(to_float(data.get("openPrice"))),
                "close": round2(to_float(data.get("closePrice"))),
            
                "foreignBuyVol":  mul(to_int(data.get("buyForeignQuantity")), k),
                "foreignSellVol": mul(to_int(data.get("sellForeignQuantity")), k),
                "foreignRoom":    mul(to_int(data.get("foreignerOrderLimitQuantity")), k),
                "foreignBuyVal":  mul(to_float(data.get("buyForeignValue")), 1_000_000_000),
                "foreignSellVal": mul(to_float(data.get("sellForeignValue")), 1_000_000_000),
            }}

        # Publish result sang Redis để Hub gom về 1 WS port
        publish(result)

    except Exception:
        logging.exception("MI message error")
        
def main():
    try:
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=1200)
        client.loop_forever()
    except Exception as e:
        print(f'lỗi {e}')
        time.sleep(2)

if __name__ == "__main__":
    main()