import os, json, time, logging, signal, sys, redis
import paho.mqtt.client as mqtt
from datetime import datetime,timezone, time as dtime
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
            topic = f"plaintext/quotes/krx/mdds/topprice/v1/roundlot/symbol/{sym}"
            client.subscribe(topic, qos=1)
            logging.info(f"📡 Subscribed: {sym}")
    else:
        logging.info(f"❌ Failed to connect, return code {rc}")

def _mk_side(levels, side: str, sym: str):
    sym = (sym or "").upper()
    x10 = sym in x10_list

    items = []
    for x in (levels or []):
        try:
            p = float(x.get("price"))          # GIỮ NGUYÊN
            q = float(x.get("qtty", 0))
            if x10:
                q *= 10.0                      # ✅ x10_list nhân 10
            items.append((p, q))
        except Exception:
            pass

    items.sort(key=lambda t: t[0], reverse=(side == "bid"))

    prices = [None, None, None]
    vols   = [None, None, None]
    for i, (p, v) in enumerate(items[:3]):
        prices[i] = p
        vols[i]   = v
    return prices, vols

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

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
        sym = data.get('symbol')

        b_price, b_vol = _mk_side(data.get("bid"), "bid", sym)
        s_price, s_vol = _mk_side(data.get("offer"), "offer", sym)
        sending_time = data.get("sendingTime")  # DNSE key
        time = fmt_time_z_to_minute(sending_time) if sending_time else None
        z = lambda v: None if (v in (0, 0.0, "0")) else v

        result = {
            "function": "dnse_asset",
            "content": {
                "symbol": sym,
                "time": time,

                "buyPrice1": z(b_price[0]), "buyVol1": z(b_vol[0]),
                "buyPrice2": z(b_price[1]), "buyVol2": z(b_vol[1]),
                "buyPrice3": z(b_price[2]), "buyVol3": z(b_vol[2]),

                "sellPrice1": z(s_price[0]), "sellVol1": z(s_vol[0]),
                "sellPrice2": z(s_price[1]), "sellVol2": z(s_vol[1]),
                "sellPrice3": z(s_price[2]), "sellVol3": z(s_vol[2]),
            }
        }

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