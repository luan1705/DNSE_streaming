import os
import json
import time
import logging
import redis

from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ======================
# ENV
# ======================
REDIS_URL = os.getenv("REDIS_URL", "redis://default:%40Vns123456@videv.cloud:6379/1")
DB_URL    = os.getenv("DB_URL", "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech")

FLUSH_INTERVAL_MS = int(os.getenv("FLUSH_INTERVAL_MS", "200"))
MAX_BUFFER_SIZE   = int(os.getenv("MAX_BUFFER_SIZE", "5000"))

# ======================
# Redis
# ======================
POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=60,
    socket_connect_timeout=5,
    health_check_interval=30,
    max_connections=10,
    timeout=1.0,
)
r = redis.Redis(connection_pool=POOL)

# ======================
# Postgres
# ======================
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "0")),
    pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "10")),
    pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "1800")),
    connect_args={"application_name": "db_writer_batch"},
    echo=False,
)

md = MetaData()
asset      = Table("dnse_asset", md, schema="details", autoload_with=engine)
indices_tbl = Table("dnse_vietnam", md, schema="indices", autoload_with=engine)

# ======================
# SQL
# ======================
SQL_ENSURE_ASSET = text("""
    INSERT INTO "details".dnse_asset(symbol)
    VALUES (:symbol)
    ON CONFLICT (symbol) DO NOTHING
""")

# INFO snapshot (flat)
SQL_UPDATE_INFO = text("""
    UPDATE "details".dnse_asset
    SET
      "ceiling"  = COALESCE(:ceiling, "ceiling"),
      "floor"    = COALESCE(:floor, "floor"),
      "refPrice" = COALESCE(:refPrice, "refPrice"),

      "matchPrice"       = COALESCE(:matchPrice, "matchPrice"),
      "matchVol"         = COALESCE(:matchVol, "matchVol"),
      "matchChange"      = COALESCE(:matchChange, "matchChange"),
      "matchRatioChange" = COALESCE(:matchRatioChange, "matchRatioChange"),

      "totalVol" = COALESCE(:totalVol, "totalVol"),
      "totalVal" = COALESCE(:totalVal, "totalVal"),

      "high"  = COALESCE(:high, "high"),
      "low"   = COALESCE(:low, "low"),
      "open"  = COALESCE(:open, "open"),
      "close" = COALESCE(:close, "close"),
      "foreignBuyVol"  = COALESCE(:foreignBuyVol, "foreignBuyVol"),
      "foreignSellVol" = COALESCE(:foreignSellVol, "foreignSellVol"),
      "foreignRoom"    = COALESCE(:foreignRoom, "foreignRoom"),
      "foreignBuyVal"  = COALESCE(:foreignBuyVal, "foreignBuyVal"),
      "foreignSellVal" = COALESCE(:foreignSellVal, "foreignSellVal")
                       
    WHERE symbol = :symbol
""")

# TOPPRICE orderbook (flat)
SQL_UPDATE_TOPPRICE = text("""
    UPDATE "details".dnse_asset
    SET
      "buyPrice1" = :buyPrice1, "buyVol1" = :buyVol1,
      "buyPrice2" = :buyPrice2, "buyVol2" = :buyVol2,
      "buyPrice3" = :buyPrice3, "buyVol3" = :buyVol3,

      "sellPrice1" = :sellPrice1, "sellVol1" = :sellVol1,
      "sellPrice2" = :sellPrice2, "sellVol2" = :sellVol2,
      "sellPrice3" = :sellPrice3, "sellVol3" = :sellVol3
    WHERE symbol = :symbol
""")

# ======================
# Indices upsert
# ======================
def upsert_many(conn, tbl: Table, rows: list[dict], pk: str = "symbol"):
    if not rows:
        return
    stmt = pg_insert(tbl).values(rows)
    update_dict = {c.name: getattr(stmt.excluded, c.name) for c in tbl.columns if c.name != pk}
    stmt = stmt.on_conflict_do_update(index_elements=[pk], set_=update_dict)
    conn.execute(stmt)

# ======================
# Mapping
# ======================
def content_to_row_info(content: dict) -> dict:
    return {
        "symbol": content.get("symbol"),

        "ceiling":  content.get("ceiling"),
        "floor":    content.get("floor"),
        "refPrice": content.get("refPrice"),

        "matchPrice": content.get("matchPrice"),
        "matchVol": content.get("matchVol"),
        "matchChange": content.get("matchChange"),
        "matchRatioChange": content.get("matchRatioChange"),

        "totalVol": content.get("totalVol"),
        "totalVal": content.get("totalVal"),

        "high": content.get("high"),
        "low":  content.get("low"),
        "open": content.get("open"),
        "close": content.get("close"),

        "foreignBuyVol": content.get("foreignBuyVol"),
        "foreignSellVol": content.get("foreignSellVol"),
        "foreignRoom": content.get("foreignRoom"),
        "foreignBuyVal": content.get("foreignBuyVal"),
        "foreignSellVal": content.get("foreignSellVal"),
    }

def content_to_row_topprice(content: dict) -> dict:
    return {
        "symbol": content.get("symbol"),

        "buyPrice1": content.get("buyPrice1"), "buyVol1": content.get("buyVol1"),
        "buyPrice2": content.get("buyPrice2"), "buyVol2": content.get("buyVol2"),
        "buyPrice3": content.get("buyPrice3"), "buyVol3": content.get("buyVol3"),

        "sellPrice1": content.get("sellPrice1"), "sellVol1": content.get("sellVol1"),
        "sellPrice2": content.get("sellPrice2"), "sellVol2": content.get("sellVol2"),
        "sellPrice3": content.get("sellPrice3"), "sellVol3": content.get("sellVol3"),
    }

def content_to_row_indices(content: dict) -> dict:
    return {
        "symbol": content.get("symbol"),
        "point": content.get("point"),
        "refPoint": content.get("refPoint"),
        "change": content.get("change"),
        "ratioChange": content.get("ratioChange"),

        "totalMatchVol": content.get("totalMatchVol"),
        "totalMatchVal": content.get("totalMatchVal"),
        "totalDealVol": content.get("totalDealVol"),
        "totalDealVal": content.get("totalDealVal"),
        "totalVol": content.get("totalVol"),
        "totalVal": content.get("totalVal"),

        "advancers": content.get("advancers"),
        "noChanges": content.get("noChanges"),
        "decliners": content.get("decliners"),

        "advancersVal": content.get("advancersVal"),
        "noChangesVal": content.get("noChangesVal"),
        "declinersVal": content.get("declinersVal"),

        "ceiling": content.get("ceiling"),
        "floor": content.get("floor"),
        "open": content.get("open"),
        "high": content.get("high"),
        "low": content.get("low"),
        "close": content.get("close"),
    }

# ======================
# Helpers: detect message type
# ======================
def is_info_asset(content: dict) -> bool:
    if "matchPrice" in content:
        return True

    for k in (
        "ceiling","floor","refPrice","totalVol","totalVal","high","low","open","close",
        "matchVol","matchChange","matchRatioChange",
        "foreignBuyVol","foreignSellVol","foreignRoom","foreignBuyVal","foreignSellVal",
    ):
        if k in content:
            return True
    return False

def is_topprice_asset(content: dict) -> bool:
    return any(k in content for k in ("buyPrice1", "sellPrice1", "buyVol1", "sellVol1"))

# ======================
# Main
# ======================
def main():
    pubsub = r.pubsub()
    pubsub.subscribe("DNSE_asset", "DNSE_indices")
    logging.info("DB_WRITER listening channels=%s ...", ["DNSE_asset", "DNSE_indices"])

    info_buf: dict[str, dict]     = {}
    topprice_buf: dict[str, dict] = {}
    indices_buf: dict[str, dict]  = {}

    last_flush = time.time()
    last_log = time.time()
    processed = 0

    def flush():
        nonlocal last_flush

        if not (info_buf or topprice_buf or indices_buf):
            last_flush = time.time()
            return

        info_rows = list(info_buf.values()); info_buf.clear()
        top_rows  = list(topprice_buf.values()); topprice_buf.clear()
        i_rows    = list(indices_buf.values()); indices_buf.clear()

        symbols = set()
        for r0 in info_rows: symbols.add(r0["symbol"])
        ensure_rows = [{"symbol": s} for s in symbols if s]

        t0 = time.time()
        try:
            with engine.begin() as conn:
                if ensure_rows:
                    conn.execute(SQL_ENSURE_ASSET, ensure_rows)

                if info_rows:
                    conn.execute(SQL_UPDATE_INFO, info_rows)

                if top_rows:
                    conn.execute(SQL_UPDATE_TOPPRICE, top_rows)

                if i_rows:
                    upsert_many(conn, indices_tbl, i_rows, pk="symbol")

        except Exception:
            logging.exception("FLUSH error")

        dt = (time.time() - t0) * 1000
        last_flush = time.time()
        if dt > 200:
            logging.warning("flush took %.1fms (info=%d top=%d indices=%d)",
                            dt, len(info_rows), len(top_rows), len(i_rows))

    while True:
        msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
        if msg is None:
            if (time.time() - last_flush) * 1000 >= FLUSH_INTERVAL_MS:
                flush()
            continue

        if msg.get("type") != "message":
            continue

        data_str = msg.get("data")
        if not data_str:
            continue

        try:
            raw = json.loads(data_str)
            ch = msg.get("channel")
            if isinstance(ch, bytes):
                ch = ch.decode()
        except Exception:
            continue

        content = raw
        symbol = content.get("symbol")
        if not symbol:
            continue

        if ch == "DNSE_asset":
            if is_topprice_asset(content):
                topprice_buf[symbol] = content_to_row_topprice(content)
            elif is_info_asset(content):
                info_buf[symbol] = content_to_row_info(content)

        elif ch == "DNSE_indices":
            indices_buf[symbol] = content_to_row_indices(content)

        processed += 1

        if (len(info_buf) + len(topprice_buf) + len(indices_buf)) >= MAX_BUFFER_SIZE:
            flush()

        if (time.time() - last_flush) * 1000 >= FLUSH_INTERVAL_MS:
            flush()

        if time.time() - last_log >= 5:
            logging.info("processed=%d buffers: info=%d top=%d indices=%d",
                         processed, len(info_buf), len(topprice_buf), len(indices_buf))
            last_log = time.time()

if __name__ == "__main__":
    main()
