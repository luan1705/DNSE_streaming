# common_shared.py
import logging
from datetime import date
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ============ Logging ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ============ DB ============
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech",
    echo=False,
    pool_pre_ping=True,
)
metadata = MetaData()

eboard=Table(
    "dnse_asset", metadata,
    Column('symbol', String, primary_key=True),
    # Column('exchange', String),
    # Column('indices', String),
    Column('ceiling', Float),
    Column('floor', Float),
    Column('refPrice', Float),
    Column('buyPrice3', Float), Column('buyVol3', Float),
    Column('buyPrice2', Float), Column('buyVol2', Float),
    Column('buyPrice1', Float), Column('buyVol1', Float),
    Column('matchPrice', Float), Column('matchVol', Float),
    Column('matchChange', Float), Column('matchRatioChange', Float),
    Column('sellPrice1', Float), Column('sellVol1', Float),
    Column('sellPrice2', Float), Column('sellVol2', Float),
    Column('sellPrice3', Float), Column('sellVol3', Float),
    Column('totalVol', Float), Column('totalVal', Float),
    Column('high', Float), Column('low', Float), Column('open', Float), Column('close', Float),
    Column("foreignBuyVol", Float),
    Column("foreignSellVol", Float),
    Column("foreignRoom", Float),
    Column("foreignBuyVal", Float),
    Column("foreignSellVal", Float),
    schema="details"
)

mi_table = Table(
    "dnse_vietnam", metadata,
    Column("symbol", String, primary_key=True),
    Column("point", Float),
    Column("refPoint", Float),
    Column("change", Float),
    Column("ratioChange", Float),
    Column("totalMatchVol", Float),
    Column("totalMatchVal", Float),
    Column("totalDealVol", Float),
    Column("totalDealVal", Float),
    Column("totalVol", Float),
    Column("totalVal", Float),
    Column("advancers", Integer),
    Column("noChange", Integer),
    Column("decliners", Integer),
    Column("advancersVal", Float),
    Column("noChangeVal", Float),
    Column("declinersVal", Float),
    Column("ceiling", Integer),
    Column("floor", Integer),
    # Column("open", Float),
    # Column("close", Float),
    # Column("high", Float),
    # Column("low", Float),
    # Column("vol", Float),
    # Column("val", Float),
    schema="indices"
)

metadata.create_all(engine)

def upsert_eboard(row: dict):
    with engine.begin() as conn:
        stmt = pg_insert(eboard).values([row])
        update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
        conn.execute(stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict))

def update_eboard(row: dict):
    """UPDATE history_data.eboard theo symbol. Trả về số dòng được cập nhật."""
    symbol = row.get("symbol")
    if not symbol:
        return 0
    vals = {k: v for k, v in row.items() if k != "symbol"}  # không set lại khóa
    with engine.begin() as conn:
        res = conn.execute(
            eboard.update()
                  .where(eboard.c.symbol == symbol)
                  .values(**vals)
        )
        return res.rowcount

def upsert_mi(row: dict):
    with engine.begin() as conn:
        stmt = pg_insert(mi_table).values([row])
        update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
        conn.execute(stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict))

def update_price_now(price_now: dict):
    symbol = price_now.get("symbol")
    value  = price_now.get("price_now")

    if not symbol:
        logging.error("update_price_now: missing symbol")
        return 0

    sql = text("""
        UPDATE status.break
        SET price_now = :value
        WHERE symbol = :symbol;
    """)

    try:
        with engine.begin() as conn:
            result = conn.execute(sql, {"symbol": symbol, "value": value})
        return result.rowcount   # số dòng update được
    except Exception as e:
        logging.error(f"update_price_now error: {e}")
        return 0
