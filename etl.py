
"""
Real-time Bitcoin ETL (Blockstream API) with incremental datamarts.

Features:
- Polls Blockstream API for new blocks
- Extract -> Transform -> Load into Postgres raw tables
- Incrementally updates datamart tables per block (created IF NOT EXISTS)
- Uses bulk inserts (execute_batch) for performance
- Uses a single transaction for mart updates
- Checkpointing in `etl_state` table
- Start from START_HEIGHT, START_DATE, or resume from checkpoint
- Graceful shutdown (KeyboardInterrupt)
- Robust logging (no emojis to avoid encoding errors)
"""

import requests
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import pandas as pd
import logging
import sys
import os
import traceback
import time
from typing import Optional


# CONFIGURATION

BLOCKSTREAM_API = "https://blockstream.info/api"
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Choose one of:
START_HEIGHT: Optional[int] = None    # or set a block height to start from
START_DATE: Optional[str] = "2021-01-01"  # format YYYY-MM-DD or None

POLL_INTERVAL = 30  # seconds between checks


# LOGGING SETUP 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_bitcoin.log", mode="a", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)


# DB HELPERS

def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

def ensure_state_table():
    """Create table to track last processed block height and ensure datamart schema exists"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS datamart;")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_state (
            id SERIAL PRIMARY KEY,
            last_block_height BIGINT,
            updated_at TIMESTAMP DEFAULT now()
        );
        """)
        conn.commit()
    except Exception:
        conn.rollback()
        logger.error("Failed to ensure etl_state or datamart schema exists")
        logger.error(traceback.format_exc())
        raise
    finally:
        cur.close()
        conn.close()

def get_last_processed_height() -> Optional[int]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT last_block_height FROM etl_state ORDER BY id DESC LIMIT 1;")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    finally:
        cur.close()
        conn.close()

def update_last_processed_height(height: int):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO etl_state (last_block_height) VALUES (%s);", (height,))
        conn.commit()
        logger.info(f"Updated checkpoint: last_block_height = {height}")
    except Exception:
        conn.rollback()
        logger.error(f"Failed to update checkpoint for height {height}")
        logger.error(traceback.format_exc())
        raise
    finally:
        cur.close()
        conn.close()




# BLOCK HEIGHT <-> DATE helper

def get_height_from_date(date_str: str) -> int:
    """
    Use Blockstream endpoint /blocks/{date} to find earliest block for date.
    Returns a block height integer.
    """
    try:
        url = f"{BLOCKSTREAM_API}/blocks/{date_str}"
        resp = requests.get(url)
        resp.raise_for_status()
        blocks = resp.json()
        if not blocks:
            raise Exception(f"No blocks found for {date_str}")
        # API returns most recent first; take the last one for earliest of day
        block = blocks[-1]
        height = int(block["height"])
        logger.info(f"Using block {height} for date {date_str}")
        return height
    except Exception as e:
        logger.error(f"Failed to fetch block height for date {date_str}: {e}")
        logger.error(traceback.format_exc())
        raise


# GET LATEST BLOCK HEIGHT
def get_latest_height() -> int:
    resp = requests.get(f"{BLOCKSTREAM_API}/blocks/tip/height")
    resp.raise_for_status()
    return int(resp.text.strip())




# ETL

# EXTRACT

def extract_block(block_height: int):
    try:
        logger.info(f"Extracting block {block_height}")
        r = requests.get(f"{BLOCKSTREAM_API}/block-height/{block_height}")
        r.raise_for_status()
        block_hash = r.text.strip()
        logger.info(f"Got block hash: {block_hash}")

        
        r2 = requests.get(f"{BLOCKSTREAM_API}/block/{block_hash}")
        r2.raise_for_status()
        block_data = r2.json()


        r3 = requests.get(f"{BLOCKSTREAM_API}/block/{block_hash}/txs")
        r3.raise_for_status()
        txs = r3.json()
        logger.info(f"Found {len(txs)} transactions in block {block_height} (first page)")

        detailed_txs = []
        for tx in txs:
            txid = tx.get("txid")
            if not txid:
                continue
            try:
                rtx = requests.get(f"{BLOCKSTREAM_API}/tx/{txid}")
                rtx.raise_for_status()
                detailed_txs.append(rtx.json())
            except Exception as e:
                logger.error(f"Failed to fetch tx {txid}: {e}")
                logger.error(traceback.format_exc())

        logger.info(f"Extracted {len(detailed_txs)} detailed transactions for block {block_height}")
        return block_data, detailed_txs

    except Exception:
        logger.error("Extract step failed")
        logger.error(traceback.format_exc())
        raise


# TRANSFORM

def transform_block(block_data, detailed_txs):
    try:
        logger.info("Transforming block data")
        block_df = pd.DataFrame([{
            "block_id": block_data.get("id"),
            "height": int(block_data.get("height")),
            "timestamp": pd.to_datetime(block_data.get("timestamp"), unit="s"),
            "tx_count": int(block_data.get("tx_count", 0)),
            "size": int(block_data.get("size", 0)),
            "weight": int(block_data.get("weight", 0))
        }])

        tx_list, input_list, output_list = [], [], []
        address_dict = {}

        for tx in detailed_txs:
            block_time = tx.get("status", {}).get("block_time")
            tx_time = pd.to_datetime(block_time, unit="s") if block_time else None

            tx_list.append({
                "txid": tx.get("txid"),
                "block_id": block_data.get("id"),
                "fee": tx.get("fee"),
                "size": tx.get("size"),
                "weight": tx.get("weight"),
                "version": tx.get("version"),
                "locktime": tx.get("locktime")
            })

            # Inputs
            for vin in tx.get("vin", []):
                prevout = vin.get("prevout") or {}
                input_list.append({
                    "txid": tx.get("txid"),
                    "prev_txid": vin.get("txid"),
                    "prev_index": vin.get("vout"),
                    "input_address": prevout.get("scriptpubkey_address"),
                    "input_value": prevout.get("value")
                })
                addr = prevout.get("scriptpubkey_address")
                if addr:
                    address_dict.setdefault(addr, {
                        "total_received": 0, "total_sent": 0,
                        "first_seen": tx_time, "last_seen": tx_time
                    })
                    address_dict[addr]["total_sent"] += prevout.get("value", 0) or 0
                    address_dict[addr]["last_seen"] = tx_time or address_dict[addr]["last_seen"]

            # Outputs
            for idx, vout in enumerate(tx.get("vout", [])):
                output_list.append({
                    "txid": tx.get("txid"),
                    "vout_index": idx,
                    "output_address": vout.get("scriptpubkey_address"),
                    "output_value": vout.get("value"),
                    "spent": vout.get("spent", False)
                })
                addr = vout.get("scriptpubkey_address")
                if addr:
                    address_dict.setdefault(addr, {
                        "total_received": 0, "total_sent": 0,
                        "first_seen": tx_time, "last_seen": tx_time
                    })
                    address_dict[addr]["total_received"] += vout.get("value", 0) or 0
                    if address_dict[addr]["first_seen"] is None:
                        address_dict[addr]["first_seen"] = tx_time
                    address_dict[addr]["last_seen"] = tx_time or address_dict[addr]["last_seen"]

        tx_df = pd.DataFrame(tx_list) if tx_list else pd.DataFrame(columns=["txid","block_id","fee","size","weight","version","locktime"])
        inputs_df = pd.DataFrame(input_list) if input_list else pd.DataFrame(columns=["txid","prev_txid","prev_index","input_address","input_value"])
        outputs_df = pd.DataFrame(output_list) if output_list else pd.DataFrame(columns=["txid","vout_index","output_address","output_value","spent"])

        addr_list = []
        for addr, stats in address_dict.items():
            balance = (stats.get("total_received", 0) or 0) - (stats.get("total_sent", 0) or 0)
            addr_list.append({
                "address": addr,
                "total_received": stats.get("total_received", 0) or 0,
                "total_sent": stats.get("total_sent", 0) or 0,
                "balance": balance,
                "first_seen": stats.get("first_seen"),
                "last_seen": stats.get("last_seen")
            })
        address_df = pd.DataFrame(addr_list) if addr_list else pd.DataFrame(columns=["address","total_received","total_sent","balance","first_seen","last_seen"])

        logger.info(f"Transform complete: {len(tx_df)} txs, {len(inputs_df)} inputs, {len(outputs_df)} outputs, {len(address_df)} addresses")
        return block_df, tx_df, inputs_df, outputs_df, address_df

    except Exception:
        logger.error("Transform step failed")
        logger.error(traceback.format_exc())
        raise


# LOAD (raw tables) using execute_batch for speed

def load_to_postgres(block_df, tx_df, inputs_df, outputs_df, address_df):
    try:
        logger.info("Loading block and related records into Postgres (raw tables)")
        conn = get_connection()
        cur = conn.cursor()

        # Ensure raw tables exist
        cur.execute("CREATE SCHEMA IF NOT EXISTS public;")  # safe no-op
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bitcoin_blocks (
            block_id TEXT PRIMARY KEY,
            height NUMERIC,
            timestamp TIMESTAMP,
            tx_count NUMERIC,
            size BIGINT,
            weight BIGINT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bitcoin_transactions (
            txid TEXT PRIMARY KEY,
            block_id TEXT,
            fee NUMERIC,
            size BIGINT,
            weight BIGINT,
            version NUMERIC,
            locktime BIGINT,
            FOREIGN KEY(block_id) REFERENCES bitcoin_blocks(block_id)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bitcoin_inputs (
            id SERIAL PRIMARY KEY,
            txid TEXT,
            prev_txid TEXT,
            prev_index NUMERIC,
            input_address TEXT,
            input_value NUMERIC
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bitcoin_outputs (
            id SERIAL PRIMARY KEY,
            txid TEXT,
            vout_index NUMERIC,
            output_address TEXT,
            output_value NUMERIC,
            spent BOOLEAN
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bitcoin_addresses (
            address TEXT PRIMARY KEY,
            total_received NUMERIC,
            total_sent NUMERIC,
            balance NUMERIC,
            first_seen TIMESTAMP,
            last_seen TIMESTAMP
        );
        """)
        conn.commit()

        # Insert block(s) (usually one)
        if not block_df.empty:
            block_rows = [
                (row.block_id, row.height, row.timestamp.to_pydatetime() if not pd.isna(row.timestamp) else None,
                 row.tx_count, row.size, row.weight)
                for _, row in block_df.iterrows()
            ]
            execute_batch(cur,
                """
                INSERT INTO bitcoin_blocks (block_id, height, timestamp, tx_count, size, weight)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (block_id) DO NOTHING;
                """,
                block_rows, page_size=100)

        # Insert transactions
        if not tx_df.empty:
            tx_rows = [
                (row.txid, row.block_id, row.fee, row.size, row.weight, row.version, row.locktime)
                for _, row in tx_df.iterrows()
            ]
            execute_batch(cur,
                """
                INSERT INTO bitcoin_transactions (txid, block_id, fee, size, weight, version, locktime)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (txid) DO NOTHING;
                """,
                tx_rows, page_size=200)

        # Insert inputs
        if not inputs_df.empty:
            input_rows = [
                (row.txid, row.prev_txid, row.prev_index, row.input_address, row.input_value)
                for _, row in inputs_df.iterrows()
            ]
            execute_batch(cur,
                """
                INSERT INTO bitcoin_inputs (txid, prev_txid, prev_index, input_address, input_value)
                VALUES (%s, %s, %s, %s, %s)
                """,
                input_rows, page_size=500)

        # Insert outputs
        if not outputs_df.empty:
            output_rows = [
                (row.txid, row.vout_index, row.output_address, row.output_value, row.spent)
                for _, row in outputs_df.iterrows()
            ]
            execute_batch(cur,
                """
                INSERT INTO bitcoin_outputs (txid, vout_index, output_address, output_value, spent)
                VALUES (%s, %s, %s, %s, %s)
                """,
                output_rows, page_size=500)

        # Upsert addresses: we store cumulative totals coming from transform
        if not address_df.empty:
            addr_rows = [
                (row.address, row.total_received, row.total_sent, row.balance, 
                 (row.first_seen.to_pydatetime() if not pd.isna(row.first_seen) else None),
                 (row.last_seen.to_pydatetime() if not pd.isna(row.last_seen) else None))
                for _, row in address_df.iterrows()
            ]
            # We'll upsert by summing totals (the transform already computed per-block deltas)
            execute_batch(cur,
                """
                INSERT INTO bitcoin_addresses (address, total_received, total_sent, balance, first_seen, last_seen)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (address)
                DO UPDATE SET
                    total_received = bitcoin_addresses.total_received + EXCLUDED.total_received,
                    total_sent = bitcoin_addresses.total_sent + EXCLUDED.total_sent,
                    balance = bitcoin_addresses.balance + EXCLUDED.balance,
                    last_seen = GREATEST(bitcoin_addresses.last_seen, EXCLUDED.last_seen);
                """,
                addr_rows, page_size=200)

        conn.commit()
        logger.info("Load completed into raw tables.")
    except Exception:
        conn.rollback()
        logger.error("Load step failed")
        logger.error(traceback.format_exc())
        raise
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


# DATAMART UPDATERS
# Note: these functions do NOT commit; commit happens in update_marts transaction wrapper

def dm_update_block_stats(cur, block_height: int):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS datamart.mart_block_stats (
        height BIGINT PRIMARY KEY,
        block_date DATE,
        tx_count NUMERIC,
        avg_fee NUMERIC,
        total_fees NUMERIC,
        avg_tx_size NUMERIC,
        avg_block_size NUMERIC,
        avg_block_weight NUMERIC
    );
    """)
    cur.execute("""
    INSERT INTO datamart.mart_block_stats (height, block_date, tx_count, avg_fee, total_fees, avg_tx_size, avg_block_size, avg_block_weight)
    SELECT 
        b.height,
        b.timestamp::date,
        COUNT(t.txid) AS tx_count,
        COALESCE(AVG(t.fee), 0) AS avg_fee,
        COALESCE(SUM(t.fee), 0) AS total_fees,
        COALESCE(AVG(t.size), 0) AS avg_tx_size,
        COALESCE(AVG(b.size), 0) AS avg_block_size,
        COALESCE(AVG(b.weight), 0) AS avg_block_weight
    FROM bitcoin_blocks b
    LEFT JOIN bitcoin_transactions t ON b.block_id = t.block_id
    WHERE b.height = %s
    GROUP BY b.height, b.timestamp::date
    ON CONFLICT (height) DO NOTHING;
    """, (block_height,))

def dm_update_address_activity(cur, block_height: int):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS datamart.mart_address_activity (
        address TEXT,
        block_height BIGINT,
        total_received NUMERIC,
        total_sent NUMERIC,
        balance NUMERIC,
        PRIMARY KEY(address, block_height)
    );
    """)
    cur.execute("""
    INSERT INTO datamart.mart_address_activity (address, block_height, total_received, total_sent, balance)
    SELECT 
        a.address,
        b.height,
        a.total_received,
        a.total_sent,
        a.balance
    FROM bitcoin_addresses a
    JOIN bitcoin_outputs o ON a.address = o.output_address
    JOIN bitcoin_transactions t ON o.txid = t.txid
    JOIN bitcoin_blocks b ON t.block_id = b.block_id
    WHERE b.height = %s
    ON CONFLICT (address, block_height) DO NOTHING;
    """, (block_height,))

def dm_update_tx_patterns(cur, block_height: int):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS datamart.mart_tx_patterns (
        block_height BIGINT,
        txid TEXT PRIMARY KEY,
        input_count NUMERIC,
        output_count NUMERIC,
        total_input_value NUMERIC,
        total_output_value NUMERIC
    );
    """)
    cur.execute("""
    INSERT INTO datamart.mart_tx_patterns (block_height, txid, input_count, output_count, total_input_value, total_output_value)
    SELECT
        b.height,
        t.txid,
        COALESCE(COUNT(i.id),0) AS input_count,
        COALESCE(COUNT(o.id),0) AS output_count,
        COALESCE(SUM(i.input_value), 0) AS total_input_value,
        COALESCE(SUM(o.output_value), 0) AS total_output_value
    FROM bitcoin_transactions t
    LEFT JOIN bitcoin_inputs i ON t.txid = i.txid
    LEFT JOIN bitcoin_outputs o ON t.txid = o.txid
    JOIN bitcoin_blocks b ON t.block_id = b.block_id
    WHERE b.height = %s
    GROUP BY b.height, t.txid
    ON CONFLICT (txid) DO NOTHING;
    """, (block_height,))



def dm_update_daily_tx_volume(cur, block_height: int):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS datamart.mart_daily_tx_volume (
        block_date DATE PRIMARY KEY,
        tx_count NUMERIC,
        total_fees NUMERIC,
        total_output_value NUMERIC
    );
    """)
    cur.execute("""
    INSERT INTO datamart.mart_daily_tx_volume (block_date, tx_count, total_fees, total_output_value)
    SELECT 
        b.timestamp::date AS block_date,
        COUNT(t.txid) AS tx_count,
        COALESCE(SUM(t.fee), 0) AS total_fees,
        COALESCE(SUM(o.output_value), 0) AS total_output_value
    FROM bitcoin_blocks b
    LEFT JOIN bitcoin_transactions t ON b.block_id = t.block_id
    LEFT JOIN bitcoin_outputs o ON t.txid = o.txid
    WHERE b.height = %s
    GROUP BY b.timestamp::date
    ON CONFLICT (block_date) DO UPDATE 
    SET tx_count = EXCLUDED.tx_count,
        total_fees = EXCLUDED.total_fees,
        total_output_value = EXCLUDED.total_output_value;
    """, (block_height,))


def update_marts(conn, block_height: int):
    """
    Wrap all mart updates in a single transaction so they commit atomically.
    """
    cur = conn.cursor()
    try:
        cur.execute("SET LOCAL synchronous_commit = OFF;")  # optional perf tweak
        dm_update_block_stats(cur, block_height)
        dm_update_tx_patterns(cur, block_height)
        dm_update_address_activity(cur, block_height)
        dm_update_daily_tx_volume(cur, block_height)
        conn.commit()
        logger.info(f"Updated data marts for block {block_height}")
    except Exception:
        conn.rollback()
        logger.error(f"Failed updating datamarts for block {block_height}")
        logger.error(traceback.format_exc())
        raise
    finally:
        cur.close()


# STREAMING WITH CHECKPOINT + GRACEFUL SHUTDOWN

def stream_blocks(poll_interval=POLL_INTERVAL):
    ensure_state_table()
    last_height = get_last_processed_height()

    if last_height:
        logger.info(f"Resuming from checkpoint block {last_height}")
    else:
        if START_HEIGHT:
            last_height = START_HEIGHT
            logger.info(f"Starting from configured block height {START_HEIGHT}")
        elif START_DATE:
            last_height = get_height_from_date(START_DATE)
            logger.info(f"Starting from date {START_DATE}, block height {last_height}")
        else:
            last_height = get_latest_height()
            logger.info(f"Starting from tip block {last_height}")
        update_last_processed_height(last_height)

    try:
        while True:
            try:
                tip = get_latest_height()
            except Exception as e:
                logger.error(f"Failed to fetch latest height: {e}")
                time.sleep(poll_interval)
                continue

            if last_height < tip:
                for h in range(last_height + 1, tip + 1):
                    logger.info(f"Processing block {h}")
                    try:
                        block_data, detailed_txs = extract_block(h)
                        block_df, tx_df, inputs_df, outputs_df, address_df = transform_block(block_data, detailed_txs)
                        # load raw
                        load_to_postgres(block_df, tx_df, inputs_df, outputs_df, address_df)
                        # update marts
                        conn = get_connection()
                        try:
                            update_marts(conn, h)
                        finally:
                            conn.close()
                        # update checkpoint
                        update_last_processed_height(h)
                        last_height = h
                    except Exception:
                        logger.error(f"Failed to process block {h}, continuing to next block")
                        logger.error(traceback.format_exc())
                        
            else:
                logger.info("No new block yet")
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt - stopping stream gracefully")
    except Exception:
        logger.critical("ETL pipeline crashed unexpectedly")
        logger.critical(traceback.format_exc())
        raise


# MAIN

if __name__ == "__main__":
    try:
        stream_blocks(poll_interval=POLL_INTERVAL)
    except Exception as e:
        logger.critical(f"ETL pipeline crashed: {e}")
        logger.critical(traceback.format_exc())
        raise
