import sys
import os
import requests
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import pandas as pd
import logging
import traceback
import time
from typing import Optional


# configurations

BLOCKSTREAM_API = "https://blockstream.info/api"
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Choose one of:
START_HEIGHT: Optional[int] = None    # or specify any block height to start from
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


# BLOCK HEIGHT - DATE helper

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
