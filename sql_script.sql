CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS bitcoin_blocks (
             block_id TEXT PRIMARY KEY,
            height NUMERIC,
            timestamp TIMESTAMP,
            tx_count NUMERIC,
            size BIGINT,
            weight BIGINT
        );

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


CREATE TABLE IF NOT EXISTS bitcoin_inputs (
            id SERIAL PRIMARY KEY,
            txid TEXT,
            prev_txid TEXT,
            prev_index NUMERIC,
            input_address TEXT,
            input_value NUMERIC
        );      


CREATE TABLE IF NOT EXISTS bitcoin_outputs (
            id SERIAL PRIMARY KEY,
            txid TEXT,
            vout_index NUMERIC,
            output_address TEXT,
            output_value NUMERIC,
            spent BOOLEAN
        );


CREATE TABLE IF NOT EXISTS bitcoin_addresses (
            address TEXT PRIMARY KEY,
            total_received NUMERIC,
            total_sent NUMERIC,
            balance NUMERIC,
            first_seen TIMESTAMP,
            last_seen TIMESTAMP
        );


CREATE TABLE IF NOT EXISTS etl_state (
            id SERIAL PRIMARY KEY,
            last_block_height BIGINT,
            updated_at TIMESTAMP DEFAULT now()
        );


SELECT*FROM bitcoin_blocks;
SELECT*FROM bitcoin_transactions;
SELECT*FROM bitcoin_inputs;
SELECT*FROM bitcoin_addresses;
SELECT*FROM bitcoin_outputs;

SELECT*FROM etl_state;


TRUNCATE TABLE etl_state;
TRUNCATE TABLE  bitcoin_blocks CASCADE;
TRUNCATE TABLE bitcoin_transactions CASCADE;
TRUNCATE TABLE bitcoin_inputs CASCADE;
TRUNCATE TABLE bitcoin_outputs CASCADE;
TRUNCATE TABLE bitcoin_addresses CASCADE;



DROP TABLE IF EXISTS datamart.mart_daily_tx_volume CASCADE;
DROP TABLE IF EXISTS datamart.mart_tx_patterns CASCADE;
DROP TABLE IF EXISTS datamart.mart_address_activity CASCADE;
DROP TABLE IF EXISTS datamart.mart_block_stats CASCADE;


SELECT*FROM datamart.mart_daily_tx_volume;
SELECT*FROM datamart.mart_tx_patterns;
SELECT*FROM datamart.mart_address_activity;
SELECT*FROM datamart.mart_block_stats;

