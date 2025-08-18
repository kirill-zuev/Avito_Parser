from coder import fetch_coordinates

from fastapi import APIRouter
from loguru import logger

import asyncio
import asyncpg

DATABASE_CONFIG = {
    "user": "admin",
    "password": "admin",
    "database": "avito",
    "host": "localhost",
    "port": 5432
}

router = APIRouter()


@router.get("/tables")
async def get_tables():
    conn = await asyncpg.connect(**DATABASE_CONFIG)
    try:
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """
        rows = await conn.fetch(query)
        tables = [row["table_name"] for row in rows]
        return tables
    finally:
        await conn.close()


@router.get("/data")
async def get_data(table: str):
    conn = await asyncpg.connect(**DATABASE_CONFIG)
    try:
        query = f"""
            SELECT days, AVG(price) AS avg_price
            FROM {table}
            GROUP BY days
            ORDER BY days
        """
        rows = await conn.fetch(query)
        data = [{"days": row["days"], "avg_price": float(row["avg_price"])} for row in rows]
        return data
    finally:
        await conn.close()


async def update_address(coder_table="coder"):
    conn = await asyncpg.connect(**DATABASE_CONFIG)
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {coder_table} (
            address TEXT PRIMARY KEY,
            lon TEXT,
            lat TEXT
        )
    """)
    logger.info(f"Create table {coder_table} if not exists")
    while True:
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """
        rows = await conn.fetch(query)
        tables = [row["table_name"] for row in rows if row["table_name"] != coder_table]
        for table in tables:
            addresses = await conn.fetch(f"SELECT DISTINCT address FROM {table}")
            for record in addresses:
                address = record['address']
                exists = await conn.fetchval(
                    f"SELECT 1 FROM {coder_table} WHERE address = $1", 
                    address
                )
                if not exists:
                    try:
                        apikey = ''
                        lon, lat = fetch_coordinates(apikey, address)
                        await conn.execute(
                            f"""
                            INSERT INTO {coder_table} (address, lon, lat)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (address) DO UPDATE
                            SET lon = EXCLUDED.lon,
                                lat = EXCLUDED.lat
                            """,
                            address, lon, lat
                        )
                        logger.info(f"{lon=}, {lat=}")
                    except:
                        logger.info(f"{apikey=}")
                        await asyncio.sleep(600)
                else:
                    logger.info(f"Адрес {address=} уже существует в целевой таблице")
        await asyncio.sleep(10)