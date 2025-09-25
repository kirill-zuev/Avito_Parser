from fastapi import APIRouter
from loguru import logger

from utils import Item

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


@router.post("/post")
async def find_radius(request: Item):
    conn = await asyncpg.connect(**DATABASE_CONFIG)
    address = request.address
    radius = request.radius
    try:
        query = """
            SELECT lat::double precision, lon::double precision 
            FROM coder 
            WHERE address = $1
            LIMIT 1
        """
        cords = await conn.fetchrow(query, address)
        if not cords:
            return {"error": "Адрес не найден в базе данных"}
        lat = cords['lat']
        lon = cords['lon']
        query = """
            SELECT 
                address,
                lat::double precision,
                lon::double precision,
                ST_Distance(
                    geography(ST_MakePoint(lon::double precision, lat::double precision)),
                    geography(ST_MakePoint($1, $2))
                ) as distance_meters
            FROM coder 
            WHERE ST_DWithin(
                geography(ST_MakePoint(lon::double precision, lat::double precision)),
                geography(ST_MakePoint($1, $2)),
                $3
            )
            ORDER BY distance_meters ASC
        """
        results = await conn.fetch(query, lon, lat, radius)
        return [dict(row) for row in results]
    finally:
        await conn.close()


async def update_address(coder_table="coder"):
    conn = await asyncpg.connect(**DATABASE_CONFIG)
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {coder_table} (
            address TEXT PRIMARY KEY,
            lat TEXT,
            lon TEXT
        )
    """)
    logger.info(f"Create table {coder_table} if not exists")
    while True:
        table = "ads"
        logger.info(f"{table=}")
        addresses = await conn.fetch(f"SELECT DISTINCT address, lat, lon FROM {table}")
        for record in addresses:
            address = record['address']
            exists = await conn.fetchval(
                f"SELECT 1 FROM {coder_table} WHERE address = $1", 
                address
            )
            if not exists:
                try:
                    lat, lon = record['lat'], record['lon']
                    await conn.execute(
                        f"""
                        INSERT INTO {coder_table} (address, lat, lon)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (address) DO UPDATE
                        SET lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon
                        """,
                        address, lat, lon
                    )
                    logger.info(f"{lat=}, {lon=}")
                except:
                    await asyncio.sleep(600)
            else:
                logger.info(f"Адрес {address=} уже существует в целевой таблице")
        await asyncio.sleep(600)