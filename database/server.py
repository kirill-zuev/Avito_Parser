from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from fastapi import FastAPI
from loguru import logger

import routers

import asyncio
import uvicorn
import os

load_dotenv()

DATARPORT=int(os.getenv("DATARPORT"))
BASE_PREFIX='/api'

app = FastAPI(
    docs_url=BASE_PREFIX + '/docs',
    redoc_url=BASE_PREFIX + '/redoc',
    openapi_url=BASE_PREFIX + '/openapi.json'
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

app.include_router(routers.router, prefix=BASE_PREFIX)


async def run_server():
    config = uvicorn.Config(app, host="0.0.0.0", port=DATARPORT)
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    tasks = [
        routers.update_address(),
        run_server(),
    ]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())