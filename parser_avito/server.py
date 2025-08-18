from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from fastapi import FastAPI
from loguru import logger

from routers import clean_proc

import routers

import uvicorn
import psutil
import signal
import sys
import os

load_dotenv()

PARSERPORT=int(os.getenv("PARSERPORT"))
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


def signal_handler(sig, frame):
    clean_proc()
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)

    host = '0.0.0.0'

    logger.info(f'Сервис запущен ip: {host}:{PARSERPORT}')

    try:
        uvicorn.run(app, host=host, port=PARSERPORT)
    except KeyboardInterrupt:
        clean_proc()