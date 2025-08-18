import multiprocessing
import threading
import psutil
import random
import signal
import time
import os
import re

from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from dotenv import load_dotenv
from fastapi import APIRouter
from datetime import datetime
from seleniumbase import SB
from loguru import logger

from postgre import PostgresHandler
from utils import Item, Validator
from locator import LocatorAvito

load_dotenv()

NUM_ADS=int(os.getenv("NUM_ADS"))
MAX_PRICE=int(os.getenv("MAX_PRICE"))
MIN_PRICE=int(os.getenv("MIN_PRICE"))
PROXY=os.getenv("PROXY")
NEED_MORE_INFO=int(os.getenv("NEED_MORE_INFO"))
FAST_SPEED=int(os.getenv("FAST_SPEED"))
DEBUG_MODE=int(os.getenv("DEBUG_MODE"))

sleep_=1.0

router = APIRouter()

postgres_handler=PostgresHandler()
validator = Validator()
pids = {"main": None}


class AvitoParser:
    def __init__(self,
                 url: list,
                 date_: str,
                 days: int,
                 count: int = 5,
                 max_price: int = 0,
                 min_price: int = 0,
                 debug_mode: int = 0,
                 need_more_info: int = 1,
                 proxy: str = None,
                 fast_speed: int = 0
                 ):
        self.url = url
        self.date_ = date_
        self.days = days
        self.count = count
        self.data = []
        self.max_price = int(max_price)
        self.min_price = int(min_price)
        self.debug_mode = debug_mode
        self.need_more_info = need_more_info
        self.proxy = proxy
        self.fast_speed = fast_speed
        
    def get_url(self):
        logger.info(f"Открываю страницу: {self.url}")
        self.driver.get(self.url)
        time.sleep(sleep_)

        if "Доступ ограничен" in self.driver.get_title():
            self.ip_block()
            return self.get_url()

    def ip_block(self) -> None:
        if all([self.proxy]):
            logger.info("Блок IP. Прокси есть, делаю паузу")
        else:
            logger.info("Блок IP. Прокси нет, делаю паузу")
        time.sleep(sleep_)

    def open_next_btn(self, i):
        self.url = self.get_next_page_url(url=self.url)
        logger.info(f"Следующая страница {i}")
        self.driver.get(self.url)
        time.sleep(sleep_)
    
    @staticmethod
    def get_next_page_url(url: str):
        try:
            url_parts = urlparse(url)
            query_params = parse_qs(url_parts.query)
            current_page = int(query_params.get('p', [1])[0])
            query_params['p'] = current_page + 1

            new_query = urlencode(query_params, doseq=True)
            next_url = urlunparse((url_parts.scheme, url_parts.netloc, url_parts.path, url_parts.params, new_query,
                                   url_parts.fragment))
            return next_url
        except Exception as err:
            logger.error(f"Не смог сформировать ссылку на следующую страницу для {url}. Ошибка: {err}")
        

class ExtendedParser(AvitoParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def parse(self):
        with SB(
                uc=True,
                headed=True if self.debug_mode else False,
                headless2=True if not self.debug_mode else False,
                page_load_strategy="eager",
                block_images=True if not self.debug_mode else False,
                agent=random.choice(open("user_agent_pc.txt").readlines()),
                proxy=self.proxy,
                sjw=True if self.fast_speed else False,
                ) as driver:
            self.driver = driver

            try:
                self.get_url()
                self.paginator()
            except Exception as err:
                logger.debug(f"Ошибка: {err}")
        logger.info("Парсинг завершен")
    
    def paginator(self):
        logger.info('Страница загружена. Просматриваю объявления')
        time.sleep(sleep_)
        body = self.driver.find_element(By.TAG_NAME, "body")
        logger.info(f"Start")
        for i in range(100):
            body.send_keys(Keys.PAGE_DOWN)
        for i in range(100):
            body.send_keys(Keys.PAGE_UP)
        logger.info(f"Finish")
        for i in range(self.count):
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception:
                logger.info(f"window.scrollTo(0, document.body.scrollHeight);")
                pass
            time.sleep(sleep_)
            self.parse_page()
            time.sleep(sleep_)
            self.open_next_btn(i)
    
    def parse_page(self):
        all_titles = self.driver.find_elements(LocatorAvito.TITLES[1], by="css selector")
        titles = [title for title in all_titles if "avitoSales" not in title.get_attribute("class")]
        data_from_general_page = []
        for title in titles:
            try:
                name = title.find_element(*LocatorAvito.NAME).text
            except Exception:
                continue

            if title.find_elements(*LocatorAvito.DESCRIPTIONS):
                try:
                    description = title.find_element(*LocatorAvito.DESCRIPTIONS).text
                except Exception as err:
                    logger.debug(f"Ошибка при получении описания: {err}")
                    description = ''
            else:
                description = ''

            url = title.find_element(*LocatorAvito.URL).get_attribute("href")
            price = title.find_element(*LocatorAvito.PRICE).get_attribute("content")
            ads_id = title.get_attribute("data-item-id")

            if url and not ads_id:
                try:
                    regex = r"_\d+$"
                    ids = re.findall(pattern=regex, string=url)
                    logger.info(f"{ids=}")
                    if ids:
                        ads_id = url[-1][:-1]
                    continue
                except Exception:
                    continue

            if not ads_id:
                continue

            if postgres_handler.exist(ads_id, self.days):
                logger.debug(f"Пропускаю {ads_id=} {url=}")
                continue
            data = {
                'date_': self.date_,
                'name': name,
                'description': description,
                'url': url,
                'price': price,
                'adsid': ads_id
            }
            if self.min_price <= int(price) <= self.max_price:
                data_from_general_page.append(data)
        if data_from_general_page:
            for item_info in data_from_general_page:
                item_info = self.parse_full_page(item_info)
                item_info["days"] = self.days
                postgres_handler.update_database(item_info)
    
    def parse_full_page(self, data: dict) -> dict:
        self.driver.get(data.get("url"))
        if "Доступ ограничен" in self.driver.get_title():
            logger.info("Доступ ограничен: проблема с IP")
            self.ip_block()
            return self.parse_full_page(data=data)

        data["rgeo"] = ''
        data["comp"] = ''
        data["lat"] = ''
        data["lon"] = ''
        data["views"] = ''

        data['количество_комнат'] = ''
        data['кровати'] = ''
        data['общая_площадь'] = ''
        data['этаж'] = ''
        data['вид_из_окна'] = ''
        data['техника'] = ''
        data['интернет_и_тв'] = ''
        data['комфорт'] = ''
        data['залог'] = ''
        data['возможна_помесячная_аренда'] = ''
        data['заезд_после'] = ''
        data['выезд_до'] = ''
        data['количество_гостей'] = ''
        data['бесконтактное_заселение'] = ''
        data['можно_с_детьми'] = ''
        data['можно_с_животными'] = ''
        data['можно_курить'] = ''
        data['разрешены_вечеринки'] = ''
        data['есть_отчётные_документы'] = ''
        data['этажей_в_доме'] = ''
        data['лифт'] = ''
        data['парковка'] = ''
        data['балкон_или_лоджия'] = ''
        try:
            if self.driver.find_elements(LocatorAvito.RGEO[1], by="css selector"):
                rgeo = self.driver.find_element(LocatorAvito.RGEO[1], by="css selector").text
                data["rgeo"] = rgeo.lower()
            if self.driver.find_elements(LocatorAvito.COMP[1], by="css selector"):
                comp = self.driver.find_element(LocatorAvito.COMP[1], by="css selector").text
                data["comp"] = comp.lower()
            if self.driver.find_elements(LocatorAvito.LATLON[1], by="css selector"):
                mape = self.driver.find_element(LocatorAvito.LATLON[1], by="css selector")
                data["lat"] = mape.get_attribute("data-map-lat")
                data["lon"] = mape.get_attribute("data-map-lon")
            if self.driver.find_elements(LocatorAvito.INFO[1], by="css selector"):
                char_list = self.driver.find_elements(LocatorAvito.INFO[1], by="css selector")
                for item in char_list:
                    name = item.find_element(By.CSS_SELECTOR, "span.Lg7Ax").text.replace(":", "").strip()
                    value = item.text.replace(name + ":", "").strip()
                    key = name.lower().replace(" ", "_")
                    data[key] = value
        except Exception:
            if "Доступ ограничен" in self.driver.get_title():
                logger.info("Доступ ограничен: проблема с IP")
                self.ip_block()
                return self.parse_full_page(data=data)
            return data
        return data


def parse_url(url, today, days, proxy):
    try:
        ExtendedParser(
            url=url,
            date_=today,
            days=days,
            count=int(NUM_ADS),
            max_price=MAX_PRICE,
            min_price=MIN_PRICE,
            debug_mode=DEBUG_MODE,
            need_more_info=NEED_MORE_INFO if NEED_MORE_INFO else 0,
            proxy=proxy,
            fast_speed=FAST_SPEED if FAST_SPEED else 0
        ).parse()
    except Exception as error:
        logger.debug(f"{error}")


def main(proxy, today, urls=[]):
    processes = []
    try:
        for item in urls:
            url, days = item["url"], item["days"]
            process = threading.Thread(
                target=parse_url,
                args=(url, today, days, proxy)
            )
            process.start()
            processes = []
            processes.append(process)
            time.sleep(sleep_)
        
            for process in processes:
                process.join()
    except Exception as error:
        logger.debug(error)
        time.sleep(15)


def multi_parsing(date_, n, proxy):
    prevd = None
    today = date_
    try:
        while True:
            if today != prevd:
                prevd = today
                logger.info(f"{prevd}")
                urls = validator.generate_avito(n, today)
                logger.info(f"{urls}")
                postgres_handler.create_database(today)
                main(proxy, today, urls)
                
            today = datetime.now().date()
    except Exception as error:
        logger.debug(f"{error}")


@router.post("/post")
async def main_parser(request: Item):
    if pids["main"]:
        logger.info(f'Finish {pids["main"]=}')
        os.kill(pids["main"], signal.SIGKILL)
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] in ('chrome', 'chromium', 'firefox', 'geckodriver'):
                try:
                    os.kill(proc.info['pid'], signal.SIGKILL)
                    logger.info(f"Finish {proc.info['pid']=}")
                except:
                    logger.info(f"Exception {proc.info['pid']=}")
        
    proxy = PROXY
    if PROXY and "@" not in PROXY:
        logger.info("Прокси - user:pass@ip:port")
        proxy = None
    
    process = multiprocessing.Process(
        target=multi_parsing,
        args=(request.date_, request.n, proxy),
    )
    process.start()
    pids["main"] = process.pid
    logger.info(f'Start {pids["main"]=}')
    
    return {"date": request.date_}