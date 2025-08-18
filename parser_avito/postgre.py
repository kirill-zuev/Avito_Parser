from dotenv import load_dotenv
from datetime import datetime
from loguru import logger

from utils import Validator

import psycopg2
import os

load_dotenv()

DB_NAME=os.getenv("DB_NAME")
PGUSER=os.getenv("PGUSER")
PGPASSWORD=os.getenv("PGPASSWORD")
PGHOST=os.getenv("PGHOST")
PGPORT=os.getenv("PGPORT")
NEW_DB_NAME=os.getenv("NEW_DB_NAME")

validator = Validator()


class PostgresHandler:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.table_name = None

    def create_database(self, today):
        try:
            conn = psycopg2.connect(
                database=DB_NAME,
                user=PGUSER,
                password=PGPASSWORD,
                host=PGHOST,
                port=PGPORT
            )
            conn.autocommit = True
            cursor = conn.cursor()
        
            exists = cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", 
                (NEW_DB_NAME,)
            )
            exists = cursor.fetchone()
            if not exists:
                logger.info(f"{NEW_DB_NAME=}")
                cursor.execute(
                    f"CREATE DATABASE {NEW_DB_NAME}"
                )
                logger.info(f"База данных {NEW_DB_NAME} успешно создана")
            else:
                logger.info(f"База данных {NEW_DB_NAME} уже существует")
                
            cursor.close()
            conn.close()

            self.conn = psycopg2.connect(
                database=NEW_DB_NAME,
                user=PGUSER,
                password=PGPASSWORD,
                host=PGHOST,
                port=PGPORT
            )
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()

            t = today.strftime("%Y-%m-%d").replace("-", "_")
            self.table_name = f"advertisements_{t}"

            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id SERIAL PRIMARY KEY,
                    date_ DATE NOT NULL,
                    adsid BIGINT NOT NULL,
                    title TEXT NOT NULL,
                    price BIGINT NOT NULL,
                    url TEXT NOT NULL,
                    description TEXT NOT NULL,
                    address TEXT NOT NULL,
                    competitor TEXT NOT NULL,
                    apartment_type TEXT,
                    square_meters BIGINT,
                    beds BIGINT,
                    days BIGINT NOT NULL,
                    lat TEXT,
                    lon TEXT,
                    
                    rooms TEXT,
                    beds_description TEXT,
                    total_area TEXT,
                    floor TEXT,
                    balcony_or_loggia TEXT,
                    window_view TEXT,
                    appliances TEXT,
                    internet_tv TEXT,
                    comforts TEXT,
                    deposit TEXT,
                    monthly_rent TEXT,
                    check_in_time TEXT,
                    check_out_time TEXT,
                    max_guests TEXT,
                    contactless_checkin TEXT,
                    children_allowed TEXT,
                    pets_allowed TEXT,
                    smoking_allowed TEXT,
                    parties_allowed TEXT,
                    documents_provided TEXT,
                    total_floors TEXT,
                    has_elevator TEXT,
                    parking_available TEXT
                    )
                """)
            logger.info(f"Таблица {self.table_name} создана или уже существует")
        except Exception as e:
            logger.info(f"{e}")
    
    def update_database(self, data):
        try:
            apartment_type, square_meters, beds = validator.validate_apartment(data["name"])
            item = (data["date_"], int(data["adsid"]), data["name"], int(data["price"]), data["url"], data["description"], data["rgeo"], data["comp"], apartment_type, square_meters, beds, data["days"], data["lat"], data["lon"],
                    data['количество_комнат'], data['кровати'], data['общая_площадь'], data['этаж'], data['балкон_или_лоджия'],
                    data['вид_из_окна'], data['техника'], data['интернет_и_тв'], data['комфорт'], data['залог'], data['возможна_помесячная_аренда'],
                    data['заезд_после'], data['выезд_до'], data['количество_гостей'], data['бесконтактное_заселение'], 
                    data['можно_с_детьми'], data['можно_с_животными'], data['можно_курить'], data['разрешены_вечеринки'],
                    data['есть_отчётные_документы'], data['этажей_в_доме'], data['лифт'], data['парковка'])
            self.cursor.execute(
                f"""INSERT INTO {self.table_name} (date_, adsid, title, price, url, description, address, competitor, apartment_type, square_meters, beds, days, lat, lon,
                rooms, beds_description, total_area, floor, balcony_or_loggia,
                window_view, appliances, internet_tv, comforts, deposit, monthly_rent,
                check_in_time, check_out_time, max_guests, contactless_checkin,
                children_allowed, pets_allowed, smoking_allowed, parties_allowed,
                documents_provided, total_floors, has_elevator, parking_available) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                item
            )
            logger.info(f'{data["url"]=}')
        except Exception as e:
            logger.info(f"{e}")
    
    def exist(self, ads_id, days):
        try:
            self.cursor.execute(
                f"SELECT EXISTS(SELECT 1 FROM {self.table_name} WHERE adsid = %s AND days = %s)",
                (int(ads_id), int(days))
            )
            return self.cursor.fetchone()[0]
        except Exception as e:
            logger.info(f"{e}")
            return False
    
    def __del__(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()