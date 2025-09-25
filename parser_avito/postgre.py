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
        self.table_name_ads = "ads"
        self.table_name_ads_snapshots = "ads_snapshots"

    def create_database(self):
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

            # t = today.strftime("%Y-%m-%d").replace("-", "_")
            # self.table_name = f"advertisements_{t}"

            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name_ads} (
                    ad_uid SERIAL PRIMARY KEY,
                    adsid BIGINT NOT NULL,
                    property_hash VARCHAR NOT NULL,
                    address TEXT NOT NULL,
                    lat TEXT,
                    lon TEXT,
                    square_meters BIGINT,
                    url TEXT NOT NULL,
                    description TEXT NOT NULL,
                    
                    title TEXT NOT NULL,
                    competitor TEXT NOT NULL,
                    apartment_type TEXT,
                    beds BIGINT,
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
            logger.info(f"Таблица {self.table_name_ads} создана или уже существует")
            
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name_ads_snapshots} (
                    id SERIAL PRIMARY KEY,
                    ad_uid BIGINT NOT NULL,
                    parsed_at DATE NOT NULL,
                    date_available_from DATE NOT NULL,
                    date_available_to DATE NOT NULL,
                    price BIGINT NOT NULL,
                    views BIGINT
                    )
                """)
            logger.info(f"Таблица {self.table_name_ads_snapshots} создана или уже существует")
        except Exception as e:
            logger.info(f"{e}")
    
    def update_database(self, data):
        try:
            apartment_type, square_meters, beds = validator.validate_apartment(data["name"])
            self.cursor.execute(
                f"""SELECT ad_uid FROM {self.table_name_ads}
                WHERE adsid = %s AND property_hash = %s
                ORDER BY ad_uid DESC
                LIMIT 1
                """, (int(data["adsid"]), data["shash"])
            )

            existing_record = self.cursor.fetchone()

            if not existing_record:
                item = (int(data["adsid"]), data["shash"], data["rgeo"], data["lat"], data["lon"], square_meters, data["url"], data["description"], data["name"], data["comp"], apartment_type, beds,
                        data['количество_комнат'], data['кровати'], data['общая_площадь'], data['этаж'], data['балкон_или_лоджия'],
                        data['вид_из_окна'], data['техника'], data['интернет_и_тв'], data['комфорт'], data['залог'], data['возможна_помесячная_аренда'],
                        data['заезд_после'], data['выезд_до'], data['количество_гостей'], data['бесконтактное_заселение'], 
                        data['можно_с_детьми'], data['можно_с_животными'], data['можно_курить'], data['разрешены_вечеринки'],
                        data['есть_отчётные_документы'], data['этажей_в_доме'], data['лифт'], data['парковка'])
                self.cursor.execute(
                    f"""INSERT INTO {self.table_name_ads} (adsid, property_hash, address, lat, lon, square_meters, url, description, title, competitor, apartment_type, beds, rooms, beds_description, total_area, floor, balcony_or_loggia, window_view, appliances, internet_tv, comforts, deposit, monthly_rent, check_in_time, check_out_time, max_guests, contactless_checkin, children_allowed, pets_allowed, smoking_allowed, parties_allowed, documents_provided, total_floors, has_elevator, parking_available) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING ad_uid""", item
                )
                aduid = self.cursor.fetchone()[0]
            else:
                aduid = existing_record[0]
            
            item = (int(aduid), data["today"], data["date_"], data["days"], data['price'], data['views'])

            self.cursor.execute(
                f"SELECT EXISTS(SELECT 1 FROM {self.table_name_ads_snapshots} WHERE ad_uid = %s AND parsed_at = %s AND date_available_from = %s AND date_available_to = %s )",
                (int(aduid), data["today"], data["date_"], data["days"])
            )
            exists = self.cursor.fetchone()[0]
            if not exists:
                self.cursor.execute(
                    f"""INSERT INTO {self.table_name_ads_snapshots} (ad_uid, parsed_at, date_available_from, date_available_to, price, views)
                    VALUES (%s, %s, %s, %s, %s, %s)""",
                    item
                )
            logger.info(f'{data["url"]=}')
        except Exception as e:
            logger.info(f"{e}")
    
    def check_and_aduid(self, adsid):
        self.cursor.execute(f"""
            SELECT ad_uid FROM {self.table_name_ads} 
            WHERE adsid = %s
            ORDER BY ad_uid DESC
            LIMIT 1
        """, (adsid,))
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def exist(self, ads_id, today, date_, days):
        try:
            aduid = self.check_and_aduid(ads_id)
            if aduid is None:
                return False
            self.cursor.execute(
                f"SELECT EXISTS(SELECT 1 FROM {self.table_name_ads_snapshots} WHERE ad_uid = %s AND parsed_at = %s AND date_available_from = %s AND date_available_to = %s )",
                (int(aduid), today, date_, days)
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