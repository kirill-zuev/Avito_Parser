from datetime import datetime, timedelta
from pydantic import BaseModel
from datetime import date

import base64
import re


class Item(BaseModel):
    date_: date
    n: int = 1


class Validator:
    def generate_avito(self, days_, today) -> str:
        urls=[]
        for i in range(days_):
            end_date = today + timedelta(days=i+1)
            check_in = today.strftime("%Y-%m-%d")
            check_out = end_date.strftime("%Y-%m-%d")
            date_from = datetime.strptime(check_in, '%Y-%m-%d').strftime('%Y%m%d')
            date_to = datetime.strptime(check_out, '%Y-%m-%d').strftime('%Y%m%d')

            json_data = f'{{"from":{date_from},"to":{date_to}}}'

            b64_encoded = base64.b64encode(json_data.encode()).decode()
            b64_clean = b64_encoded.replace('=', '')
            
            base_url = "https://www.avito.ru/moskva/kvartiry/sdam/posutochno/-ASgBAgICAkSSA8gQ8AeSUg"
            f_prefix = "ASgBAgECAkSSA8gQ8AeSUgFFqC0f"
            
            urls.append({"url": f"{base_url}?cd=1&context=&f={f_prefix}{b64_clean}&localPriority=0", "days": i+1})

        return urls
    
    def validate_apartment(self, apartment_str):
        pattern = r'^(.*?),\s*(\d+)\s*м²,\s*(\d+)\s*кроват(?:и|ь|ей)$'
        match = re.fullmatch(pattern, apartment_str.strip())
        
        if not match:
            return None, None, None

        apartment_type = match.group(1).strip()
        square_meters = match.group(2)
        beds = match.group(3)

        return f"{apartment_type}", int(square_meters), int(beds)