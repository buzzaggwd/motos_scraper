import scrapy
from bs4 import BeautifulSoup
import json
import re
from motos_scraper.items import MotosScraperItem
from collections import defaultdict
from difflib import SequenceMatcher
import sqlite3
from loguru import logger
from datetime import datetime

with open("../motos.json", "r", encoding="utf-8") as file:
    motos = json.load(file)

BRANDS = [
    "honda", "kawasaki", "harley", "harley-davidson", "harleydavidson",
    "bmw", "yamaha", "ktm", "suzuki", "triumph", "ducati", "buell",
    "mv agusta", "mvagusta"
]

def normalize(s):
    if not s:
        return ""
    s = s.lower().strip()
    s = re.sub(r'[()\[\]{}]', '', s)
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    for brand in BRANDS:
        s = re.sub(rf"\b{re.escape(brand)}\b", " ", s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalize_brand(brand):
    brand = brand.lower().strip()
    brand = re.sub(r'\s+', ' ', brand)
    brand = re.sub(r'[()\[\]{}]', '', brand)
    brand = re.sub(r'[^a-z0-9]', '', brand)
    brand = brand.replace("harley-davidson","harleydavidson")
    brand = brand.replace("mv agusta","mvagusta")
    return brand

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def get_existing_api_ids():
    try:
        conn = sqlite3.connect("../motos.db")
        cur = conn.cursor()
        cur.execute("SELECT api_id FROM motos")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return set(r[0] for r in rows)
    except sqlite3.Error as e:
        return set()

class BikezSpider(scrapy.Spider):
    name = "bikez_spider"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.motos = motos
        self.by_brand = defaultdict(list)    # defaultdict(<type 'list'>, {})
        self.existing_api_ids = get_existing_api_ids()
        self.brand_index = {}
        self.loguru_logger = logger.bind(spider="bikez")

        for moto in self.motos:
            brand = normalize_brand(moto.get("brand"))
            self.by_brand[brand].append(moto)

        for brand, lst in self.by_brand.items():
            idx = defaultdict(list)
            for moto in lst:
                if moto["api_id"] not in self.existing_api_ids:
                    nm = normalize(moto.get("model"))
                    idx[nm].append(moto)
            self.brand_index[brand] = idx  # brand_index[brand][normalized_model_name] = [moto1, moto2, ...]

        self.stats = {
            'start_time': datetime.now(),
            'models_processed': 0,
            'items_found': 0,
            'items_with_power': 0,
            'unique_items': set()
        }

    def start_requests(self):
        self.loguru_logger.info(f"Bikez Spider запущен")
        for brand, lst in self.by_brand.items():
            url = f"https://bikez.com/models/{brand}_models.php"
            yield scrapy.Request(
                url=url,
                callback=self.parse_catalog,
                meta={"brand": brand},
            )

    def parse_catalog(self, response):
        brand = response.meta["brand"]
        brand_key = normalize_brand(brand)
        index = self.brand_index.get(brand_key, {})

        rows = response.css("tr.even, tr.odd")
        for row in rows:
            model_link = row.css("td:nth-child(2) a[href$='.php']")
            raw_name = model_link.css("::text").get()
            href = model_link.css("::attr(href)").get()

            if not raw_name or not href:
                continue
            
            model_name = normalize(raw_name)

            matches = list(index.get(model_name, []))

            if not matches:
                best_key = None
                best_score = 0.0
                for key in index.keys():
                    score = similar(model_name, key)
                    if score > best_score:
                        best_score = score 
                        best_key = key
                if best_score >= 0.90:
                    matches = index[best_key]

            if not matches:
                continue

            model_url = response.urljoin(href)
            yield scrapy.Request(
                url=model_url,
                callback=self.parse_model,
                meta={
                    "brand": brand,
                    "model_url": model_url,
                    "model_name": model_name,
                    "matches": matches,
                    "tried_expansion": False,
                }
            )

    def parse_model(self, response):
        self.stats['models_processed'] += 1
        model_url = response.meta["model_url"]
        soup = BeautifulSoup(response.text, "html.parser")

        parsed_data = {}

        for trs in soup.find_all("tr"):
            tds = trs.find_all("td")
            if len(tds) < 2:
                continue
                
            header = tds[0].get_text(strip=True)
            value = tds[1].get_text(strip=True)
                
            if header in ["Category", "Type"]:
                if value != "Loading...":
                    parsed_data["category"] = value
                
            elif header in ["Engine type", "Type of engine"]:
                if value != "Loading...":
                    parsed_data["engine_type"] = value
                
            elif header in ["Displacement", "Engine size"]:
                match = re.search(r'([\d.]+)\s*ccm', value)
                if match:
                    parsed_data["engine_displacement_cc"] = match.group(1)

            elif header in ["Power", "Output"] and "..." not in value:
                match = re.search(r'([\d.]+)\s*HP', value)
                if match:
                    parsed_data["engine_power_hp"] = match.group(1)
                    
            elif header == "Torque":
                match = re.search(r'([\d.]+)\s*Nm', value)
                if match:
                    parsed_data["engine_torque_nm"] = match.group(1)
                    
            elif header == "Gearbox":
                if value != "Loading...":
                    parsed_data["gearbox"] = value
                
            elif header == "Weight incl. oil, gas, etc":
                match = re.search(r'([\d.]+)\s*kg', value)
                if match:
                    parsed_data["wet_weight_kg"] = match.group(1)
                    
            elif header == "Dry weight":
                match = re.search(r'([\d.]+)\s*kg', value)
                if match:
                    parsed_data["dry_weight_kg"] = match.group(1)
                    
            elif header == "Fuel capacity":
                match = re.search(r'([\d.]+)\s*litres', value)
                if match:
                    parsed_data["fuel_capacity_l"] = match.group(1)
                    
            elif header == "Fuel consumption":
                match = re.search(r'([\d.]+)\s*litres/100 km', value)
                if match:
                    parsed_data["fuel_consumption_l_per_100km"] = match.group(1)
                    
            elif header == "Transmission type":
                match = re.search(r'^(\w+)', value)
                if match:
                    parsed_data["transmission_type"] = match.group(1)
                    
            elif header == "Clutch":
                if value != "Loading...":
                    parsed_data["transmission_clutch"] = value

        for moto in response.meta["matches"]:
            item = MotosScraperItem()
            item["api_id"] = moto["api_id"]
            item["source"] = "bikez"
            item["source_url"] = model_url
            item["brand"] = moto["brand"]
            item["model"] = moto["model"]
            item["year"] = moto["year"]

            for k, v in parsed_data.items():
                item[k] = v

            if item.get("brand"):
                brand_lower = item["brand"].lower()
                if brand_lower in ["honda", "kawasaki", "yamaha", "suzuki"]:
                    item["origin_country"] = "Japan"
                elif brand_lower in ["harley-davidson", "buell"]:
                    item["origin_country"] = "USA"
                elif brand_lower in ["bmw"]:
                    item["origin_country"] = "Germany"
                elif brand_lower in ["ktm"]:
                    item["origin_country"] = "Austria"
                elif brand_lower in ["triumph"]:
                    item["origin_country"] = "Britain"
                elif brand_lower in ["ducati"]:
                    item["origin_country"] = "Italy"
                else:
                    item["origin_country"] = None
            
            # СОХРАНЕНИЕ ТОЛЬКО ПРИ НАЙДЕННОЙ МОЩНОСТИ
            if item.get("engine_power_hp"):
                self.stats['items_with_power'] += 1
                self.stats['unique_items'].add(item['api_id'])
                self.logger.info(f"[НАШЕЛ bikez] {item['model']} - {item.get('engine_power_hp')}")
                yield item

            else:
                self.logger.info(f"[НАШЕЛ bikez] {item['model']} - нет мощности")

            # СОХРАНЕНИЕ ДАЖЕ ПРИ ОТСУТСТВИИ МОЩНОСТИ
            # self.stats['unique_items'].add(item['api_id'])
            # self.logger.info(f"[НАШЕЛ bikez] {item['model']}")
            # yield item

            self.stats['items_found'] += 1

    def closed(self, reason):
        self.loguru_logger.info(f"Парсер завершил работу.")
        self.log_statistics()

    def log_statistics(self):
        duration = datetime.now() - self.stats['start_time']
        minutes = duration.total_seconds() / 60
        
        self.loguru_logger.info(
            f"Статистика парсера:\n"
            f"- Время работы: {minutes:.2f} минут\n"
            f"- Обработано моделей: {self.stats['models_processed']}\n"
            f"- Найдено items: {self.stats['items_found']}\n"
            f"- Items с мощностью: {self.stats['items_with_power']}\n"
            f"- Уникальных мотоциклов: {len(self.stats['unique_items'])}\n"
            f"- Скорость: {self.stats['models_processed'] / minutes:.2f} моделей/мин"
        )