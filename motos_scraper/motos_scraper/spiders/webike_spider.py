import scrapy
from bs4 import BeautifulSoup
import json
import re
from motos_scraper.items import MotosScraperItem
from difflib import SequenceMatcher
import sqlite3
from loguru import logger
from datetime import datetime

with open("../motos.json", "r", encoding="utf-8") as f:
    motos = json.load(f)

def normalize(s):
    if not s:
        return ""
    s = s.lower().strip()
    s = re.sub(r'[()\[\]{}]', '', s)
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

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

class WebikeSpider(scrapy.Spider):
    name = "webike_spider"
    start_urls = [
            "https://www.webike.com.ru/Moto/honda/",
            "https://www.webike.com.ru/Moto/kawasaki/",
            "https://www.webike.com.ru/Moto/harley-davidson/",
            "https://www.webike.com.ru/Moto/bmw/",
            "https://www.webike.com.ru/Moto/yamaha/",
            "https://www.webike.com.ru/Moto/ktm/",
            "https://www.webike.com.ru/Moto/suzuki/",
            "https://www.webike.com.ru/Moto/triumph/",
            "https://www.webike.com.ru/Moto/ducati/",
            "https://www.webike.com.ru/Moto/buell/",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.motos = motos
        self.existing_api_ids = get_existing_api_ids()
        self.normalized_motos = {}
        self.loguru_logger = logger.bind(spider="webike")

        for moto in motos:
            if moto["api_id"] not in self.existing_api_ids:
                norm = normalize(moto["model"])
                self.normalized_motos.setdefault(norm, []).append(moto)

        self.stats = {
            'start_time': datetime.now(),
            'models_processed': 0,
            'items_found': 0,
            'items_with_power': 0,
            'unique_items': set()
        }

    def start_requests(self):
        self.loguru_logger.info(f"Webike Spider запущен")
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, meta={"url": url})

    def parse(self, response):
        soup = BeautifulSoup(response.text, "html.parser")

        for div in soup.select("div.model_name"):
            a_tag = div.find("a")
            if not a_tag:
                continue
            title = a_tag.get_text(strip=True)
            href = a_tag.get("href")
            modified_url = href.replace("mtop/", "m-spec/")

            normalized_name = normalize(title)

            match = None
            max_similarity = 0.0

            for moto in self.normalized_motos:
                score = similar(normalized_name, moto)
                if score > max_similarity:
                    max_similarity = score
                    match = moto
            if max_similarity>=0.90:
                normalized_name = match

            if normalized_name in self.normalized_motos:
                yield scrapy.Request(
                    url=modified_url,
                    callback=self.parse_model_info,
                    meta={"normalized": normalized_name, "title": title, "url": modified_url},
                    dont_filter=True,
                )
            else:
                self.logger.debug(f"[ПРОПУСК webike] {title}")

    def parse_model_info(self, response):
        self.stats['models_processed'] += 1
        normalized_name = response.meta["normalized"]
        title = response.meta["title"]

        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table", class_="md-specifications_table")
        if not table:
            self.logger.info(f"[ПРОПУСК webike] Не найдена таблица для {title}")
            return

        matching_motos = self.normalized_motos.get(normalized_name, [])
        if not matching_motos:
            return
            
        for moto in matching_motos:
            item = MotosScraperItem()
            item["source"] = "webike"
            item["source_url"] = response.url
            item["api_id"] = moto.get("api_id")
            item["brand"] = moto.get("brand")
            item["model"] = moto.get("model")
            item["year"] = moto.get("year")

            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 4:
                    continue

                header1 = tds[0].get_text(strip=True)
                value1 = tds[1].get_text(strip=True)
                header2 = tds[2].get_text(strip=True)
                value2 = tds[3].get_text(strip=True)

                if header1 == "производитель":
                    item["brand"] = value1
                elif header1 == "Название модели":
                    item["model"] = value1
                elif header1 == "Год выпуска":
                    item["year"] = value1
                elif header2 == "Тип двигателя":
                    item["engine_type"] = value2
                elif header1 == "Объём двигателя":
                    match = re.search(r'(\d+)', value1)
                    if match:
                        item["engine_displacement_cc"] = match.group(1)
                elif header2 == "Максимальная мощность (л.с)":
                    power_match = re.search(r'(\d+)ps', value2)
                    rpm_match = re.search(r'/(\d+)rpm', value2)
                    if power_match:
                        item["engine_power_hp"] = power_match.group(1)
                    if rpm_match:
                        item["engine_power_rpm"] = rpm_match.group(1)
                elif header2 == "Максимальный крутящий момент (кгс*м)":
                    torque_match = re.search(r'(\d+\.?\d*)N・m', value2)
                    rpm_match = re.search(r'/(\d+)rpm', value2)
                    if torque_match:
                        item["engine_torque_nm"] = torque_match.group(1)
                    if rpm_match:
                        item["engine_torque_rpm"] = rpm_match.group(1)
                elif header2 == "Вес мотоцикла (Сухой вес)":
                    match = re.search(r'(\d+)', value2)
                    if match:
                        item["dry_weight_kg"] = match.group(1)
                elif header2 == "Снаряженная масса":
                    match = re.search(r'(\d+)', value2)
                    if match:
                        item["wet_weight_kg"] = match.group(1)
                elif header1 == "Емкость топливного бака":
                    match = re.search(r'(\d+\.?\d*)', value1)
                    if match:
                        item["fuel_capacity_l"] = match.group(1)

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

            # СОХРАНЕНИЕ ТОЛЬКО ПРИ НАЙДЕННОЙ МОЩНОСТИ
            if item.get("engine_power_hp"):
                self.stats['items_with_power'] += 1
                self.stats['unique_items'].add(item['api_id'])
                self.logger.info(f"[НАШЕЛ webike] {title} - {item.get('engine_power_hp')}")
                yield item

            else:
                self.logger.info(f"[НАШЕЛ webike] {title} - нет мощности")

            # СОХРАНЕНИЕ ДАЖЕ ПРИ ОТСУТСТВИИ МОЩНОСТИ
            # self.stats['unique_items'].add(item['api_id'])
            # self.logger.info(f"[НАШЕЛ webike] {title}")
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