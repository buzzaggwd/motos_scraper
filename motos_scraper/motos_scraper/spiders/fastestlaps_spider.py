import scrapy
from bs4 import BeautifulSoup
import json
import re
import logging
from motos_scraper.items import MotosScraperItem
from difflib import SequenceMatcher
import random
from scrapy.exceptions import CloseSpider

with open("../motos.json", "r", encoding="utf-8") as f:
    motos = json.load(f)

logger = logging.getLogger(__name__)

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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
]

class FastestlapsSpider(scrapy.Spider):
    name = "fastestlaps_spider"
    start_urls = ["https://fastestlaps.com/makes"]

    custom_settings = {
        "DOWNLOAD_TIMEOUT": 30,
        "RETRY_TIMES": 5,
        'DOWNLOADER_MIDDLEWARES': {
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 350,
        },
        "CLOSESPIDER_ERRORCOUNT": 100,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.motos = motos
        self.moto_map = {normalize(moto.get("model")): moto for moto in self.motos}
        self.zero_items_in_row = 0

    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        brand_links = []
        for ul in soup.select("ul.fl-indexlist"):
            for li in ul.find_all("li"):
                a_tag = li.find("a")
                brand_text = a_tag.text.strip()
                if any(brand in brand_text for brand in ["Honda", "Kawasaki", "Harley-Davidson", "BMW", "Yamaha", "KTM", "Suzuki", "Triumph", "Ducati", "Buell"]):
                    href = a_tag.get("href")
                    brand_links.append(response.urljoin(href))

        for url in brand_links:
            yield scrapy.Request(
                url=url,
                callback=self.parse_models_urls,
                headers={"User-Agent": random.choice(USER_AGENTS)},
                errback=self.errback_handle,
            )

    def parse_models_urls(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        for ul in soup.select("ul.fl-indexlist"):
            for li in ul.find_all("li"):
                a_tag = li.find("a")
                if a_tag:
                    href = a_tag.get("href")
                    name = a_tag.text.strip()
                    item = MotosScraperItem()
                    item["model"] = name
                    item["source_url"] = response.urljoin(href)

                    yield scrapy.Request(
                        url=response.urljoin(href),
                        callback=self.check_if_motocycle,
                        meta={"item": item},
                        dont_filter=True,
                        headers={"User-Agent": random.choice(USER_AGENTS)},
                        errback=self.errback_handle,
                    )

    def check_if_motocycle(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        item = response.meta["item"]
        table = soup.find("table", class_="fl-datasheet")
        is_moto = False

        if table:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2 and "Motorcycle type" in tds[0].get_text(strip=True):
                    is_moto = True
                    break

        if is_moto:
            self.logger.info(f"[ПОЛУЧЕН fastestlaps] {item['model']}")
            yield scrapy.Request(
                url=item["source_url"],
                callback=self.parse_models_info,
                meta={"item": item},
                headers={"User-Agent": random.choice(USER_AGENTS)},
                errback=self.errback_handle,
            )
        else:
            self.logger.info(f"[ПРОПУСК fastestlaps] {item['model']} не является мотоциклом")

    def parse_models_info(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        item = response.meta["item"]
        tables = soup.find_all("table", class_="fl-datasheet")

        normalized_name = normalize(item["model"])

        match = None
        max_similarity = 0.0
        moto = None

        for key in self.moto_map.keys():
            sc = similar(normalized_name, key)
            if sc > max_similarity:
                max_similarity = sc
                match = key
        if match and max_similarity >= 0.90:
            moto = self.moto_map[match]

        if moto:
            item["api_id"] = moto.get("api_id")
            item["source"] = "fastestlaps"
            item["brand"] = moto.get("brand")
            item["year"] = moto.get("year")

            for table in tables:
                for tr in table.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) < 2:
                        continue
                    header = tds[0].get_text(strip=True)
                    value = tds[1].get_text(strip=True)

                    if header == "Motorcycle type":
                        item["category"] = value

                    elif header == "Origin country":
                        item["origin_country"] = value

                    elif header == "Engine type":
                        item["engine_type"] = value

                    elif header == "Displacement":
                        match = re.search(r'(\d+)\s*cc', value)
                        if match:
                            item["engine_displacement_cc"] = match.group(1)

                    elif header == "Power":
                        hp_match = re.search(r'(\d+)\s*ps', value, re.I)
                        rpm_match = re.search(r'(\d+)\s*rpm', value, re.I)
                        if hp_match:
                            item["engine_power_hp"] = hp_match.group(1)
                        if rpm_match:
                            item["engine_power_rpm"] = rpm_match.group(1)

                    elif header == "Torque":
                        nm_match = re.search(r'(\d+)\s*Nm', value)
                        if nm_match:
                            item["engine_torque_nm"] = nm_match.group(1)
                        rpm_match = re.search(r'(\d+)\s*rpm', value)
                        if rpm_match:
                            item["engine_torque_rpm"] = rpm_match.group(1)

                    elif header == "Transmission":
                        item["gearbox"] = value

                    elif header == "Curb weight":
                        match = re.search(r'(\d+)\s*kg', value)
                        if match:
                            item["wet_weight_kg"] = match.group(1)

                    elif header == "Top speed":
                        match = re.search(r'(\d+)\s*kph', value)
                        if match:
                            item["top_speed_kph"] = match.group(1)

                    elif header == "Gas mileage":
                        match = re.search(r'(\d+)\s*l/100 km', value)
                        if match:
                            item["fuel_consumption_l_per_100km"] = match.group(1)

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

            self.zero_items_in_row = 0

            # СОХРАНЕНИЕ ТОЛЬКО ПРИ НАЙДЕННОЙ МОЩНОСТИ
            if item.get("engine_power_hp"):
                self.logger.info(f"[НАШЕЛ fastestlaps] {moto.get('model')} - {moto.get('engine_power_hp')}")
                yield item
            else:
                self.logger.info(f"[ПРОПУСК fastestlaps] {moto.get('model')}")


            # СОХРАНЕНИЕ ДАЖЕ ПРИ ОТСУТСТВИИ МОЩНОСТИ
            # self.logger.info(f"[НАШЕЛ fastestlaps] {item['model']}")
            # yield item


        else:
            self.logger.info(f"[ПРОПУСК fastestlaps] {item['model']} не найден")
            self.zero_items_in_row += 1
            if self.zero_items_in_row >= 10:
                raise CloseSpider(f"Превышен лимит пустых страниц: {self.zero_items_in_row}")


    def errback_handler(self, failure):
        self.logger.error(f'Ошибка парсинга fastestlaps: {failure.value}')
        self.crawler.stats.inc_value('failed_request_count')