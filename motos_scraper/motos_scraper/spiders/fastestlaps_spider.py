import scrapy
from bs4 import BeautifulSoup
import json
import re
import logging
import random
from scrapy_playwright.page import PageMethod
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from motos_scraper.items import MotosScraperItem

with open("../motos.json", "r", encoding="utf-8") as f:
    motos = json.load(f)

logger = logging.getLogger(__name__)

def normalize(s):
    if not s:
        return ""
    s = s.lower().replace(" ", "")
    # s = re.sub(r'[()\[\]{}]', '', s)
    # s = re.sub(r'[^a-z0-9]', '', s)
    return s

class FastestlapsSpider(scrapy.Spider):
    name = "fastestlaps_spider_alone"
    start_urls = ["https://fastestlaps.com/makes"]

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
        }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.motos = motos
        self.moto_map = {normalize(moto.get("model")): moto for moto in self.motos}

    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        for ul in soup.select("ul.fl-indexlist"):
            for li in ul.find_all("li"):
                a_tag = li.find("a")
                brand_text = a_tag.text.strip()
                if any(brand in brand_text for brand in ["Honda", "Kawasaki", "Harley-Davidson", "BMW", "Yamaha", "KTM", "Suzuki", "Triumph", "Ducati", "Buell"]):
                    href = a_tag.get("href")
                    
                    yield scrapy.Request(
                        url=response.urljoin(href),
                        callback=self.parse_models_urls
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
                    item["name"] = name
                    item["url"] = response.urljoin(href)

                    yield scrapy.Request(
                        url=response.urljoin(href),
                        callback=self.check_if_motocycle,
                        meta={"item": item},
                        dont_filter=True
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
            yield scrapy.Request(
                url=item["url"],
                callback=self.parse_models_info,
                meta={"item": item}
            )
        else:
            logger.info(f"[ПРОПУСК fastestlaps] {item['name']} не является мотоциклом")

    def parse_models_info(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        item = response.meta["item"]
        tables = soup.find_all("table", class_="fl-datasheet")

        normalized_name = normalize(item["name"])
        if normalized_name in self.moto_map:
            moto = self.moto_map[normalized_name]

        item["api_id"] = moto.get("api_id")
        item["source"] = "fastestlaps"
        item["brand"] = moto.get("brand")
        item["model"] = moto.get("model")
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
                    match = re.search(r'(\d+)\s*ps', value)
                    if match:
                        item["engine_power_hp"] = match.group(1)

                elif header == "Power":
                    match = re.search(r'(\d+)\s*rpm', value)
                    if match:
                        item["engine_power_rpm"] = match.group(1)

                elif header == "Torque":
                    match = re.search(r'(\d+)\s*Nm', value)
                    if match:
                        item["engine_torque_nm"] = match.group(1)

                elif header == "Torque":
                    match = re.search(r'(\d+)\s*rpm', value)
                    if match:
                        item["engine_torque_rpm"] = match.group(1)

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

                    logger.info(f"[НАШЕЛ fastestlaps] {moto.get('model')} - {moto.get('power')}")
                else:
                    logger.info(f"[ПРОПУСК fastestlaps] {moto.get('model')}")

        yield item
