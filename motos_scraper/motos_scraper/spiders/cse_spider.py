import scrapy
from bs4 import BeautifulSoup
import json
import re
import logging
import random
from scrapy_playwright.page import PageMethod
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

with open("../motos.json", "r", encoding="utf-8") as f:
    motos = json.load(f)

outputfile = "../../../motos_final_cse.json"

logger = logging.getLogger(__name__)

def normalize(s):
    if not s:
        return ""
    s = s.lower().replace(" ", "")
    s = re.sub(r'[()\[\]{}]', '', s)
    s = re.sub(r'[^a-z0-9]', '', s)
    return s

class CseSpider(scrapy.Spider):
    name = "cse_spider"
    custom_settings = {
        "CONCURRENT_REQUESTS": 10, 
        "LOG_LEVEL": "INFO",
        "PLAYWRIGHT_MAX_PAGES_PER_CONTEXT": 4,
        "DOWNLOAD_TIMEOUT": 60,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504],
        "CLOSESPIDER_ITEMCOUNT": 500,
        "CLOSESPIDER_PAGECOUNT": 700,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.motos = motos
        self.results = []

    def start_requests(self):
        for moto in self.motos:
            if "power" not in moto:
                model = moto["model"].replace(" ", "%20")
                url = f"https://cse.google.co.za/cse?cx=partner-pub-6706711710399117:3179068794&ie=UTF-8&sa=Search&q={model}"
                yield scrapy.Request(
                    url=url,
                    meta={
                        "playwright": True,
                        "playwright_include_page": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_selector", "div.gsc-expansionArea", state="attached") # timeout=5000,
                        ],
                        "playwright_launch_options": {
                            "headless": True,
                        },
                        "moto": moto,
                    },
                    callback=self.parse_cse,
                    dont_filter=True
                )

    async def parse_cse(self, response):
        page = response.meta["playwright_page"]
        moto = response.meta["moto"]

        try:
            link = response.css("div.gsc-expansionArea a.gs-title::attr(href)").get()

            await page.close()

            if link:
                yield scrapy.Request(
                    link,
                    callback=self.parse_moto_page,
                    meta={"moto": moto, "link": link},
                    dont_filter=False,
                )
            else:
                yield moto
                self.results.append(moto)
        except Exception as e:
            self.logger.info(f"[ПРОПУСК cse] нет ссылки {moto["model"]}")

        finally:
            await page.close()

    async def parse_moto_page(self, response):
        moto = response.meta["moto"]
        trs = response.css("tr")
        found_power = False

        for tr in trs:
            header = tr.xpath("string(td[1])").get()
            value = tr.xpath("string(td[2])").get()

            if header and value:
                header_clean = " ".join(header.lower().split())
                value_clean = " ".join(value.split())

                if "power" in header_clean:
                    match = re.search(r"\d+(\.\d+)?", value_clean)
                    if match:
                        moto["power"] = match.group()
                        found_power = True
                        self.logger.info(f"[НАШЕЛ cse] {moto['model']} - {moto['power']}")
                        break

        if not found_power:
            self.logger.info(f"[ПРОПУСК cse] {moto['model']}")

        yield moto
        self.results.append(moto)
    
    def closed(self, reason):
        with open(outputfile, "w", encoding="utf-8") as final_file:
            json.dump(self.results, final_file, ensure_ascii=False, indent=2)
