import scrapy
import json
import re
import random
from scrapy_playwright.page import PageMethod
from motos_scraper.items import MotosScraperItem
from scrapy.exceptions import CloseSpider
import requests
import sqlite3

with open("../motos.json", "r", encoding="utf-8") as f:
    motos = json.load(f)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:118.0) Gecko/20100101 Firefox/118.0"
]

def check_proxy(proxy_str, timeout=5):
    try:
        parts = proxy_str.split(":")
        if len(parts) == 4:
            ip, port, login, password = parts
            proxy_url = f"http://{login}:{password}@{ip}:{port}"
            resp = requests.get("https://httpbin.org/ip", 
                                proxies={"http": proxy_url, "https": proxy_url}, 
                                timeout=timeout)
            print(f"Proxy {proxy_str} works: {resp.status_code}")
            return resp.status_code == 200
    except Exception:
        return False
    return False

with open("../proxies.txt", "r") as f:
    raw_proxies = [line.strip() for line in f if line.strip()]
    PROXIES = [p for p in raw_proxies if check_proxy(p)]

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

class CseSpider(scrapy.Spider):
    name = "cse_spider"
    
    IDLE_TIMEOUT = 300

    custom_settings = {
        "LOG_LEVEL": "INFO",
        "DOWNLOAD_TIMEOUT": 180,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504],
        "DOWNLOAD_TIMEOUT": 180,
        "CLOSESPIDER_ERRORCOUNT": 25,
        "DOWNLOAD_DELAY": random.uniform(1, 3),
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,
        "PLAYWRIGHT_ABORT_REQUEST": lambda req: req.resource_type in ["image", "media", "font", "stylesheet"],
        "USER_AGENT_ROTATION": True,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
        },
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "ignore_https_errors": True,
            },
        },
        "PLAYWRIGHT_CLOSE_PAGE": True,
        "PLAYWRIGHT_CLOSE_CONTEXT": True,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.motos = motos
        self.results = []
        self.zero_items_in_row = 0
        self.existing_api_ids = get_existing_api_ids()

    def parse_proxy(self, proxy_str):
        parts = proxy_str.split(":")
        if len(parts) == 4:
            ip, port, login, password = parts
            return {
                "server": f"http://{ip}:{port}",
                "username": login,
                "password": password,
            }
        return None

    def start_requests(self):
        for moto in self.motos:
            if moto["api_id"] in self.existing_api_ids:
                continue

            model = moto["model"].replace(" ", "%20")
            url = f"https://cse.google.co.za/cse?cx=partner-pub-6706711710399117:3179068794&ie=UTF-8&sa=Search&q={model}"
            
            proxy = random.choice(PROXIES) if PROXIES else None
            ua = random.choice(USER_AGENTS)

            playwright_meta = {
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_goto_timeout": 20000,
                "playwright_page_methods": [
                    PageMethod("set_extra_http_headers", {
                        "User-Agent": ua,
                        "Accept-Language": "en-US,en;q=0.9"
                    }),
                    PageMethod("evaluate", """() => {
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    }"""),
                    PageMethod("wait_for_selector", "div.gsc-expansionArea", state="attached", timeout=120000),
                    PageMethod("wait_for_timeout", random.randint(2000, 5000)),
                ],
                "playwright_page_goto_kwargs": {"wait_until": "domcontentloaded"},
                "moto": moto,
            }

            if proxy:
                proxy_conf = self.parse_proxy(proxy)
                if proxy_conf:
                    playwright_meta["playwright_context"] = f"context_{proxy_conf['server']}"
                    playwright_meta["playwright_context_kwargs"] = {
                        "proxy": proxy_conf
                    }
            
            yield scrapy.Request(
                url=url,
                meta=playwright_meta,
                callback=self.parse_cse,
                dont_filter=True,
                errback=self.errback_handler,
            )

    def parse_cse(self, response):
        page = response.meta["playwright_page"]
        moto = response.meta["moto"]

        link = response.css("div.gsc-expansionArea a.gs-title::attr(href)").get()
        if link:
            self.logger.info(f"[НАШЕЛ cse] ссылка для {moto["model"]}")
            yield scrapy.Request(
                url=link,
                callback=self.parse_moto_page,
                meta={"moto": moto, "link": link},
                dont_filter=False,
                errback=self.errback_handler,
            )
        else:
            self.logger.info(f"[ПРОПУСК cse] нет ссылки {moto["model"]}")

        if page and not page.is_closed():
            page.close()

    def parse_moto_page(self, response):
        moto = response.meta["moto"]
        trs = response.css("tr")

        self.logger.info(f"[cse] Зашел на страницу {response.url} для {moto['model']}")

        if moto:
            item = MotosScraperItem()
            item["api_id"] = moto["api_id"]
            item["source"] = "cse"
            item["source_url"] = response.url
            item["brand"] = moto["brand"]
            item["model"] = moto["model"]
            item["year"] = moto["year"]

            for tr in trs:
                header = ' '.join(tr.xpath("td[1]//text()").getall()).strip()
                value = ' '.join(tr.xpath("td[2]//text()").getall()).strip()

                if header and value:
                    header_clean = ' '.join(header.lower().split())
                    value_clean = ' '.join(value.split())

                    if "engine" in header_clean:
                        item["engine_type"] = value_clean

                    elif "capacity" in header_clean:
                        match = re.search(r'(\d+)', value_clean)
                        if match:
                            item["engine_displacement_cc"] = match.group(1)

                    elif "power" in header_clean:
                        hp_match = re.search(r'([\d.]+)\s*hp', value_clean.lower())
                        rpm_match = re.search(r'([\d.]+)\s*rpm', value_clean.lower())
                        if hp_match:
                            item["engine_power_hp"] = hp_match.group(1)
                        if rpm_match:
                            item["engine_power_rpm"] = rpm_match.group(1)

                    elif "torque" in header_clean:
                        nm_match = re.search(r'([\d.]+)\s*Nm', value_clean.lower())
                        rpm_match = re.search(r'([\d.]+)\s*rpm', value_clean.lower())
                        if nm_match:
                            item["engine_torque_nm"] = nm_match.group(1)
                        if rpm_match:
                            item["engine_torque_rpm"] = rpm_match.group(1)

                    elif "transmission" in header_clean:
                        item["gearbox"] = value_clean

                    elif "clutch" in header_clean:
                        item["transmission_clutch"] = value_clean

                    elif "secondary drive" in header_clean:
                        item["transmission_type"] = value_clean

                    elif "abs" in header_clean:
                        item["abs_type"] = value_clean

                    elif "dry weight" in header_clean:
                        match = re.search(r'(\d+)', value_clean)
                        if match:
                            item["dry_weight_kg"] = match.group(1)

                    elif "wet weight" in header_clean:
                        match = re.search(r'(\d+)', value_clean)
                        if match:
                            item["wet_weight_kg"] = match.group(1)

                    elif "fuel capacity" in header_clean:
                        match = re.search(r'(\d+)', value_clean)
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
                else:
                    item["origin_country"] = None

            self.zero_items_in_row = 0

            # СОХРАНЕНИЕ ТОЛЬКО ПРИ НАЙДЕННОЙ МОЩНОСТИ
            if item.get("engine_power_hp"):
                self.logger.info(f"[НАШЕЛ cse] {item.get('model')} - {item.get('engine_power_hp')}")
                yield item
            else:
                self.logger.info(f"[ПРОПУСК cse] {item.get('model')}")


            # СОХРАНЕНИЕ ДАЖЕ ПРИ ОТСУТСТВИИ МОЩНОСТИ
            # self.logger.info(f"[НАШЕЛ cse] {item.get('model')}")
            # yield item

        else:
            self.logger.info(f"[ПРОПУСК cse] {moto['model']} не найден")
            self.zero_items_in_row += 1
            if self.zero_items_in_row >= 10:
                raise CloseSpider(f"Превышен лимит пустых страниц: {self.zero_items_in_row}")


    async def errback_handler(self, failure):
        self.logger.error(f'Ошибка парсинга cse: {failure.value}')
        self.crawler.stats.inc_value('failed_request_count')
        fail_request = failure.request
        page = fail_request.meta.get("playwright_page")
        if page:
            try:
                if not page.is_closed():
                    await page.close()
            except Exception:
                pass