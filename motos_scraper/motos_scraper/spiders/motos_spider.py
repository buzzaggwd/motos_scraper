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



# class BikezSpider(scrapy.Spider):
#     name = "bikez_spider"
#     start_urls = ["https://bikez.com/years/index.php"]

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.motos = motos
#         self.to_update = []

#     def parse(self, response):
#         year_tags = response.css("td.even a, td.odd a")
#         for tag in year_tags:
#             year_url = response.urljoin(tag.attrib['href'])
#             yield scrapy.Request(
#                 url=year_url, 
#                 callback=self.parse_year
#             )

#     def parse_year(self, response):
#         rows = response.css("tr.even, tr.odd")

#         year_match = re.search(r'(\d{4})', response.url)
#         year = int(year_match.group(1)) if year_match else 0

#         for row in rows:
#             a_tag = row.css("a::attr(href)").get()
#             model_name = row.css("a::text").get()
#             if a_tag and model_name:
#                 model_url = response.urljoin(a_tag)
#                 normalized = normalize(model_name)
#                 if any(normalize(moto["model"]) == normalized and not moto.get("power") for moto in self.motos):

#                     yield scrapy.Request(
#                         url=model_url, 
#                         callback=self.parse_model, 
#                         meta={
#                             "model_name": model_name, 
#                             "year": year
#                         }
#                     )

#     def parse_model(self, response):
#         model_name = response.meta["model_name"]
#         soup = BeautifulSoup(response.text, "html.parser")
#         power = None
#         for trs in soup.find_all("tr"):
#             tds = trs.find_all("td")
#             if len(tds) < 2:
#                 continue
#             header = tds[0].get_text(strip=True)
#             if header in ["Power", "Output"] and "..." not in tds[1].get_text():
#                 match = re.search(r'([\d.]+)', tds[1].get_text())
#                 if match:
#                     power = match.group(1)
#                     break

#         for moto in self.motos:
#             if normalize(moto["model"]) == normalize(model_name) and not moto.get("power"):
#                 if power:
#                     moto["power"] = power
#                     self.logger.info(f"[НАШЕЛ bikez] {model_name} - {power}")
#                 else:
#                     moto["power"] = None
#                     self.logger.info(f"[ПРОПУСК bikez] {model_name}")

#     def closed(self, reason):
#         with open(outputfile, "w", encoding="utf-8") as f:
#             json.dump(self.motos, f, ensure_ascii=False, indent=2)




# class FastestlapsSpider(scrapy.Spider):
#     name = "fastestlaps_spider"
#     start_urls = ["https://fastestlaps.com/makes"]

#     custom_settings = {
#         "DEFAULT_REQUEST_HEADERS": {
#             "User-Agent": (
#                 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                 "AppleWebKit/537.36 (KHTML, like Gecko) "
#                 "Chrome/131.0.0.0 Safari/537.36"
#             )
#         }
#     }

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.motos = motos
#         self.moto_map = {normalize(moto.get("model")): moto for moto in self.motos}

#     def parse(self, response):
#         soup = BeautifulSoup(response.text, 'html.parser')
#         for ul in soup.select("ul.fl-indexlist"):
#             for li in ul.find_all("li"):
#                 a_tag = li.find("a")
#                 brand_text = a_tag.text.strip()
#                 if any(brand in brand_text for brand in ["Honda", "Kawasaki", "Harley-Davidson", "BMW", "Yamaha", "KTM", "Suzuki", "Triumph", "Ducati", "Buell"]):
#                     href = a_tag.get("href")
                    
#                     yield scrapy.Request(
#                         url=response.urljoin(href),
#                         callback=self.parse_models_urls
#                     )

#     def parse_models_urls(self, response):
#         soup = BeautifulSoup(response.text, 'html.parser')
#         for ul in soup.select("ul.fl-indexlist"):
#             for li in ul.find_all("li"):
#                 a_tag = li.find("a")
#                 if a_tag:
#                     href = a_tag.get("href")
#                     name = a_tag.text.strip()
#                     item = {"name": name, "url": response.urljoin(href)}

#                     yield scrapy.Request(
#                         url=response.urljoin(href),
#                         callback=self.check_if_motocycle,
#                         meta={"item": item},
#                         dont_filter=True
#                     )

#     def check_if_motocycle(self, response):
#         soup = BeautifulSoup(response.text, 'html.parser')
#         item = response.meta["item"]
#         table = soup.find("table", class_="fl-datasheet")
#         is_moto = False

#         if table:
#             for tr in table.find_all("tr"):
#                 tds = tr.find_all("td")
#                 if len(tds) >= 2 and "Motorcycle type" in tds[0].get_text(strip=True):
#                     is_moto = True
#                     break

#         if is_moto:
#             yield scrapy.Request(
#                 url=item["url"],
#                 callback=self.parse_models_info,
#                 meta={"item": item}
#             )
#         else:
#             logger.info(f"[ПРОПУСК fastestlaps] {item['name']} не является мотоциклом")

#     def parse_models_info(self, response):
#         soup = BeautifulSoup(response.text, 'html.parser')
#         item = response.meta["item"]
#         tables = soup.find_all("table", class_="fl-datasheet")

#         for table in tables:
#             for tr in table.find_all("tr"):
#                 tds = tr.find_all("td")
#                 if len(tds) < 2:
#                     continue
#                 header = tds[0].get_text(strip=True)
#                 value = tds[1].get_text(strip=True)
#                 match = re.search(r'\d+', value)
#                 if "Power" == header.strip():
#                     item["power"] = match.group() if match else None

#         normalized_name = normalize(item["name"])
#         if normalized_name in self.moto_map:
#             moto = self.moto_map[normalized_name]
#             if "power" not in moto or not moto["power"]:
#                 if "power" in item and item["power"]:
#                     moto["power"] = item["power"]
#                     logger.info(f"[НАШЕЛ fastestlaps] {moto['model']} - {moto['power']}")
#                 else:
#                     logger.info(f"[ПРОПУСК fastestlaps] {moto['name']}")

#         yield item

#     def closed(self, reason):
#         with open(outputfile, "w", encoding="utf-8") as final_file:
#             json.dump(self.motos, final_file, ensure_ascii=False, indent=2)




# class WebikeSpider(scrapy.Spider):
#     name = "webike_spider"
#     start_urls = [
#             "https://www.webike.com.ru/Moto/honda/",
#             "https://www.webike.com.ru/Moto/kawasaki/",
#             "https://www.webike.com.ru/Moto/harley-davidson/",
#             "https://www.webike.com.ru/Moto/bmw/",
#             "https://www.webike.com.ru/Moto/yamaha/",
#             "https://www.webike.com.ru/Moto/ktm/",
#             "https://www.webike.com.ru/Moto/suzuki/",
#             "https://www.webike.com.ru/Moto/triumph/",
#             "https://www.webike.com.ru/Moto/ducati/",
#             "https://www.webike.com.ru/Moto/buell/",
#         ]

#     custom_settings = {
#             "DEFAULT_REQUEST_HEADERS": {
#                 "User-Agent": (
#                     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                     "AppleWebKit/537.36 (KHTML, like Gecko) "
#                     "Chrome/131.0.0.0 Safari/537.36"
#                 )
#             }
#         }

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.motos = motos
#         self.normalized_motos = {}
#         for moto in motos:
#             norm = normalize(moto["model"])
#             self.normalized_motos.setdefault(norm, []).append(moto)

#     def parse(self, response):
#         soup = BeautifulSoup(response.text, "html.parser")

#         for div in soup.select("div.model_name"):
#             a_tag = div.find("a")
#             if not a_tag:
#                 continue
#             title = a_tag.get_text(strip=True)
#             href = a_tag.get("href")
#             modified_url = href.replace("mtop/", "m-spec/")

#             normalized_name = normalize(title)

#             if normalized_name in self.normalized_motos:
#                 yield scrapy.Request(
#                     url=modified_url,
#                     callback=self.parse_model_info,
#                     meta={"normalized": normalized_name, "title": title},
#                     dont_filter=True,
#                 )
#             else:
#                 self.logger.debug(f"[ПРОПУСК webike] {title}")

#     def parse_model_info(self, response):
#         soup = BeautifulSoup(response.text, "html.parser")
#         normalized_name = response.meta["normalized"]

#         table = soup.find("table", class_="md-specifications_table")
#         if not table:
#             return
#         data = {}
#         for tr in table.find_all("tr"):
#             tds = tr.find_all("td")
#             if len(tds) < 4:
#                 continue
#             header = tds[2].get_text(strip=True)
#             value = tds[3].get_text(strip=True)
#             match = re.search(r"\d+", value)
#             if "Максимальная мощность" in header:
#                 data["power"] = match.group() if match else None

#         if normalized_name in self.normalized_motos:
#             for moto in self.normalized_motos[normalized_name]:
#                 for k, v in data.items():
#                     if v and (k not in moto or not moto[k]):
#                         moto[k] = v
#             self.logger.info(f"[НАШЕЛ webike] {response.meta['title']} - {data['power']}")

#     def closed(self, reason):
#         with open(outputfile, "w", encoding="utf-8") as f:
#             all_motos = []
#             for models in self.normalized_motos.values():
#                 all_motos.extend(models)
#             json.dump(all_motos, f, ensure_ascii=False, indent=2)




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
                    dont_filter=True,
                )
            else:
                yield moto
                self.results.append(moto)
        except Exception as e:
            self.logger.info(f"[ПРОПУСК cse] нет ссылки {moto["model"]}")

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




if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    # process.crawl(BikezSpider)
    # process.crawl(FastestlapsSpider)
    # process.crawl(WebikeSpider)
    process.crawl(CseSpider)
    process.start()