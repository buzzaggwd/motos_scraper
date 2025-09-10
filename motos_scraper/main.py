import sys
import os
import asyncio

if sys.platform == 'win32':
    os.environ['TWISTED_REACTOR'] = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from loguru import logger
import json
import sqlite3

logger.remove(0)

logger.add("statistics.log", rotation="500 MB", format="{time:YYYY-MM-DD HH:mm:ss} SYSTEM: {message}", filter=lambda record: "spider" not in record["extra"])
logger.add(sys.stderr, format="<i><lg>{time:YYYY-MM-DD HH:mm:ss}</lg></i> <l><lw>SYSTEM:</lw></l> {message}", colorize=True,  filter=lambda record: "spider" not in record["extra"])

logger.add("statistics.log", rotation="500 MB", format="{time:YYYY-MM-DD HH:mm:ss} BIKEZ: {message}", filter=lambda record: record["extra"].get("spider") == "bikez")
logger.add(sys.stderr, format="<i><lg>{time:YYYY-MM-DD HH:mm:ss}</lg></i> <lr>BIKEZ:</lr> {message}", colorize=True, filter=lambda record: record["extra"].get("spider") == "bikez")

logger.add("statistics.log", rotation="500 MB", format="{time:YYYY-MM-DD HH:mm:ss} WEBIKE: {message}", filter=lambda record: record["extra"].get("spider") == "webike")
logger.add(sys.stderr, format="<i><lg>{time:YYYY-MM-DD HH:mm:ss}</lg></i> <lc>WEBIKE:</lc> {message}", colorize=True, filter=lambda record: record["extra"].get("spider") == "webike")

logger.add("statistics.log", rotation="500 MB", format="{time:YYYY-MM-DD HH:mm:ss} FASTESTLAPS: {message}", filter=lambda record: record["extra"].get("spider") == "fastestlaps")
logger.add(sys.stderr, format="<i><lg>{time:YYYY-MM-DD HH:mm:ss}</lg></i> <lm>FASTESTLAPS:</lm> {message}", colorize=True, filter=lambda record: record["extra"].get("spider") == "fastestlaps")

logger.add("statistics.log", rotation="500 MB", format="{time:YYYY-MM-DD HH:mm:ss} CSE: {message}", filter=lambda record: record["extra"].get("spider") == "cse")
logger.add(sys.stderr, format="<i><lg>{time:YYYY-MM-DD HH:mm:ss}</lg></i> <ly>CSE:</ly> {message}", colorize=True, filter=lambda record: record["extra"].get("spider") == "cse")


with open("../motos.json", "r", encoding="utf-8") as file:
    motos = json.load(file)

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from motos_scraper.spiders.bikez_spider import BikezSpider
from motos_scraper.spiders.webike_spider import WebikeSpider
from motos_scraper.spiders.fastestlaps_spider import FastestlapsSpider
# from motos_scraper.spiders.cse_spider import CseSpider

process = CrawlerProcess(get_project_settings())

logger.info(f"Всего мотоциклов для обработки: {len(motos)}")

process.crawl(BikezSpider)
process.crawl(WebikeSpider)
# process.crawl(FastestlapsSpider)
# process.crawl(CseSpider)

process.start()


# ЕСЛИ ОБЩАЯ БАЗА
def get_api_ids():
    try:
        conn = sqlite3.connect("motos.db")
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM motos")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except sqlite3.Error as e:
        return 0

logger.info(f"Всего собрано мотоцклов: {get_api_ids()}")


# ЕСЛИ БАЗА ДЛЯ КАЖДОГО ПАУКА СВОЯ
# def get_api_ids(db_path):
#     try:
#         conn = sqlite3.connect(db_path)
#         cur = conn.cursor()
#         cur.execute("SELECT COUNT(*) FROM motos")
#         count = cur.fetchone()[0]
#         cur.close()
#         conn.close()
#         return count
#     except sqlite3.Error as e:
#         return 0

# spiders_db = {
#     "motos_bikez_spider.db",
#     "motos_webike_spider.db",
#     "motos_fastestlaps_spider.db",
#     "motos_cse_spider.db"
# }

# for spider_db in spiders_db:
#     count = get_api_ids(spider_db)
#     logger.info(f"Собрано мотоцклов в {spider_db}: {count}")