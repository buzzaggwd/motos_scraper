from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from motos_scraper.spiders.bikez_spider import BikezSpider
from motos_scraper.spiders.webike_spider import WebikeSpider
from motos_scraper.spiders.fastestlaps_spider import FastestlapsSpider
from motos_scraper.spiders.cse_spider import CseSpider

process = CrawlerProcess(get_project_settings())

process.crawl(BikezSpider)
process.crawl(WebikeSpider)
process.crawl(FastestlapsSpider)
process.crawl(CseSpider)

process.start()