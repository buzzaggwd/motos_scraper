from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from motos_scraper.spiders.bikez_spider import BikezSpider
from motos_scraper.spiders.webike_spider import WebikeSpider

process = CrawlerProcess(get_project_settings())

process.crawl(BikezSpider)
process.crawl(WebikeSpider)

process.start()