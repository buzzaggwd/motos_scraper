# Scrapy settings for motos_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "motos_scraper"

SPIDER_MODULES = ["motos_scraper.spiders"]
NEWSPIDER_MODULE = "motos_scraper.spiders"

ADDONS = {}


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "motos_scraper (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Concurrency and throttling settings
CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 8
DOWNLOAD_DELAY = 1

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
   "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                )
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "motos_scraper.middlewares.MotosScraperSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
# #    "motos_scraper.middlewares.MotosScraperDownloaderMiddleware": 543,
# #    "motos_scraper.middlewares.CustomProxyMiddleware": 350,
#    "rotating_proxies.middlewares.RotatingProxyMiddleware": 350,
# }

def load_proxies(path):
    proxies = []
    with open(path, 'r') as file:
        for line in file:
            line = line.strip()
            if line:
                parts = line.split(':')
                if len(parts) == 4:
                    ip, port, login, password = parts
                    proxy_url = f"http://{login}:{password}@{ip}:{port}"
                    proxies.append(proxy_url)
    return proxies

ROTATING_PROXY_LIST = load_proxies("../proxies.txt")

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
#    "motos_scraper.pipelines.MotosScraperPipeline": 300,
    "motos_scraper.pipelines.MotosScraperPipelineNew": 300,
#    "motos_scraper.pipelines.MotosJsonPipeline": 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"


# PLAYWRIGHT    
# This configuration ensures that requests flagged with the playwright=True meta key will be processed by Playwright
# Requests without this flag will be handled by Scrapy’s default download handler
# DOWNLOAD_HANDLERS = {
#     "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
#     "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
# }

# Playwright requires an asyncio-compatible Twisted reactor to handle asynchronous tasks
# TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# PLAYWRIGHT_BROWSER_TYPE = "chromium"  # Choose 'chromium', 'firefox', or 'webkit'
# PLAYWRIGHT_LAUNCH_OPTIONS = {
#     "headless": True,  # Set to True if you prefer headless mode
# }

# PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 120000

# CLOSESPIDER_TIMEOUT = 360
# PLAYWRIGHT_CLOSE_PAGE = True
# PLAYWRIGHT_CLOSE_CONTEXT = True


# Logs
LOG_ENABLED = True
LOG_LEVEL = "INFO"
# LOG_FILE = 'scrapy.log'