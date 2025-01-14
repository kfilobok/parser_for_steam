import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider
from urllib.parse import urlencode
import sqlite3
import asyncio
from asyncio import LifoQueue


class My_Data:
    def __init__(self, title="", price="", rate="", developer="", genres="", release_date="", query=""):
        self.query = query
        self.title = title
        self.price = price
        self.rate = rate
        self.developer = developer
        self.genres = genres
        self.release_date = release_date


class SteamSpider(scrapy.Spider):
    name = "steam"

    SEARCH_QUERIES = ["strategy", "rpg", "action"]  # Запросы
    MAX_PAGES = 3

    BASE_URL = "https://store.steampowered.com/search/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.data = My_Data()
        self.conn = sqlite3.connect("results.db")
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS games (
                title TEXT UNIQUE,
                price TEXT,
                rating TEXT,
                developer TEXT,
                genres TEXT,
                release_date TEXT
            )
        ''')
        self.conn.commit()
        self.queue = LifoQueue()

    async def async_start_requests(self):
        for query in self.SEARCH_QUERIES:
            for page in range(1, self.MAX_PAGES + 1):
                params = {
                    'term': query,
                    'filter': 'popularnew',
                    'page': page
                }
                # url = f"{self.BASE_URL}?tags=popularnew&page={page}?cc=us?agecheck=1&age_day=1&age_month=January&age_year=1990"  # ?{urlencode(params)}
                # https://store.steampowered.com/search/?tags=popularnew&page=1
                url = f"{self.BASE_URL}?{urlencode(params)}&cc=us?"
                # print(url)
                await self.queue.put(url)

    async def parse_queue(self):
        while not self.queue.empty():
            url = await self.queue.get()
            yield scrapy.Request(url=url, callback=self.parse_search_results)

    def start_requests(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_start_requests())

        while not self.queue.empty():
            url = self.queue.get_nowait()
            yield scrapy.Request(url=url, callback=self.parse_search_results)

    async def parse_search_results(self, response):

        games = response.xpath('//div[@id="search_resultsRows"]/a')
        if not games:
            raise CloseSpider(f"No more games found for query {response.meta['query']}")

        for game in games:
            data = My_Data()
            data.title = game.xpath('.//span[@class="title"]/text()').get()

            price = game.xpath(".//div[contains(@class, 'discount_final_price')]/text()")
            # print(price)
            data.price = price.get().strip() if price else "Free"
            data.rate = \
                game.xpath('.//span[contains(@class, "search_review_summary")]/@data-tooltip-html').get().split("<br>")[
                    1][
                :3].rstrip()
            # print(reviews)
            data.release_date = (game.xpath(".//div[contains(@class, 'search_released')]/text()").get()).strip()
            game_url = game.xpath('./@href').get().split('?')[0]
            # print(game_url)
            mycookies = {
                'birthtime': '283993666',
                'mature_content': '1'
            }

            # cookies = {'agecheck': '1', 'age_day': '1', 'age_month': 'January', 'age_year': '1999'}
            # request = scrapy.Request(url=game_url, cookies=mycookies)
            # response = self.crawler.engine.download(request, )
            # await self.parse_game_details(response, data)

            # print(data.title)

            yield response.follow(url=game_url, cookies=mycookies, callback=self.parse_game_details,
                                  meta={"data": data, })

    async def parse_game_details(self, response):
        data = response.meta['data']
        # print(data.title)
        data.developer = response.xpath('//div[@id="developers_list"]/a/text()').get()
        # print(developer)
        data.genres = response.xpath('//div[@class="details_block"]//a[contains(@href, "genre")]/text()').getall()
        # release_date = response.xpath('//div[contains(@class, "release_date")]/div[@class="date"]/text()').get()
        await self.adding_to_db(data)

    async def adding_to_db(self, data):
        # print(data.title, )
        self.cursor.execute('''
            INSERT OR IGNORE INTO games (title, price, rating, developer, genres, release_date) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (data.title, data.price, data.rate, data.developer, ", ".join(data.genres),
              data.release_date))
        self.conn.commit()

    def closed(self, reason):
        self.conn.close()


if __name__ == "__main__":
    process = CrawlerProcess(settings={
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "DOWNLOAD_DELAY": 1
    })
    process.crawl(SteamSpider)
    process.start()
