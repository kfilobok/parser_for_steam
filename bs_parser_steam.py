import aiohttp
import asyncio
import aiosqlite
from bs4 import BeautifulSoup

# Константы
SEARCH_QUERIES = ["strategy", "rpg"]  # Запросы
MAX_PAGES = 3  # Макс кол-во страниц
BASE_URL = "https://store.steampowered.com/search/"
DELAY = 1
CONST = 0
#   id INTEGER PRIMARY KEY AUTOINCREMENT,

CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS games (
    title TEXT UNIQUE PRIMARY KEY,
    price TEXT,
    rating TEXT,
    developer TEXT,
    genres TEXT,
    release_date TEXT
);
"""

INSERT_QUERY = """
INSERT OR IGNORE  INTO games (title, price, rating, developer, genres, release_date)
VALUES (?, ?, ?, ?, ?, ?);
"""
# <br>

class MyGame:
    def __init__(self, title="", price="", rating="", developer="", genres="", release_date="", query=""):
        self.query = query
        self.title = title
        self.price = price
        self.rating = rating
        self.developer = developer
        self.genres = genres
        self.release_date = release_date

    async def add_to_db(self,):
        async with aiosqlite.connect("results.db") as db:
            # await db.execute(f"""
            #     INSERT OR IGNORE  INTO games (title, price, rating, developer, genres, release_date)
            #     VALUES ({self.title}, {self.price}, {self.rating}, {self.developer}, {self.genres}, {self.release_date});
            #                 """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    title TEXT UNIQUE PRIMARY KEY,
                    price TEXT,
                    rating TEXT,
                    developer TEXT,
                    genres TEXT,
                    release_date TEXT
                            );
                            """)


            await db.execute('''
                        INSERT OR IGNORE INTO games (title, price, rating, developer, genres, release_date) 
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (self.title, self.price, self.rating, self.developer, self.genres,
                          self.release_date))
            await db.commit()




def construct_url(query, page):
    params = {
        "term": query,
        "page": page,
        "filter": "popularnew",
        "cc": "ru"
    }
    query_string = "&".join(f"{key}={value}" for key, value in params.items())
    return f"{BASE_URL}?{query_string}"

async def fetch_page(session, url):
    mycookies = {
        'birthtime': '283993000',
        'mature_content': '1'
    }
    async with session.get(url, cookies=mycookies) as response:
        if response.status == 200:
            return await response.text()
        else:
            print(f"Failed to fetch {url}: {response.status}")
            return None

async def parse_game_details(session, game_url):
    html = await fetch_page(session, game_url)

    if not html:
        return "", "", ""

    soup = BeautifulSoup(html, "lxml")
    developer = soup.select_one("div.dev_row a")
    developer = developer.get_text(strip=True) if developer else ""

    genres = soup.select(".details_block a[href*='genre']")
    genres = ";".join([genre.get_text(strip=True) for genre in genres]) if genres else ""

    release_date = soup.select_one("div.date")
    release_date = release_date.get_text(strip=True) if release_date else ""

    return developer, genres, release_date

async def parse_page(session, html,):
    data= MyGame()
    soup = BeautifulSoup(html, "lxml")
    results = []
    # CONST =0
    for game in soup.select(".search_result_row"):
        data.title = game.select_one(".title").get_text(strip=True)
        data.price = game.select_one(".discount_final_price").get_text(strip=True) if game.select_one(".discount_final_price") else "Free"
        data.rating = (game.select_one(".search_review_summary").get("data-tooltip-html", "") ).split("<br>")[1][:3]
        # print(data.rating)
        # data.rating_text = data.rating["data-tooltip-html"] if data.rating else ""

        game_url = game["href"]


        data.developer, data.genres, data.release_date = await parse_game_details(session, game_url)
        #print(developer)

        results.append((data))
        await data.add_to_db()

    return results

# async def save_to_db(db_path, games):
#     async with aiosqlite.connect(db_path) as db:
#         await db.execute(CREATE_TABLE_QUERY)
#         await db.executemany(INSERT_QUERY, games)
#         await db.commit()

async def scrape_query(query, max_pages):
    games = []
    async with aiohttp.ClientSession() as session:
        for page in range(1, max_pages + 1):
            url = construct_url(query, page)
            print(url)
            html = await fetch_page(session, url)
            if html:
                page_results = await parse_page(session, html)
                if not page_results:
                    break
                games.extend(page_results)
            await asyncio.sleep(DELAY)
    return games

async def main():
    # db_path = "results.db"
    all_games = []
    for query in SEARCH_QUERIES:
        games = await scrape_query(query, MAX_PAGES)
        all_games.extend(games)

    # await save_to_db(db_path, all_games)


if __name__ == "__main__":
    asyncio.run(main())
