import asyncio

from scrape_test.pipeline import scrape_reviews


if __name__ == "__main__":
    asyncio.run(scrape_reviews())
