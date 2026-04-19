import shutil
from pathlib import Path

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    UndetectedAdapter,
)
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy


def build_primary_browser_config(profile_path: str) -> BrowserConfig:
    # Primary uses login profile clone, but unmanaged so many browser instances can run in parallel.
    return BrowserConfig(
        headless=False,
        enable_stealth=True,
        use_managed_browser=False,
        use_persistent_context=True,
        user_data_dir=profile_path,
        browser_type="chromium",
    )


def build_fallback_browser_config(profile_path: str) -> BrowserConfig:
    # Fallback runs unmanaged + persistent profile clone; crawl path uses undetected strategy.
    return BrowserConfig(
        headless=False,
        enable_stealth=True,
        use_managed_browser=False,
        use_persistent_context=True,
        user_data_dir=profile_path,
        browser_type="chromium",
    )


def build_crawl_config() -> CrawlerRunConfig:
    return CrawlerRunConfig(
        wait_for="css:body",
        wait_until="load",
        delay_before_return_html=20,
        page_timeout=60000,
        magic=False,
        simulate_user=False,
    )


async def crawl_once(url: str, browser_config: BrowserConfig, crawl_config: CrawlerRunConfig):
    async with AsyncWebCrawler(config=browser_config, verbose=True) as crawler:
        return await crawler.arun(url=url, config=crawl_config)


async def crawl_once_undetected(url: str, browser_config: BrowserConfig, crawl_config: CrawlerRunConfig):
    strategy = AsyncPlaywrightCrawlerStrategy(
        browser_config=browser_config,
        browser_adapter=UndetectedAdapter(),
    )
    async with AsyncWebCrawler(
        crawler_strategy=strategy,
        config=browser_config,
        verbose=True,
    ) as crawler:
        return await crawler.arun(url=url, config=crawl_config)


def ensure_worker_profile(base_profile: Path, worker_profile: Path) -> None:
    if worker_profile.exists():
        return
    if not base_profile.exists():
        raise FileNotFoundError(f"Base profile not found: {base_profile}")
    worker_profile.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(
        base_profile,
        worker_profile,
        ignore=shutil.ignore_patterns("SingletonLock", "SingletonSocket", "SingletonCookie"),
    )
