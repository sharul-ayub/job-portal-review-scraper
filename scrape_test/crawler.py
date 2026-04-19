import random
import shutil
from pathlib import Path

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    UndetectedAdapter,
)
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy


HEADER_PROFILES = [
    {
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",
        },
        "viewport": (1366, 768),
    },
    {
        "user_agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/16.4 Safari/605.1.15"
        ),
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://www.bing.com/",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",
        },
        "viewport": (1440, 900),
    },
]


def _pick_header_profile() -> dict:
    return random.choice(HEADER_PROFILES)


def build_primary_browser_config(profile_path: str) -> BrowserConfig:
    # Primary uses login profile clone, but unmanaged so many browser instances can run in parallel.
    profile = _pick_header_profile()
    viewport_width, viewport_height = profile["viewport"]
    return BrowserConfig(
        headless=False,
        enable_stealth=True,
        use_managed_browser=False,
        use_persistent_context=True,
        user_data_dir=profile_path,
        browser_type="chromium",
        user_agent=profile["user_agent"],
        headers=profile["headers"],
        viewport_width=viewport_width,
        viewport_height=viewport_height,
    )


def build_fallback_browser_config(profile_path: str) -> BrowserConfig:
    # Fallback runs unmanaged + persistent profile clone; crawl path uses undetected strategy.
    profile = _pick_header_profile()
    viewport_width, viewport_height = profile["viewport"]
    return BrowserConfig(
        headless=False,
        enable_stealth=True,
        use_managed_browser=False,
        use_persistent_context=True,
        user_data_dir=profile_path,
        browser_type="chromium",
        user_agent=profile["user_agent"],
        headers=profile["headers"],
        viewport_width=viewport_width,
        viewport_height=viewport_height,
    )


def build_crawl_config(
    c4a_script: str | None = None,
    delay_before_return_html: float = 4.0,
) -> CrawlerRunConfig:
    config_kwargs = {
        "wait_for": "css:body",
        "wait_until": "load",
        "delay_before_return_html": delay_before_return_html,
        "page_timeout": 60000,
        "magic": True,
        "simulate_user": True,
        "scan_full_page": True,
        "scroll_delay": 0.8,
    }
    if c4a_script:
        # Optional scripted interactions for pages that require typing/clicking behavior.
        config_kwargs["c4a_script"] = c4a_script
    return CrawlerRunConfig(**config_kwargs)


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
