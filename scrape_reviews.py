import asyncio
from pathlib import Path

from scrape_test import EngineConfig, run_engine
from scrape_test.crawler import (
    build_crawl_config,
    build_fallback_browser_config,
    build_primary_browser_config,
    crawl_once,
    crawl_once_undetected,
    ensure_worker_profile,
)
from scrape_test.parser_indeed import extract_review_count_from_html, extract_reviews_from_html

# Target-specific config lives here (not in scrape engine)
BASE_URL = "https://malaysia.indeed.com/cmp/United-Overseas-Bank/reviews"
PAGE_SIZE = 20

PROFILE_PATH = Path.home() / ".crawl4ai" / "profiles" / "job_portal_profile_indeed"
WORKER_PROFILES_ROOT = Path("data/raw/worker_profiles")
OUTPUT_FOLDER = Path("data/raw/pages")
DEBUG_FOLDER = Path("data/raw/debug")
CHECKPOINT_PATH = Path("data/raw/checkpoint.json")

IDLE_DELAY_RANGE_SECONDS = (5.0, 12.0)
PAGE_SETTLE_DELAY_SECONDS = 4.0
MAX_PARALLEL = 3


async def discover_total_reviews() -> int:
    probe_profile = WORKER_PROFILES_ROOT / "probe_review_count"
    ensure_worker_profile(PROFILE_PATH, probe_profile)

    crawl_config = build_crawl_config(delay_before_return_html=PAGE_SETTLE_DELAY_SECONDS)

    primary_error = ""
    try:
        primary_browser = build_primary_browser_config(str(probe_profile.resolve()))
        primary_result = await crawl_once(BASE_URL, primary_browser, crawl_config)
        if primary_result.success:
            html = getattr(primary_result, "html", "") or ""
            if html:
                try:
                    return extract_review_count_from_html(html)
                except Exception:
                    pass
        else:
            primary_error = primary_result.error_message or "primary crawl failed"
    except Exception as exc:
        primary_error = str(exc)

    fallback_browser = build_fallback_browser_config(str(probe_profile.resolve()))
    fallback_result = await crawl_once_undetected(BASE_URL, fallback_browser, crawl_config)
    if not fallback_result.success:
        raise RuntimeError(
            "Unable to discover total review count from base URL. "
            f"Primary error: {primary_error or 'unknown'}; "
            f"Fallback error: {fallback_result.error_message}"
        )

    fallback_html = getattr(fallback_result, "html", "") or ""
    if not fallback_html:
        raise RuntimeError("Fallback crawl succeeded but returned empty HTML while detecting review count.")

    return extract_review_count_from_html(fallback_html)


async def scrape_reviews() -> None:
    total_reviews = await discover_total_reviews()
    print(f"Detected total reviews: {total_reviews}")

    cfg = EngineConfig(
        base_url=BASE_URL,
        total_reviews=total_reviews,
        page_size=PAGE_SIZE,
        profile_path=PROFILE_PATH,
        worker_profiles_root=WORKER_PROFILES_ROOT,
        output_folder=OUTPUT_FOLDER,
        debug_folder=DEBUG_FOLDER,
        checkpoint_path=CHECKPOINT_PATH,
        extract_rows=extract_reviews_from_html,
        max_parallel=MAX_PARALLEL,
        idle_delay_range_seconds=IDLE_DELAY_RANGE_SECONDS,
        page_settle_delay_seconds=PAGE_SETTLE_DELAY_SECONDS,
        output_file_pattern="reviews_page_{page_idx}.json",
    )
    await run_engine(cfg)


if __name__ == "__main__":
    asyncio.run(scrape_reviews())
