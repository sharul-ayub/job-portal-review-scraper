import asyncio
import json
import math
import random
import re
import shutil
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
    UndetectedAdapter,
)
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy


def _clean_text(value: str) -> str:
    return " ".join((value or "").split())


def _get_nested(dct: dict, *keys):
    cur = dct
    for k in keys:
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(k)
    return cur if cur is not None else ""


def _employee_flag(review: dict) -> str:
    val = review.get("currentEmployee")
    if val is True:
        return "current"
    if val is False:
        return "former"
    return ""


def _extract_reviews_from_html(html: str) -> list[dict]:
    script_match = re.search(
        r'<script[^>]*id=["\']comp-initialData["\'][^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not script_match:
        raise RuntimeError("Could not find comp-initialData JSON in live HTML.")

    payload = script_match.group(1).strip()
    data = json.loads(payload)
    reviews = data.get("reviewsList", {}).get("items", [])

    rows = []
    for r in reviews:
        rows.append(
            {
                "review_date": _clean_text(r.get("submissionDate", "")),
                "review_title": _clean_text(_get_nested(r, "title", "text")),
                "review_body_text": _clean_text(_get_nested(r, "text", "text")),
                "overall_review_rating": r.get("overallRating", ""),
                "job_title": _clean_text(r.get("jobTitle", "")),
                "location": _clean_text(r.get("location", "")),
                "employee_flag": _employee_flag(r),
                "rating_work_life_balance": _get_nested(r, "workAndLifeBalanceRating", "rating"),
                "rating_compensation_benefits": _get_nested(r, "compensationAndBenefitsRating", "rating"),
                "rating_job_security_advancement": _get_nested(r, "jobSecurityAndAdvancementRating", "rating"),
                "rating_management": _get_nested(r, "managementRating", "rating"),
                "rating_culture_values": _get_nested(r, "cultureAndValuesRating", "rating"),
                "pros_text": _clean_text(_get_nested(r, "pros", "text")),
                "cons_text": _clean_text(_get_nested(r, "cons", "text")),
            }
        )

    return rows


def build_page_url(base_url: str, start: int) -> str:
    if start == 0:
        return base_url
    return f"{base_url}?start={start}"


def page_index_from_url(url: str, page_size: int) -> int:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    start = int(qs.get("start", ["0"])[0])
    return (start // page_size) + 1


def _load_checkpoint(path: Path, base_url: str, total_pages: int, page_size: int) -> dict:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("base_url") == base_url:
                return data
        except Exception:
            pass

    return {
        "base_url": base_url,
        "total_pages": total_pages,
        "page_size": page_size,
        "pages": {},
    }


def _save_checkpoint(path: Path, checkpoint: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(checkpoint, indent=2, ensure_ascii=False), encoding="utf-8")


def _prepare_worker_profile(base_profile: Path, worker_profile: Path) -> None:
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


async def _crawl_once(url: str, browser_config: BrowserConfig, run_config: CrawlerRunConfig):
    async with AsyncWebCrawler(config=browser_config, verbose=True) as crawler:
        return await crawler.arun(url=url, config=run_config)


async def _crawl_once_undetected(
    url: str, browser_config: BrowserConfig, run_config: CrawlerRunConfig
):
    strategy = AsyncPlaywrightCrawlerStrategy(
        browser_config=browser_config,
        browser_adapter=UndetectedAdapter(),
    )
    async with AsyncWebCrawler(
        crawler_strategy=strategy,
        config=browser_config,
        verbose=True,
    ) as crawler:
        return await crawler.arun(url=url, config=run_config)


async def parallel_scrape_reviews() -> None:
    # Must match setup_profile.py profile name.
    base_profile_path = Path.home() / ".crawl4ai" / "profiles" / "job_portal_profile_indeed"
    if not base_profile_path.exists():
        raise FileNotFoundError(
            f"Profile not found: {base_profile_path}. Run setup_profile.py first."
        )

    worker_profiles_root = Path("data/raw/worker_profiles")
    worker_profiles_root.mkdir(parents=True, exist_ok=True)

    base_url = "https://malaysia.indeed.com/cmp/Malayan-Banking-Berhad-(maybank)/reviews"
    total_reviews = 309
    page_size = 30
    total_pages = math.ceil(total_reviews / page_size)

    output_folder = Path("data/raw/pages")
    debug_folder = Path("data/raw/debug")
    checkpoint_path = Path("data/raw/checkpoint.json")
    output_folder.mkdir(parents=True, exist_ok=True)
    debug_folder.mkdir(parents=True, exist_ok=True)

    checkpoint = _load_checkpoint(checkpoint_path, base_url, total_pages, page_size)

    pending_urls = []
    for page_num in range(total_pages):
        page_idx = page_num + 1
        page_key = str(page_idx)
        if checkpoint.get("pages", {}).get(page_key, {}).get("status") == "success":
            continue
        start = page_num * page_size
        pending_urls.append(build_page_url(base_url, start))

    if not pending_urls:
        print("No pending pages. All pages already marked success in checkpoint.")
        return

    print(f"Pending pages to scrape: {len(pending_urls)}")

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_for="css:body",
        wait_until="load",
        delay_before_return_html=20,
        page_timeout=60000,
        magic=True,
        simulate_user=True,
    )

    max_parallel_browsers = 3
    success_pages = 0
    failed_pages = 0
    sem = asyncio.Semaphore(max_parallel_browsers)
    checkpoint_lock = asyncio.Lock()
    failed_after_primary = []

    primary_config = BrowserConfig(
        headless=False,
        enable_stealth=True,
        use_managed_browser=True,
        use_persistent_context=True,
        user_data_dir=str(base_profile_path.resolve()),
        browser_type="chromium",
    )

    def _save_success(page_idx: int, start_offset: int, url: str, rows: list[dict], attempts: int, mode_used: str) -> None:
        output_path = output_folder / f"reviews_page_{page_idx}.json"
        output_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
        checkpoint["pages"][str(page_idx)] = {
            "status": "success",
            "attempts": attempts,
            "start_offset": start_offset,
            "source_url": url,
            "output_file": str(output_path),
            "review_count": len(rows),
            "mode_used": mode_used,
            "last_error": "",
        }
        _save_checkpoint(checkpoint_path, checkpoint)

    def _save_failed(page_idx: int, start_offset: int, url: str, attempts: int, mode_used: str, last_error: str) -> None:
        checkpoint["pages"][str(page_idx)] = {
            "status": "failed",
            "attempts": attempts,
            "start_offset": start_offset,
            "source_url": url,
            "output_file": "",
            "review_count": 0,
            "mode_used": mode_used,
            "last_error": last_error,
        }
        _save_checkpoint(checkpoint_path, checkpoint)

    # Stage 1: sequential primary mode using shared managed profile.
    for url in pending_urls:
        page_idx = page_index_from_url(url, page_size)
        page_key = str(page_idx)
        start_offset = (page_idx - 1) * page_size
        prev = checkpoint.get("pages", {}).get(page_key, {})
        attempts = int(prev.get("attempts", 0)) + 1
        html = ""
        last_error = ""

        print(f"PRIMARY page {page_idx}: {url}")
        try:
            result = await _crawl_once(url, primary_config, run_config)
            if not result.success:
                last_error = f"primary crawl failed: {result.error_message}"
            else:
                html = getattr(result, "html", "") or ""
                if not html:
                    last_error = "primary returned empty HTML"
        except Exception as exc:
            last_error = f"primary crawler exception: {exc}"

        if html:
            try:
                rows = _extract_reviews_from_html(html)
                for row in rows:
                    row["page_number"] = page_idx
                    row["start_offset"] = start_offset
                    row["source_url"] = url
                _save_success(page_idx, start_offset, url, rows, attempts, "managed_profile")
                success_pages += 1
                print(f"SUCCESS page {page_idx}: saved {len(rows)} reviews (managed_profile)")
                continue
            except Exception as exc:
                last_error = f"primary parse failed: {exc}"
                failed_html_path = debug_folder / f"failed_page_{page_idx}.html"
                failed_html_path.write_text(html, encoding="utf-8")

        failed_after_primary.append(
            {"url": url, "page_idx": page_idx, "start_offset": start_offset, "attempts": attempts, "last_error": last_error}
        )
        _save_failed(page_idx, start_offset, url, attempts, "managed_profile", last_error or "primary failed")
        print(f"PRIMARY FAILED page {page_idx}: {last_error or 'primary failed'}")

    if not failed_after_primary:
        print("No fallback needed. All pending pages succeeded in primary mode.")
        print(f"Completed parallel run. Success pages: {success_pages}, Failed pages: {failed_pages}")
        print(f"Checkpoint updated: {checkpoint_path}")
        return

    print(f"Fallback pages to scrape in parallel (undetected): {len(failed_after_primary)}")

    # Stage 2: parallel fallback mode for only failed pages.
    async def process_fallback(item: dict) -> None:
        nonlocal success_pages, failed_pages
        url = item["url"]
        page_idx = int(item["page_idx"])
        start_offset = int(item["start_offset"])
        attempts = int(item["attempts"]) + 1
        last_error = item.get("last_error", "")

        async with sem:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            worker_profile = worker_profiles_root / f"page_{page_idx}"

            try:
                _prepare_worker_profile(base_profile_path, worker_profile)
                fallback_config = BrowserConfig(
                    headless=False,
                    enable_stealth=True,
                    use_managed_browser=False,
                    use_persistent_context=True,
                    user_data_dir=str(worker_profile.resolve()),
                    browser_type="chromium",
                )
                result = await _crawl_once_undetected(url, fallback_config, run_config)
                if not result.success:
                    last_error = f"fallback crawl failed: {result.error_message}"
                    raise RuntimeError(last_error)
                html = getattr(result, "html", "") or ""
                if not html:
                    last_error = "fallback returned empty HTML"
                    raise RuntimeError(last_error)

                rows = _extract_reviews_from_html(html)
                for row in rows:
                    row["page_number"] = page_idx
                    row["start_offset"] = start_offset
                    row["source_url"] = url

                async with checkpoint_lock:
                    _save_success(page_idx, start_offset, url, rows, attempts, "fallback_unmanaged_undetected")
                    success_pages += 1
                print(f"SUCCESS page {page_idx}: saved {len(rows)} reviews (fallback_unmanaged_undetected)")
                return
            except Exception as exc:
                if not last_error:
                    last_error = f"fallback crawler exception: {exc}"
                failed_html_path = debug_folder / f"failed_page_{page_idx}.html"
                if "html" in locals() and html:
                    failed_html_path.write_text(html, encoding="utf-8")
                async with checkpoint_lock:
                    _save_failed(page_idx, start_offset, url, attempts, "fallback_unmanaged_undetected", last_error)
                    failed_pages += 1
                print(f"FAILED page {page_idx}: {last_error}")

    await asyncio.gather(*(process_fallback(item) for item in failed_after_primary))

    print(f"Completed parallel run. Success pages: {success_pages}, Failed pages: {failed_pages}")
    print(f"Checkpoint updated: {checkpoint_path}")


if __name__ == "__main__":
    asyncio.run(parallel_scrape_reviews())
