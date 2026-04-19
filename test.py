import asyncio
import json
import math
import re
from pathlib import Path

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    UndetectedAdapter,
)
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy

PAGE_STABILIZE_DELAY_SECONDS = 15


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


def _load_checkpoint(path: Path, base_url: str, total_pages: int, page_size: int) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
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


async def scrape_reviews():
    profile_path = Path.home() / ".crawl4ai" / "profiles" / "job_portal_profile_indeed"

    # Updated browser config
    browser_config = BrowserConfig(
        headless=False,
        enable_stealth=True,
        use_managed_browser=False,
        user_data_dir=str(profile_path),
        browser_type="chromium",
    )

    # Updated crawl config
    crawl_config = CrawlerRunConfig(
        wait_for="css:body",
        wait_until="load",
        delay_before_return_html=8,
        page_timeout=60000,
        magic=True,
        simulate_user=True,
    )

    base_url = "https://malaysia.indeed.com/cmp/Public-Bank-Berhad/reviews"
    total_reviews = 252
    page_size = 20
    total_pages = math.ceil(total_reviews / page_size)

    output_folder = Path("data/raw/pages")
    debug_folder = Path("data/raw/debug")
    checkpoint_path = Path("data/raw/checkpoint.json")
    output_folder.mkdir(parents=True, exist_ok=True)
    debug_folder.mkdir(parents=True, exist_ok=True)

    checkpoint = _load_checkpoint(
        checkpoint_path,
        base_url=base_url,
        total_pages=total_pages,
        page_size=page_size,
    )
    _save_checkpoint(checkpoint_path, checkpoint)

    success_pages = 0
    failed_pages = 0
    skipped_pages = 0

    for page_num in range(total_pages):
        page_idx = page_num + 1
        page_key = str(page_idx)
        start = page_num * page_size
        url = build_page_url(base_url, start)

        prev = checkpoint.get("pages", {}).get(page_key, {})
        if prev.get("status") == "success":
            skipped_pages += 1
            print(f"Skipping page {page_idx}/{total_pages} (already success in checkpoint)")
            continue

        print(f"Scraping page {page_idx}/{total_pages}: {url}")
        rows = []
        html = ""
        last_error = ""
        attempts = int(prev.get("attempts", 0))

        for attempt in range(1, 3):
            attempts += 1

            # Create a fresh strategy per attempt to avoid stale internal browser state
            strategy = AsyncPlaywrightCrawlerStrategy(
                browser_config=browser_config,
                browser_adapter=UndetectedAdapter(),
            )

            async with AsyncWebCrawler(
                crawler_strategy=strategy,
                config=browser_config,
                verbose=True,
            ) as crawler:
                result = await crawler.arun(url=url, config=crawl_config)

            await asyncio.sleep(PAGE_STABILIZE_DELAY_SECONDS)

            if not result.success:
                last_error = f"crawl failed: {result.error_message}"
                print(f"Page {page_idx}: crawl failed on attempt {attempt}: {result.error_message}")
                continue

            html = getattr(result, "html", "") or ""
            if not html:
                last_error = "empty HTML"
                print(f"Page {page_idx}: empty HTML on attempt {attempt}")
                continue

            try:
                rows = _extract_reviews_from_html(html)
                break
            except Exception as exc:
                last_error = f"parse failed: {exc}"
                print(f"Page {page_idx}: parse failed on attempt {attempt}: {exc}")

        if not rows:
            failed_pages += 1
            failed_html_path = debug_folder / f"failed_page_{page_idx}.html"
            if html:
                failed_html_path.write_text(html, encoding="utf-8")
                print(f"Saved failed HTML to {failed_html_path}")
            else:
                print(f"Page {page_idx}: no HTML captured to save")

            checkpoint["pages"][page_key] = {
                "status": "failed",
                "attempts": attempts,
                "start_offset": start,
                "source_url": url,
                "output_file": "",
                "review_count": 0,
                "last_error": last_error or "unknown error",
            }
            _save_checkpoint(checkpoint_path, checkpoint)
            continue

        for row in rows:
            row["page_number"] = page_idx
            row["start_offset"] = start
            row["source_url"] = url

        output_path = output_folder / f"reviews_page_{page_idx}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, ensure_ascii=False)

        print(f"Saved {len(rows)} reviews to {output_path}")
        success_pages += 1

        checkpoint["pages"][page_key] = {
            "status": "success",
            "attempts": attempts,
            "start_offset": start,
            "source_url": url,
            "output_file": str(output_path),
            "review_count": len(rows),
            "last_error": "",
        }
        _save_checkpoint(checkpoint_path, checkpoint)

    print(
        f"Completed. Success pages: {success_pages}, "
        f"Failed pages: {failed_pages}, Skipped pages: {skipped_pages}"
    )
    print(f"Checkpoint updated: {checkpoint_path}")


if __name__ == "__main__":
    asyncio.run(scrape_reviews())
