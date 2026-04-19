import asyncio
import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .checkpoint_store import load_checkpoint, save_checkpoint
from .crawler import (
    build_crawl_config,
    build_fallback_browser_config,
    build_primary_browser_config,
    crawl_once,
    crawl_once_undetected,
    ensure_worker_profile,
)
from .helpers import build_page_url


@dataclass(frozen=True)
class EngineConfig:
    base_url: str
    total_reviews: int
    page_size: int
    profile_path: Path
    worker_profiles_root: Path
    output_folder: Path
    debug_folder: Path
    checkpoint_path: Path
    extract_rows: Callable[[str], list[dict]]
    max_parallel: int = 3
    idle_delay_range_seconds: tuple[float, float] = (5.0, 12.0)
    page_settle_delay_seconds: float = 4.0
    output_file_pattern: str = "reviews_page_{page_idx}.json"


async def run_engine(config: EngineConfig) -> None:
    total_pages = math.ceil(config.total_reviews / config.page_size)

    config.output_folder.mkdir(parents=True, exist_ok=True)
    config.debug_folder.mkdir(parents=True, exist_ok=True)
    config.worker_profiles_root.mkdir(parents=True, exist_ok=True)

    checkpoint = load_checkpoint(
        config.checkpoint_path,
        base_url=config.base_url,
        total_pages=total_pages,
        page_size=config.page_size,
    )
    if checkpoint.get("base_url") != config.base_url:
        checkpoint = {
            "base_url": config.base_url,
            "total_pages": total_pages,
            "page_size": config.page_size,
            "pages": {},
        }
    save_checkpoint(config.checkpoint_path, checkpoint)

    if not config.profile_path.exists():
        raise FileNotFoundError(
            f"Profile not found: {config.profile_path}. Run setup_profile.py first."
        )

    crawl_config = build_crawl_config(delay_before_return_html=config.page_settle_delay_seconds)

    success_pages = 0
    failed_pages = 0
    skipped_pages = 0

    pending = []
    for page_num in range(total_pages):
        page_idx = page_num + 1
        page_key = str(page_idx)
        start = page_num * config.page_size
        url = build_page_url(config.base_url, start)
        prev = checkpoint.get("pages", {}).get(page_key, {})
        if prev.get("status") == "success":
            skipped_pages += 1
            continue
        pending.append(
            {
                "page_idx": page_idx,
                "page_key": page_key,
                "start": start,
                "url": url,
                "attempts": int(prev.get("attempts", 0)),
            }
        )

    if not pending:
        print("No pending pages. All pages already success in checkpoint.")
        print(
            f"Completed. Success pages: {success_pages}, "
            f"Failed pages: {failed_pages}, Skipped pages: {skipped_pages}"
        )
        print(f"Checkpoint updated: {config.checkpoint_path}")
        return

    print(f"Pending pages for primary parallel pass: {len(pending)}")

    sem = asyncio.Semaphore(config.max_parallel)
    checkpoint_lock = asyncio.Lock()
    fallback_queue = []

    async def idle_pause() -> None:
        low, high = config.idle_delay_range_seconds
        await asyncio.sleep(random.uniform(low, high))

    async def mark_success(item: dict, rows: list[dict], attempts: int, mode: str) -> None:
        nonlocal success_pages
        page_idx = item["page_idx"]
        page_key = item["page_key"]
        start = item["start"]
        url = item["url"]

        for row in rows:
            row["page_number"] = page_idx
            row["start_offset"] = start
            row["source_url"] = url

        output_filename = config.output_file_pattern.format(page_idx=page_idx)
        output_path = config.output_folder / output_filename
        output_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")

        async with checkpoint_lock:
            success_pages += 1
            checkpoint["pages"][page_key] = {
                "status": "success",
                "attempts": attempts,
                "start_offset": start,
                "source_url": url,
                "output_file": str(output_path),
                "review_count": len(rows),
                "mode": mode,
                "last_error": "",
            }
            save_checkpoint(config.checkpoint_path, checkpoint)
        print(f"SUCCESS page {page_idx}: saved {len(rows)} reviews ({mode})")

    async def primary_worker(item: dict) -> None:
        page_idx = item["page_idx"]
        attempts = item["attempts"] + 1
        last_error = ""
        html = ""

        worker_profile = config.worker_profiles_root / f"primary_page_{page_idx}"

        async with sem:
            print(f"Primary scraping page {page_idx}: {item['url']}")
            try:
                ensure_worker_profile(config.profile_path, worker_profile)
                primary_browser_config = build_primary_browser_config(str(worker_profile.resolve()))
                result = await crawl_once(item["url"], primary_browser_config, crawl_config)
            except Exception as exc:
                result = None
                last_error = f"primary crawler exception: {exc}"
            await idle_pause()

        if result is not None and result.success:
            html = getattr(result, "html", "") or ""
            if html:
                try:
                    rows = config.extract_rows(html)
                    await mark_success(item, rows, attempts, "primary_login_parallel")
                    return
                except Exception as exc:
                    last_error = f"primary parse failed: {exc}"
            else:
                last_error = "primary returned empty HTML"
        elif result is not None:
            last_error = f"primary crawl failed: {result.error_message}"

        async with checkpoint_lock:
            fallback_queue.append(
                {
                    "item": item,
                    "attempts": attempts,
                    "last_error": last_error,
                    "html": html,
                }
            )
        print(f"PRIMARY FAILED page {page_idx}: {last_error or 'unknown error'}")

    await asyncio.gather(*(primary_worker(item) for item in pending))

    if not fallback_queue:
        print("No fallback needed. All pending pages succeeded in primary parallel pass.")
        print(
            f"Completed. Success pages: {success_pages}, "
            f"Failed pages: {failed_pages}, Skipped pages: {skipped_pages}"
        )
        print(f"Checkpoint updated: {config.checkpoint_path}")
        return

    print(f"Pending pages for fallback parallel pass: {len(fallback_queue)}")

    async def fallback_worker(payload: dict) -> None:
        nonlocal failed_pages

        item = payload["item"]
        page_idx = item["page_idx"]
        page_key = item["page_key"]
        start = item["start"]
        url = item["url"]
        attempts = payload["attempts"] + 1
        last_error = payload.get("last_error", "")
        html = payload.get("html", "")

        worker_profile = config.worker_profiles_root / f"fallback_page_{page_idx}"

        async with sem:
            print(f"Fallback scraping page {page_idx}: {url}")
            try:
                ensure_worker_profile(config.profile_path, worker_profile)
                fallback_browser_config = build_fallback_browser_config(str(worker_profile.resolve()))
                result = await crawl_once_undetected(url, fallback_browser_config, crawl_config)
            except Exception as exc:
                result = None
                last_error = f"fallback crawler exception: {exc}"
            await idle_pause()

        if result is not None and result.success:
            html = getattr(result, "html", "") or ""
            if html:
                try:
                    rows = config.extract_rows(html)
                    await mark_success(item, rows, attempts, "fallback_undetected_parallel")
                    return
                except Exception as exc:
                    last_error = f"fallback parse failed: {exc}"
            else:
                last_error = "fallback returned empty HTML"
        elif result is not None:
            last_error = f"fallback crawl failed: {result.error_message}"

        failed_html_path = config.debug_folder / f"failed_page_{page_idx}.html"
        if html:
            failed_html_path.write_text(html, encoding="utf-8")

        async with checkpoint_lock:
            failed_pages += 1
            checkpoint["pages"][page_key] = {
                "status": "failed",
                "attempts": attempts,
                "start_offset": start,
                "source_url": url,
                "output_file": "",
                "review_count": 0,
                "mode": "fallback_undetected_parallel",
                "last_error": last_error or "unknown error",
            }
            save_checkpoint(config.checkpoint_path, checkpoint)
        print(f"FAILED page {page_idx}: {last_error or 'unknown error'}")

    await asyncio.gather(*(fallback_worker(payload) for payload in fallback_queue))

    print(
        f"Completed. Success pages: {success_pages}, "
        f"Failed pages: {failed_pages}, Skipped pages: {skipped_pages}"
    )
    print(f"Checkpoint updated: {config.checkpoint_path}")
