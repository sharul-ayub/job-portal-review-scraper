import asyncio
import subprocess
import sys
from pathlib import Path

from crawl4ai import BrowserProfiler
from crawl4ai.utils import get_chromium_path, get_home_folder


def _clear_stale_browser_cache(browser_type: str = "chromium") -> None:
    cache_file = Path(get_home_folder()) / f"{browser_type}.path"
    if not cache_file.exists():
        return

    try:
        cached_path = cache_file.read_text(encoding="utf-8").strip()
    except OSError:
        cached_path = ""

    if not cached_path or not Path(cached_path).exists():
        cache_file.unlink(missing_ok=True)
        print(f"[PROFILE]. INFO Removed stale browser cache: {cache_file}")


async def _ensure_chromium_available() -> str:
    browser_type = "chromium"
    _clear_stale_browser_cache(browser_type)

    browser_path = await get_chromium_path(browser_type)
    if Path(browser_path).exists():
        return browser_path

    print("[PROFILE]. INFO Chromium executable is missing. Installing Playwright browser...")
    subprocess.run(
        [sys.executable, "-m", "playwright", "install", browser_type],
        check=True,
    )

    _clear_stale_browser_cache(browser_type)
    browser_path = await get_chromium_path(browser_type)
    if not Path(browser_path).exists():
        raise RuntimeError(
            "Chromium was not found after installation at: "
            f"{browser_path}"
        )
    return browser_path


async def setup_profile():
    try:
        browser_path = await _ensure_chromium_available()
        print(f"[PROFILE]. INFO Using Chromium executable: {browser_path}")
    except subprocess.CalledProcessError as exc:
        print("[PROFILE]. INFO Failed to install Playwright Chromium automatically.")
        print(
            "[PROFILE]. INFO Run this manually:\n"
            f"{sys.executable} -m playwright install chromium"
        )
        raise RuntimeError("Cannot continue without Chromium.") from exc

    profiler = BrowserProfiler()
    profile_path = await profiler.create_profile(profile_name="job_portal_profile_indeed")
    print(f"Profile saved at: {profile_path}")


if __name__ == "__main__":
    asyncio.run(setup_profile())
