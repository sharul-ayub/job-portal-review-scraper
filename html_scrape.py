import asyncio
from pathlib import Path

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

URL = "https://malaysia.indeed.com/cmp/Ocbc-3/reviews"
OUTPUT_DIR = Path("data/debug")
OUTPUT_HTML = OUTPUT_DIR / "inspect_output.html"
OUTPUT_MD = OUTPUT_DIR / "inspect_output.md"


async def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    browser_config = BrowserConfig(
        headless=False,
        browser_type="chromium",
    )
    run_config = CrawlerRunConfig(
        wait_until="load",
        page_timeout=60000,
    )

    async with AsyncWebCrawler(config=browser_config, verbose=True) as crawler:
        result = await crawler.arun(url=URL, config=run_config)

    if not result.success:
        raise RuntimeError(f"Crawl failed: {result.error_message}")

    html = getattr(result, "html", "") or ""
    markdown = ""
    if getattr(result, "markdown", None):
        markdown = getattr(result.markdown, "raw_markdown", "") or str(result.markdown)

    OUTPUT_HTML.write_text(html, encoding="utf-8")
    OUTPUT_MD.write_text(markdown, encoding="utf-8")

    print(f"Saved HTML: {OUTPUT_HTML}")
    print(f"Saved Markdown: {OUTPUT_MD}")


if __name__ == "__main__":
    asyncio.run(main())
