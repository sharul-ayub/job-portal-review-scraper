from pathlib import Path

# Timing
# Random idle delay between navigations/interactions (inclusive range in seconds).
IDLE_DELAY_RANGE_SECONDS = (5.0, 12.0)
# Additional page settle delay before HTML snapshot in crawler run config.
PAGE_SETTLE_DELAY_SECONDS = 4.0

# Crawl target
BASE_URL = "https://malaysia.indeed.com/cmp/Ocbc-3/reviews"
TOTAL_REVIEWS = 125
PAGE_SIZE = 20

# Profile
PROFILE_PATH = Path.home() / ".crawl4ai" / "profiles" / "job_portal_profile_indeed"
WORKER_PROFILES_ROOT = Path("data/raw/worker_profiles")

# Output
OUTPUT_FOLDER = Path("data/raw/pages")
DEBUG_FOLDER = Path("data/raw/debug")
CHECKPOINT_PATH = Path("data/raw/checkpoint.json")
