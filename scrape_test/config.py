from pathlib import Path

# Timing
PAGE_STABILIZE_DELAY_SECONDS = 30

# Crawl target
BASE_URL = "https://malaysia.indeed.com/cmp/Cimb-Group/reviews"
TOTAL_REVIEWS = 338
PAGE_SIZE = 20

# Profile
PROFILE_PATH = Path.home() / ".crawl4ai" / "profiles" / "job_portal_profile_indeed"
WORKER_PROFILES_ROOT = Path("data/raw/worker_profiles")

# Output
OUTPUT_FOLDER = Path("data/raw/pages")
DEBUG_FOLDER = Path("data/raw/debug")
CHECKPOINT_PATH = Path("data/raw/checkpoint.json")
