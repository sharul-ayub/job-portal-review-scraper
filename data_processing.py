import csv
import json
from datetime import datetime
from pathlib import Path


FIELD_ORDER = [
    "review_date",
    "review_title",
    "review_body_text",
    "overall_review_rating",
    "job_title",
    "location",
    "employee_flag",
    "rating_work_life_balance",
    "rating_compensation_benefits",
    "rating_job_security_advancement",
    "rating_management",
    "rating_culture_values",
    "pros_text",
    "cons_text",
    "page_number",
    "start_offset",
    "source_url",
]


def _parse_review_date(value: str) -> datetime:
    text = (value or "").strip()
    if not text:
        return datetime.min
    try:
        return datetime.strptime(text, "%d %B %Y")
    except ValueError:
        return datetime.min


def _excel_safe(value):
    """Prevent Excel from interpreting text as formulas."""
    if value is None:
        return ""
    s = str(value)
    if s.startswith(("=", "+", "-", "@")):
        return "'" + s
    return s


def merge_page_json_to_csv() -> None:
    pages_dir = Path("data/raw/pages")
    merged_json_path = Path("data/processed/reviews_merged Cimb-Group.json")
    merged_csv_path = Path("data/processed/reviews_merged Cimb-Group.csv")

    if not pages_dir.exists():
        raise FileNotFoundError(f"Pages directory not found: {pages_dir.resolve()}")

    page_files = sorted(pages_dir.glob("*.json"))
    if not page_files:
        raise RuntimeError(f"No JSON files found in {pages_dir.resolve()}")

    all_rows: list[dict] = []
    for fp in page_files:
        payload = json.loads(fp.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            all_rows.extend(payload)
        elif isinstance(payload, dict):
            all_rows.append(payload)

    if not all_rows:
        raise RuntimeError("No records found after merging page JSON files.")

    # Sort latest -> oldest
    all_rows.sort(key=lambda r: _parse_review_date(r.get("review_date", "")), reverse=True)

    # Keep requested field order first, then any extras
    extra_fields = sorted({k for row in all_rows for k in row.keys()} - set(FIELD_ORDER))
    fieldnames = FIELD_ORDER + extra_fields

    merged_json_path.parent.mkdir(parents=True, exist_ok=True)
    merged_json_path.write_text(json.dumps(all_rows, indent=2, ensure_ascii=False), encoding="utf-8")

    with merged_csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        safe_rows = []
        for row in all_rows:
            safe_rows.append({k: _excel_safe(v) for k, v in row.items()})
        writer.writerows(safe_rows)

    print(f"Merged files: {len(page_files)}")
    print(f"Total records: {len(all_rows)}")
    print("Sorted by review_date: latest -> oldest")
    print(f"JSON output: {merged_json_path}")
    print(f"CSV output: {merged_csv_path}")


if __name__ == "__main__":
    merge_page_json_to_csv()
