import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

INPUT_DIR = Path("data/processed")

OUTPUTS = {
    ".json": INPUT_DIR / "reviews_compiled_from_json.json",
    ".csv": INPUT_DIR / "reviews_compiled_from_csv.csv",
}

FIELD_ORDER = [
    "company_name",
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


def infer_company_name(file_name: str) -> str:
    stem = Path(file_name).stem
    stem = re.sub(r"^reviews_merged1?", "", stem, flags=re.IGNORECASE)
    stem = stem.replace("_", " ").strip(" -")
    stem = re.sub(r"\s+", " ", stem)
    return stem or "Unknown"


def read_json_records(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        reviews = data.get("reviews")
        if isinstance(reviews, list):
            return [row for row in reviews if isinstance(row, dict)]
        return [data]
    return []


def read_csv_records(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def iter_input_files(folder: Path, suffix: str) -> Iterable[Path]:
    ignored_names = {
        OUTPUTS[".json"].name,
        OUTPUTS[".csv"].name,
    }
    for path in sorted(folder.iterdir()):
        if not path.is_file():
            continue
        if path.suffix.lower() != suffix:
            continue
        if path.name in ignored_names:
            continue
        if not path.name.lower().startswith("reviews_merged"):
            continue
        yield path


def _parse_review_date(value: str) -> datetime:
    text = (value or "").strip()
    if not text:
        return datetime.min
    try:
        return datetime.strptime(text, "%d %B %Y")
    except ValueError:
        return datetime.min


def _excel_safe(value):
    if value is None:
        return ""
    s = str(value)
    if s.startswith(("=", "+", "-", "@")):
        return "'" + s
    return s


def _norm(v) -> str:
    return " ".join(str(v or "").split()).strip().lower()


def _dedupe_key(row: dict) -> tuple:
    return (
        _norm(row.get("company_name")),
        _norm(row.get("source_url")),
        _norm(row.get("review_date")),
        _norm(row.get("review_title")),
        _norm(row.get("review_body_text")),
        _norm(row.get("job_title")),
        _norm(row.get("location")),
    )


def compile_group(suffix: str) -> None:
    paths = list(iter_input_files(INPUT_DIR, suffix))
    if not paths:
        print(f"No {suffix} input rows found to compile.")
        return

    merged: list[dict] = []

    for file_path in paths:
        company_name = infer_company_name(file_path.name)
        records = read_json_records(file_path) if suffix == ".json" else read_csv_records(file_path)

        for row in records:
            row = dict(row)
            row["company_name"] = company_name
            merged.append(row)

        print(f"[{suffix}] Loaded {len(records)} rows from {file_path.name} -> company_name='{company_name}'")

    if not merged:
        print(f"No {suffix} records found after reading files.")
        return

    before_dedupe = len(merged)
    deduped: list[dict] = []
    seen_keys: set[tuple] = set()
    for row in merged:
        key = _dedupe_key(row)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(row)

    merged = deduped
    removed = before_dedupe - len(merged)

    merged.sort(key=lambda r: _parse_review_date(r.get("review_date", "")), reverse=True)

    output_path = OUTPUTS[suffix]
    if suffix == ".json":
        output_path.write_text(json.dumps(merged, indent=2, ensure_ascii=False), encoding="utf-8")
    else:
        extra_fields = sorted({key for row in merged for key in row.keys()} - set(FIELD_ORDER))
        fieldnames = FIELD_ORDER + extra_fields
        with output_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows([{k: _excel_safe(v) for k, v in row.items()} for row in merged])

    print(f"[{suffix}] Compiled {len(merged)} total rows")
    print(f"[{suffix}] Removed duplicates: {removed}")
    print(f"[{suffix}] Sorted by review_date: latest -> oldest")
    print(f"[{suffix}] Output: {output_path}")


def main() -> None:
    if not INPUT_DIR.exists():
        raise FileNotFoundError(f"Input folder not found: {INPUT_DIR}")

    compile_group(".json")
    compile_group(".csv")


if __name__ == "__main__":
    main()
