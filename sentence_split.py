import csv
import json
import re
from datetime import datetime
from pathlib import Path

INPUT_JSON = Path("data/processed/reviews_compiled_from_json.json")

OUTPUT_JSON = Path("data/data_nlp/reviews_sentence_level.json")
OUTPUT_CSV = Path("data/data_nlp/reviews_sentence_level.csv")

SPLIT_TOKEN = "<SPLIT_SENT>"
DOT_TOKEN = "<DOT_KEEP>"
DEC_TOKEN = "<DEC_KEEP>"
PAREN_HYPHEN_TOKEN = "<PAREN_HYPHEN_KEEP>"
PROCON_DASH_TOKEN = "<PROCON_DASH_KEEP>"
DAY_RANGE_HYPHEN_TOKEN = "<DAY_RANGE_HYPHEN_KEEP>"
LABEL_COLON_DASH_TOKEN = "<LABEL_COLON_DASH_KEEP>"
CONJ_COMMA_DASH_TOKEN = "<CONJ_COMMA_DASH_KEEP>"
YEAR_FROM = 2024
YEAR_TO = 2025

ABBREVIATIONS = [
    "e.g.",
    "i.e.",
    "mr.",
    "mrs.",
    "ms.",
    "dr.",
    "prof.",
    "inc.",
    "ltd.",
    "co.",
    "corp.",
    "u.s.",
    "u.k.",
    "sdn.",
    "bhd.",
    "What you learned: - Good Communication",
]


def _clean_space(text: str) -> str:
    return " ".join((text or "").split())


def _remove_emoji_symbols(text: str) -> str:
    # Remove most emoji/pictograph symbols that create noisy NLP tokens.
    return re.sub(
        r"[\U0001F300-\U0001F5FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FAFF\U00002700-\U000027BF\U0001F1E6-\U0001F1FF]+",
        "",
        text,
    )


def _protect_abbreviations(text: str) -> str:
    out = text
    for abbr in ABBREVIATIONS:
        escaped = re.escape(abbr)
        repl = abbr.replace(".", DOT_TOKEN)
        out = re.sub(escaped, repl, out, flags=re.IGNORECASE)
    return out


def _restore_abbreviations(text: str) -> str:
    return text.replace(DOT_TOKEN, ".")


def _protect_hyphen_in_parentheses(text: str) -> str:
    def repl(match: re.Match) -> str:
        segment = match.group(0)
        return segment.replace("-", PAREN_HYPHEN_TOKEN)

    return re.sub(r"\([^()]*\)", repl, text)


def _protect_pros_cons_dash(text: str) -> str:
    # Keep pros/cons markers with dash variants together so bullet split does not break them.
    # Covers: "Pros:- - ...", "Cons:-- ...", "Pros - ...", "Con- ...".
    pattern = r"(?i)\b(pros?|cons?)\s*:?\s*(?:-\s*){1,3}"
    return re.sub(pattern, lambda m: f"{m.group(1)} {PROCON_DASH_TOKEN} ", text)


def _protect_day_ranges(text: str) -> str:
    # Prevent splitting patterns like "Mon - Fri".
    pattern = (
        r"(?i)\b("
        r"mon(?:day)?|tue(?:s|sday)?|wed(?:nesday)?|thu(?:r|rs|rsday)?|"
        r"fri(?:day)?|sat(?:urday)?|sun(?:day)?"
        r")\s*-\s*("
        r"mon(?:day)?|tue(?:s|sday)?|wed(?:nesday)?|thu(?:r|rs|rsday)?|"
        r"fri(?:day)?|sat(?:urday)?|sun(?:day)?"
        r")\b"
    )
    return re.sub(
        pattern,
        lambda m: f"{m.group(1)} {DAY_RANGE_HYPHEN_TOKEN} {m.group(2)}",
        text,
    )


def _protect_label_colon_dash(text: str) -> str:
    # Keep generic "Label: - text" together (e.g., "Benefit: - Medical for self & family").
    pattern = r"\b([A-Za-z][A-Za-z/& ]{1,40})\s*:\s*-\s+"
    return re.sub(
        pattern,
        lambda m: f"{m.group(1).strip()}: {LABEL_COLON_DASH_TOKEN} ",
        text,
    )


def _split_before_label_colon_dash(text: str) -> str:
    # Split only before likely heading labels to avoid splitting normal phrases.
    # Examples:
    # "... Good friendship What you learned: - Good Communication ..."
    # -> "... Good friendship <SPLIT> What you learned: - Good Communication ..."
    # "... Good Communication The Hardest Part: - KPI ..."
    # -> "... Good Communication <SPLIT> The Hardest Part: - KPI ..."
    pattern = (
        r"(?i)\s+(?="
        r"(?:what|the|why|how|my|overall|advice|areas|benefit|benefits|pros?|cons?)"
        r"(?:\s+[A-Za-z][A-Za-z/&]*){0,8}"
        r"\s*:\s*<LABEL_COLON_DASH_KEEP>\s*"
        r")"
    )
    return re.sub(pattern, f" {SPLIT_TOKEN} ", text)


def _protect_conjunction_comma_dash(text: str) -> str:
    # Keep patterns like "But, - paperwork overload" as one chunk.
    pattern = r"(?i)\b(but|and|so|or|however|therefore),\s*-\s+"
    return re.sub(pattern, lambda m: f"{m.group(1)}, {CONJ_COMMA_DASH_TOKEN} ", text)


def _split_sentences(text: str) -> list[str]:
    if not text or not text.strip():
        return []

    t = _remove_emoji_symbols(text.replace("\r\n", "\n").replace("\r", "\n"))
    t = _protect_hyphen_in_parentheses(t)
    t = _protect_pros_cons_dash(t)
    t = _protect_day_ranges(t)
    t = _protect_label_colon_dash(t)
    t = _protect_conjunction_comma_dash(t)
    t = _split_before_label_colon_dash(t)

    # Bullets with/without spaces, including first token in the text.
    t = re.sub(r"^\s*[-*•]+\s*", f"{SPLIT_TOKEN} ", t)
    t = re.sub(r"\n\s*[-*•]+\s*", f" {SPLIT_TOKEN} ", t)
    t = re.sub(r"(?<=\s)[-*•]+(?=\s*[A-Za-z0-9])", f" {SPLIT_TOKEN} ", t)
    # Numbered lists: supports "1) 2) 3)" and "1. 2. 3." (with/without spacing).
    # The numbering markers are removed after split by replacing with SPLIT_TOKEN.
    t = re.sub(r"\n\s*\d{1,3}[\.)]\s*", f" {SPLIT_TOKEN} ", t)
    t = re.sub(r"(?:^|(?<=\s))\d{1,3}[\.)](?=\s|[A-Za-z])\s*", f" {SPLIT_TOKEN} ", t)
    t = re.sub(r"\n+", f" {SPLIT_TOKEN} ", t)
    # Split before pros/cons markers but keep marker text in output.
    # Examples: "pros:", "cons :", "pros-", "Pros:- -", "Pro: -", "con: -"
    t = re.sub(
        r"(?i)\s+(?=(?:pro|pros|con|cons)\s*(?::|-))",
        f" {SPLIT_TOKEN} ",
        t,
    )

    t = re.sub(r"(\d)\.(\d)", rf"\1{DEC_TOKEN}\2", t)
    t = _protect_abbreviations(t)

    # Split on ! and ?, but for dot only split single "." (not ".." or "...").
    t = re.sub(r"([!?]+)(\s+|$)", rf"\1 {SPLIT_TOKEN} ", t)
    t = re.sub(r"((?<!\.)\.(?!\.))(\s+|$)", rf"\1 {SPLIT_TOKEN} ", t)
    # Handle no-space joins like: "newcommer.On"
    t = re.sub(r"([.!?])([A-Za-z])", rf"\1 {SPLIT_TOKEN} \2", t)
    t = re.sub(r";+(\s+|$)", f" {SPLIT_TOKEN} ", t)

    t = _restore_abbreviations(t)
    t = t.replace(DEC_TOKEN, ".")
    t = t.replace(PAREN_HYPHEN_TOKEN, "-")
    t = t.replace(PROCON_DASH_TOKEN, "-")
    t = t.replace(DAY_RANGE_HYPHEN_TOKEN, "-")
    t = t.replace(LABEL_COLON_DASH_TOKEN, "-")
    t = t.replace(CONJ_COMMA_DASH_TOKEN, "-")

    parts = [_clean_space(p) for p in t.split(SPLIT_TOKEN)]
    parts = [p for p in parts if p]

    # Split by ">" only when there are multiple ">" in the same sentence/chunk.
    gt_expanded = []
    for p in parts:
        if p.count(">") >= 2:
            sub_parts = [_clean_space(x) for x in re.split(r"\s*>\s*", p)]
            gt_expanded.extend([x for x in sub_parts if x])
        else:
            gt_expanded.append(p)
    parts = gt_expanded

    out = []
    for p in parts:
        if p in {"-", "*", "•"}:
            continue
        out.append(p)
    return out


def _read_reviews(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        rows = data.get("reviews")
        if isinstance(rows, list):
            return [x for x in rows if isinstance(x, dict)]
        return [data]
    return []


def _excel_safe(value):
    if value is None:
        return ""
    s = str(value)
    if s.startswith(("=", "+", "-", "@")):
        return "'" + s
    return s


def _review_year(value: str):
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%d %B %Y").year
    except ValueError:
        return None


def main() -> None:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_JSON}")

    reviews_all = _read_reviews(INPUT_JSON)
    if not reviews_all:
        raise RuntimeError(f"No review records found in {INPUT_JSON}")

    reviews = []
    dropped_by_year = 0
    for row in reviews_all:
        year = _review_year(str(row.get("review_date", "") or ""))
        if year is None or year < YEAR_FROM or year > YEAR_TO:
            dropped_by_year += 1
            continue
        reviews.append(row)

    if not reviews:
        raise RuntimeError(
            f"No reviews in year range {YEAR_FROM}-{YEAR_TO}. "
            f"Total input rows: {len(reviews_all)}"
        )

    sentence_rows: list[dict] = []

    for review_idx, row in enumerate(reviews, start=1):
        body = str(row.get("review_body_text", "") or "")
        sentences = _split_sentences(body)

        if not sentences:
            continue

        for sent_idx, sent in enumerate(sentences, start=1):
            out = dict(row)
            out["review_row_id"] = review_idx
            out["sentence_id"] = sent_idx
            out["sentence_text"] = sent
            sentence_rows.append(out)

    if not sentence_rows:
        raise RuntimeError("No sentence-level rows were produced.")

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(sentence_rows, indent=2, ensure_ascii=False), encoding="utf-8")

    fieldnames = [
        "review_row_id",
        "review_body_text",
        "sentence_id",
        "sentence_text",
    ]
    extra_fields = sorted({k for r in sentence_rows for k in r.keys()} - set(fieldnames))
    fieldnames = fieldnames + extra_fields

    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        safe_rows = [{k: _excel_safe(v) for k, v in r.items()} for r in sentence_rows]
        writer.writerows(safe_rows)

    print(f"Input reviews (all): {len(reviews_all)}")
    print(f"Kept reviews ({YEAR_FROM}-{YEAR_TO}): {len(reviews)}")
    print(f"Dropped by year/date parse: {dropped_by_year}")
    print(f"Sentence-level rows: {len(sentence_rows)}")
    print(f"JSON output: {OUTPUT_JSON}")
    print(f"CSV output: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
