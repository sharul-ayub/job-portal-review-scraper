import json
import re

from .helpers import clean_text, employee_flag, get_nested


def extract_review_count_from_html(html: str) -> int:
    # Best source for this project: <h2 data-testid="review-count">125 reviews</h2>
    m = re.search(
        r"<h2[^>]*data-testid=[\"']review-count[\"'][^>]*>\s*([\d,]+)\s+reviews\s*</h2>",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        return int(m.group(1).replace(",", ""))

    # Fallbacks if the h2 selector changes.
    for pattern in (
        r"<title[^>]*>.*?([\d,]+)\s+Reviews\b.*?</title>",
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\'][^"\']*?([\d,]+)\s+reviews\b',
        r"([\d,]+)\s+reviews\b",
    ):
        m = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if m:
            return int(m.group(1).replace(",", ""))

    raise RuntimeError("Could not extract total review count from HTML.")


def extract_reviews_from_html(html: str) -> list[dict]:
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
    for review in reviews:
        rows.append(
            {
                "review_date": clean_text(review.get("submissionDate", "")),
                "review_title": clean_text(get_nested(review, "title", "text")),
                "review_body_text": clean_text(get_nested(review, "text", "text")),
                "overall_review_rating": review.get("overallRating", ""),
                "job_title": clean_text(review.get("jobTitle", "")),
                "location": clean_text(review.get("location", "")),
                "employee_flag": employee_flag(review),
                "rating_work_life_balance": get_nested(review, "workAndLifeBalanceRating", "rating"),
                "rating_compensation_benefits": get_nested(review, "compensationAndBenefitsRating", "rating"),
                "rating_job_security_advancement": get_nested(review, "jobSecurityAndAdvancementRating", "rating"),
                "rating_management": get_nested(review, "managementRating", "rating"),
                "rating_culture_values": get_nested(review, "cultureAndValuesRating", "rating"),
                "pros_text": clean_text(get_nested(review, "pros", "text")),
                "cons_text": clean_text(get_nested(review, "cons", "text")),
            }
        )

    return rows
