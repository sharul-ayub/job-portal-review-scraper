def clean_text(value: str) -> str:
    return " ".join((value or "").split())


def get_nested(dct: dict, *keys):
    cur = dct
    for k in keys:
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(k)
    return cur if cur is not None else ""


def employee_flag(review: dict) -> str:
    val = review.get("currentEmployee")
    if val is True:
        return "current"
    if val is False:
        return "former"
    return ""


def build_page_url(base_url: str, start: int) -> str:
    if start == 0:
        return base_url
    return f"{base_url}?start={start}"
