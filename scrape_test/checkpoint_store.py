import json
from pathlib import Path


def load_checkpoint(path: Path, base_url: str, total_pages: int, page_size: int) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass

    return {
        "base_url": base_url,
        "total_pages": total_pages,
        "page_size": page_size,
        "pages": {},
    }


def save_checkpoint(path: Path, checkpoint: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(checkpoint, indent=2, ensure_ascii=False), encoding="utf-8")
