import json
from datetime import datetime
from pathlib import Path


CHECKPOINT_PATH = Path("data/raw/checkpoint.json")
BACKUP_DIR = Path("data/raw/checkpoint_backups")


def clean_checkpoint() -> None:
    if not CHECKPOINT_PATH.exists():
        print(f"Checkpoint not found: {CHECKPOINT_PATH.resolve()}")
        print("Nothing to clean.")
        return

    data = json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"checkpoint_{ts}.json"
    backup_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    # Keep metadata, reset all page statuses
    data["pages"] = {}
    CHECKPOINT_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    print("Checkpoint cleaned successfully.")
    print(f"Backup saved to: {backup_path}")
    print(f"Reset file: {CHECKPOINT_PATH}")


if __name__ == "__main__":
    clean_checkpoint()
