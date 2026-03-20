from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from threading import Lock


# ==========================================================
# CONFIG
# ==========================================================
# Target timestamp: 2005-12-01 12:52:46 +09:00 (GMT+9)
TARGET_TIME = datetime(2005, 12, 1, 12, 52, 46, tzinfo=timezone(timedelta(hours=9)))

SCRIPT_DIR = Path(__file__).resolve().parent
MAX_WORKERS = min(32, (os.cpu_count() or 8) * 2)

PRINT_LOCK = Lock()


def safe_print(msg: str) -> None:
    with PRINT_LOCK:
        print(msg)


def set_mtime(path: Path, ts: float) -> None:
    # os.utime expects seconds since epoch (UTC-based)
    os.utime(path, (ts, ts))


def collect_tga_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if name.lower().endswith(".tga"):
                out.append(Path(dirpath) / name)
    return out


def worker(path: Path, ts: float) -> tuple[Path, bool, str]:
    try:
        set_mtime(path, ts)
        return (path, True, "")
    except Exception as exc:
        return (path, False, str(exc))


def main() -> int:
    target_ts = TARGET_TIME.timestamp()

    files = collect_tga_files(SCRIPT_DIR)
    if not files:
        print("[INFO] No .tga files found under script directory.")
        return 0

    total = len(files)
    ok = 0
    fail = 0

    print(f"[INFO] Root: {SCRIPT_DIR}")
    print(f"[INFO] Found {total} .tga files")
    print(f"[INFO] Setting mtime to: {TARGET_TIME.isoformat()} (epoch={target_ts})")
    print(f"[INFO] Workers: {MAX_WORKERS}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(worker, p, target_ts) for p in files]

        done = 0
        for fut in as_completed(futures):
            path, success, err = fut.result()
            done += 1

            if success:
                ok += 1
            else:
                fail += 1
                safe_print(f"[ERROR] {path}: {err}")

            # Light progress, no spam
            if done == 1 or done % 250 == 0 or done == total:
                safe_print(f"[PROGRESS] {done}/{total} | ok={ok} fail={fail}")

    print(f"[DONE] Updated mtime for {ok}/{total} files (fail={fail})")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
