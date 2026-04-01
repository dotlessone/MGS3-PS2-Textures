from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Optional, Tuple

from PIL import Image


# ==========================================================
# CONFIG
# ==========================================================
SCRIPT_DIR = Path(__file__).resolve().parent
MAX_WORKERS = max(4, os.cpu_count() or 8)

# Pillow can be noisy about large images / decompression bombs in some cases
Image.MAX_IMAGE_PIXELS = None

PRINT_LOCK = Lock()


# ==========================================================
# Helpers
# ==========================================================
def log(msg: str) -> None:
    with PRINT_LOCK:
        print(msg, flush=True)


def is_power_of_two(n: int) -> bool:
    return n > 0 and (n & (n - 1)) == 0


def is_one_above_power_of_two(n: int) -> bool:
    # True if n = (power_of_two + 1)
    return n > 1 and is_power_of_two(n - 1)


def new_dims_if_fixable(w: int, h: int) -> Optional[Tuple[int, int]]:
    new_w = w - 1 if is_one_above_power_of_two(w) else w
    new_h = h - 1 if is_one_above_power_of_two(h) else h

    if new_w == w and new_h == h:
        return None

    if new_w <= 0 or new_h <= 0:
        return None

    return (new_w, new_h)


def process_tga(path: Path) -> Tuple[bool, str]:
    """
    Returns: (changed, message)
    """
    try:
        with Image.open(path) as im:
            w, h = im.size
            target = new_dims_if_fixable(w, h)
            if target is None:
                return (False, f"[SKIP] {path} ({w}x{h})")

            new_w, new_h = target
            if new_w == w and new_h == h:
                return (False, f"[SKIP] {path} ({w}x{h})")

            # Crop from right/bottom by reducing size
            cropped = im.crop((0, 0, new_w, new_h))

            # Preserve mode, etc. Save back to same path (overwrite)
            # Pillow will write uncompressed TGA by default.
            cropped.save(path)

            cropped.close()

        what = []
        if new_w != w:
            what.append("rightmost -1px")
        if new_h != h:
            what.append("bottom -1px")

        return (True, f"[FIX]  {path} ({w}x{h} -> {new_w}x{new_h}) ({', '.join(what)})")
    except Exception as exc:
        return (False, f"[ERR]  {path} ({exc})")


def iter_tga_files(root: Path) -> list[Path]:
    # Using rglob is fine; list first so progress is stable
    return [p for p in root.rglob("*.tga") if p.is_file()]


# ==========================================================
# Main
# ==========================================================
def main() -> int:
    root = SCRIPT_DIR
    log(f"[INFO] Root: {root}")
    log(f"[INFO] Threads: {MAX_WORKERS}")

    files = iter_tga_files(root)
    log(f"[INFO] Found .tga files: {len(files):,}")

    if not files:
        return 0

    changed = 0
    skipped = 0
    errored = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_tga, p) for p in files]

        done = 0
        total = len(futures)

        for fut in as_completed(futures):
            done += 1
            try:
                did_change, msg = fut.result()
            except Exception as exc:
                errored += 1
                log(f"[ERR]  Worker failure: {exc}")
                continue

            if msg.startswith("[FIX]"):
                changed += 1
            elif msg.startswith("[ERR]"):
                errored += 1
            else:
                skipped += 1

            # Print every result line; comment this out if you want quieter output
            log(msg)

            if done % 250 == 0 or done == total:
                log(f"[INFO] Progress {done:,}/{total:,} | fixed={changed:,} skipped={skipped:,} errors={errored:,}")

    log(f"[DONE] fixed={changed:,} skipped={skipped:,} errors={errored:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
