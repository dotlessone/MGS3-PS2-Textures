from __future__ import annotations

import csv
import hashlib
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SKIP_TOP_LEVEL_DIRS = {"Master Collection"}

MAX_WORKERS = max(1, os.cpu_count() or 4)


def sha1_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break

            h.update(chunk)

    return h.hexdigest()


def find_all_sha1_files() -> list[Path]:
    results: list[Path] = []

    for d in ROOT.glob("*/*"):
        if not d.is_dir():
            continue

        if d.parent.name in SKIP_TOP_LEVEL_DIRS:
            continue

        expected_csv = d / f"{d.parent.name}_{d.name}_ALL_SHA1s.csv"
        expected_txt = d / f"{d.parent.name}_{d.name}_ALL_SHA1s.txt"

        if expected_csv.is_file():
            results.append(expected_csv)
            continue

        if expected_txt.is_file():
            results.append(expected_txt)
            continue

        raise FileNotFoundError(f"Missing expected ALL_SHA1s file: {expected_csv}")

    return sorted(results, key=lambda p: str(p).lower())


def collect_pngs(base_dir: Path) -> list[Path]:
    pngs: list[Path] = []

    for p in base_dir.glob("*/*/*/*.png"):
        if not p.is_file():
            continue

        pngs.append(p)

    return sorted(pngs, key=lambda p: str(p.relative_to(base_dir)).lower())


def build_row(base_dir: Path, png_path: Path) -> dict[str, str]:
    rel_png = png_path.relative_to(base_dir)
    rel_img = rel_png.with_suffix(".img")

    return {
        "sha1": sha1_file(png_path),
        "relative_path": str(rel_img),
    }


def atomic_write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fd, tmp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=path.parent)

    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["sha1", "relative_path"])
            writer.writeheader()
            writer.writerows(rows)

        Path(tmp_name).replace(path)
    except Exception:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def process_all_sha1_file(all_sha1_file: Path) -> tuple[Path, int]:
    base_dir = all_sha1_file.parent
    output_csv = base_dir / f"{base_dir.parent.name}_{base_dir.name}_img_strcode_mappings.csv"

    pngs = collect_pngs(base_dir)

    rows: list[dict[str, str]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(build_row, base_dir, png) for png in pngs]

        for future in as_completed(futures):
            rows.append(future.result())

    rows.sort(key=lambda r: (r["relative_path"].lower(), r["sha1"].lower()))

    atomic_write_csv(output_csv, rows)

    return output_csv, len(rows)


def main() -> int:
    try:
        all_sha1_files = find_all_sha1_files()

        print(f"Found ALL_SHA1s files: {len(all_sha1_files)}")
        print(f"Using workers:          {MAX_WORKERS}\n")

        total_rows = 0

        for all_sha1_file in all_sha1_files:
            output_csv, row_count = process_all_sha1_file(all_sha1_file)
            total_rows += row_count

            print(f"Wrote: {output_csv}")
            print(f"Rows:  {row_count}\n")

        print("Done.")
        print(f"Total rows: {total_rows}")
        return 0

    except Exception as e:
        print(f"\nERROR: {e}")
        input("Press Enter to exit...")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())