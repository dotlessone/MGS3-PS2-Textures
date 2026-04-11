from __future__ import annotations

import csv
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import time
import threading

MAX_WORKERS = 24
CHUNK_SIZE = 8 * 1024 * 1024

# ==========================================================
# CONFIG
# ==========================================================
VALID_PARENTS = {
    "Snake Eater",
    "Subsistence",
    "Trial Edition",
}


def pause_and_exit(code: int = 0) -> None:
    try:
        input("Press ENTER to exit...")
    except EOFError:
        pass
    raise SystemExit(code)


def sha1_of_file(path: Path) -> str:
    sha1 = hashlib.sha1()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_SIZE)
            if not chunk:
                break
            sha1.update(chunk)

    return sha1.hexdigest()


def validate_location(script_dir: Path) -> str:
    parent = script_dir.name

    if parent not in VALID_PARENTS:
        print("ERROR: Script is in an unexpected location.")
        print(f"  Detected folder: '{parent}'")
        print(f"  Expected one of: {sorted(VALID_PARENTS)}")
        pause_and_exit(1)

    return parent


def build_output_name(parent: str, subfolder: str) -> str:
    return f"{parent}_{subfolder}_ALL_SHA1s.txt"


def build_csv_name(parent: str, subfolder: str) -> str:
    return f"{parent}_{subfolder}_tri_strcode_mappings.csv"


def find_png_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() == ".png"
    )


def extract_csv_row(path: Path) -> tuple[str, str, str, str]:
    texture_strcode = path.stem
    tri_strcode = path.parent.name
    stage_folder = path.parent.parent.name if path.parent.parent else ""
    dat_file = path.parent.parent.parent.name if path.parent.parent.parent else ""

    return dat_file, stage_folder, tri_strcode, texture_strcode


def hash_pngs(
    png_files: list[Path],
) -> tuple[set[str], list[tuple[str, str, str, str, str]], list[tuple[Path, str]]]:
    unique_sha1s: set[str] = set()
    csv_rows: list[tuple[str, str, str, str, str]] = []
    failures: list[tuple[Path, str]] = []

    total = len(png_files)
    completed = 0
    completed_lock = threading.Lock()

    start_time = time.time()
    last_print = 0

    def progress_thread():
        nonlocal completed, last_print

        while True:
            time.sleep(1)

            with completed_lock:
                done = completed

            if done == 0:
                continue

            elapsed = time.time() - start_time
            rate = done / elapsed if elapsed > 0 else 0
            remaining = total - done
            eta = remaining / rate if rate > 0 else 0

            percent = (done / total) * 100

            print(
                f"\rProgress: {done}/{total} ({percent:.2f}%) | "
                f"{rate:.2f} files/sec | ETA: {int(eta)}s",
                end="",
                flush=True,
            )

            if done >= total:
                break

        print()  # newline after completion

    t = threading.Thread(target=progress_thread, daemon=True)
    t.start()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_path = {
            executor.submit(sha1_of_file, path): path
            for path in png_files
        }

        for future in as_completed(future_to_path):
            path = future_to_path[future]

            try:
                texture_sha1 = future.result()
                unique_sha1s.add(texture_sha1)

                dat_file, stage_folder, tri_strcode, texture_strcode = extract_csv_row(path)
                csv_rows.append(
                    (
                        texture_sha1,
                        dat_file,
                        stage_folder,
                        tri_strcode,
                        texture_strcode,
                    )
                )
            except Exception as exc:
                failures.append((path, str(exc)))

            with completed_lock:
                completed += 1

    t.join()

    csv_rows.sort(
        key=lambda row: (
            row[1].lower(),
            row[2].lower(),
            row[3].lower(),
            row[4].lower(),
            row[0].lower(),
        )
    )

    return unique_sha1s, csv_rows, failures


def write_sha1_file(output_path: Path, sha1s: set[str]) -> None:
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for sha1 in sorted(sha1s):
            handle.write(f"{sha1}\n")


def write_csv_file(output_path: Path, rows: list[tuple[str, str, str, str, str]]) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(
            [
                "texture_sha1",
                "dat_file",
                "stage_folder",
                "tri_strcode",
                "texture_strcode",
            ]
        )

        writer.writerows(rows)


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    parent = validate_location(script_dir)

    subfolders = sorted(path for path in script_dir.iterdir() if path.is_dir())

    if not subfolders:
        print("No subfolders found.")
        pause_and_exit(0)

    overall_failures: list[tuple[Path, str]] = []
    created_count = 0
    skipped_count = 0

    print(f"Found {len(subfolders)} subfolder(s) under '{parent}'.")
    print(f"Hashing with {MAX_WORKERS} workers...")
    print()

    for subfolder in subfolders:
        txt_output_path = subfolder / build_output_name(parent, subfolder.name)
        csv_output_path = subfolder / build_csv_name(parent, subfolder.name)

        if txt_output_path.exists() or csv_output_path.exists():
            print(
                f"Skipping '{subfolder.name}' - output already exists: "
                f"{txt_output_path.name if txt_output_path.exists() else csv_output_path.name}"
            )
            skipped_count += 1
            continue

        png_files = find_png_files(subfolder)

        if not png_files:
            print(f"Skipping '{subfolder.name}' - no PNG files found.")
            skipped_count += 1
            continue

        print(f"Processing '{subfolder.name}'")
        print(f"  Found {len(png_files)} PNG file(s).")

        unique_sha1s, csv_rows, failures = hash_pngs(png_files)
        write_sha1_file(txt_output_path, unique_sha1s)
        write_csv_file(csv_output_path, csv_rows)

        print(f"  Unique SHA1 count: {len(unique_sha1s)}")
        print(f"  TXT output written to: {txt_output_path}")
        print(f"  CSV output written to: {csv_output_path}")
        print()

        overall_failures.extend(failures)
        created_count += 1

    print("Done.")
    print(f"Created: {created_count}")
    print(f"Skipped: {skipped_count}")

    if overall_failures:
        print()
        print(f"Failed to hash {len(overall_failures)} file(s):")
        for path, error in overall_failures:
            print(f"  {path} -> {error}")
        pause_and_exit(1)

    pause_and_exit(0)


if __name__ == "__main__":
    main()