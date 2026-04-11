from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


THREADS = 6
HASH_BUFFER_SIZE = 8 * 1024 * 1024


def get_script_dir() -> Path:
    return Path(__file__).resolve().parent


def sha1_of_file(path: Path) -> str:
    hasher = hashlib.sha1()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(HASH_BUFFER_SIZE), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


def find_source_pngs(script_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in script_dir.rglob("*.png")
        if path.is_file() and path.parent != script_dir
    )


def process_png(
    source_path: Path,
    script_dir: Path,
    lock_map: dict[Path, threading.Lock],
    lock_map_guard: threading.Lock,
    counters: dict[str, int],
    counters_guard: threading.Lock,
) -> str:
    destination_path = script_dir / source_path.name

    with lock_map_guard:
        destination_lock = lock_map.setdefault(destination_path, threading.Lock())

    with destination_lock:
        if not source_path.exists():
            with counters_guard:
                counters["skipped_missing"] += 1
            return f"[MISSING] {source_path}"

        if destination_path.exists():
            source_sha1 = sha1_of_file(source_path)
            destination_sha1 = sha1_of_file(destination_path)

            if source_sha1 == destination_sha1:
                source_path.unlink()

                with counters_guard:
                    counters["deleted_duplicate"] += 1

                return f"[DUPLICATE REMOVED] {source_path} -> kept {destination_path}"

            with counters_guard:
                counters["left_conflict"] += 1

            return f"[CONFLICT LEFT IN PLACE] {source_path} != {destination_path}"

        try:
            source_path.replace(destination_path)
        except Exception as exc:
            with counters_guard:
                counters["errors"] += 1

            return f"[ERROR] {source_path} -> {destination_path} | {exc}"

        with counters_guard:
            counters["moved"] += 1

        return f"[MOVED] {source_path} -> {destination_path}"


def main() -> None:
    script_dir = get_script_dir()
    source_pngs = find_source_pngs(script_dir)

    if not source_pngs:
        print("No PNG files found in subfolders.")
        return

    print(f"Script folder: {script_dir}")
    print(f"Found {len(source_pngs)} PNG file(s) in subfolders.")
    print(f"Using {THREADS} threads.\n")

    lock_map: dict[Path, threading.Lock] = {}
    lock_map_guard = threading.Lock()

    counters = {
        "moved": 0,
        "deleted_duplicate": 0,
        "left_conflict": 0,
        "skipped_missing": 0,
        "errors": 0,
    }
    counters_guard = threading.Lock()

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [
            executor.submit(
                process_png,
                source_path,
                script_dir,
                lock_map,
                lock_map_guard,
                counters,
                counters_guard,
            )
            for source_path in source_pngs
        ]

        for future in as_completed(futures):
            print(future.result())

    print("\nDone.")
    print(f"Moved: {counters['moved']}")
    print(f"Duplicate source files deleted: {counters['deleted_duplicate']}")
    print(f"Conflicts left in place: {counters['left_conflict']}")
    print(f"Skipped missing during processing: {counters['skipped_missing']}")
    print(f"Errors: {counters['errors']}")


if __name__ == "__main__":
    main()