from __future__ import annotations

import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from PIL import Image


SCRIPT_DIR = Path(__file__).resolve().parent
LOG_PATH = SCRIPT_DIR / "remaining_ctxr_folders.txt"
MAX_WORKERS = min(32, (os.cpu_count() or 1) * 2)

print_lock = threading.Lock()
count_lock = threading.Lock()

deleted_ctxr_count = 0
converted_dds_count = 0
deleted_dds_count = 0


def safe_relative(path: Path) -> str:
    try:
        return path.relative_to(SCRIPT_DIR).as_posix()
    except Exception:
        return str(path)


def delete_file(path: Path) -> None:
    path.unlink()


def convert_dds_to_png_and_remove_dds(dds_path: Path) -> tuple[bool, str | None]:
    png_path = dds_path.with_suffix(".png")
    temp_png_path = png_path.with_name(png_path.name + ".tmp")

    try:
        with Image.open(dds_path) as img:
            img.load()

            if img.mode not in ("RGB", "RGBA", "L", "LA", "P"):
                img = img.convert("RGBA")
            elif img.mode == "P":
                if "transparency" in img.info:
                    img = img.convert("RGBA")
                else:
                    img = img.convert("RGB")

            img.save(temp_png_path, format="PNG", optimize=False)

        if not temp_png_path.is_file():
            return (False, "Temporary PNG was not created")

        temp_png_path.replace(png_path)
        delete_file(dds_path)
        return (True, None)
    except Exception as exc:
        try:
            if temp_png_path.exists():
                temp_png_path.unlink()
        except Exception:
            pass
        return (False, f"Failed DDS->PNG conversion/removal: {exc}")


def process_ctxr(ctxr_path: Path) -> tuple[Path, bool, bool, bool, list[str]]:
    errors: list[str] = []

    png_path = ctxr_path.with_suffix(".png")
    dds_path = ctxr_path.with_suffix(".dds")

    deleted_ctxr = False
    converted_dds = False
    deleted_dds = False

    try:
        if png_path.is_file():
            delete_file(ctxr_path)
            deleted_ctxr = True
            return (ctxr_path, deleted_ctxr, converted_dds, deleted_dds, errors)

        if dds_path.is_file():
            conversion_ok, conversion_error = convert_dds_to_png_and_remove_dds(dds_path)

            if conversion_ok:
                converted_dds = True
                deleted_dds = True

                try:
                    delete_file(ctxr_path)
                    deleted_ctxr = True
                except Exception as exc:
                    errors.append(f"{safe_relative(ctxr_path)} | Failed to delete .ctxr after DDS conversion: {exc}")
            else:
                if conversion_error:
                    errors.append(f"{safe_relative(dds_path)} | {conversion_error}")

            return (ctxr_path, deleted_ctxr, converted_dds, deleted_dds, errors)

        return (ctxr_path, deleted_ctxr, converted_dds, deleted_dds, errors)
    except Exception as exc:
        errors.append(f"{safe_relative(ctxr_path)} | Unexpected error: {exc}")
        return (ctxr_path, deleted_ctxr, converted_dds, deleted_dds, errors)


def main() -> int:
    global deleted_ctxr_count
    global converted_dds_count
    global deleted_dds_count

    ctxr_files = [p for p in SCRIPT_DIR.rglob("*.ctxr") if p.is_file()]

    if not ctxr_files:
        LOG_PATH.write_text("", encoding="utf-8", newline="\n")
        print("No .ctxr files found.")
        print(f"Remaining log written to: {LOG_PATH}")
        return 0

    print(f"Scanning {SCRIPT_DIR}")
    print(f"Found {len(ctxr_files)} .ctxr file(s)")
    print(f"Using {MAX_WORKERS} worker(s)")

    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_ctxr, path) for path in ctxr_files]

        for future in as_completed(futures):
            ctxr_path, deleted_ctxr, converted_dds, deleted_dds, item_errors = future.result()

            rel_ctxr = safe_relative(ctxr_path)

            with count_lock:
                if deleted_ctxr:
                    deleted_ctxr_count += 1
                if converted_dds:
                    converted_dds_count += 1
                if deleted_dds:
                    deleted_dds_count += 1

            with print_lock:
                if converted_dds:
                    print(f"Converted DDS to PNG and removed DDS: {safe_relative(ctxr_path.with_suffix('.dds'))}")
                if deleted_ctxr:
                    print(f"Deleted CTXR: {rel_ctxr}")

                for error in item_errors:
                    print(f"ERROR: {error}")

            errors.extend(item_errors)

    remaining_dirs = sorted(
        {
            p.parent.resolve()
            for p in SCRIPT_DIR.rglob("*.ctxr")
            if p.is_file()
        }
    )

    with LOG_PATH.open("w", encoding="utf-8", newline="\n") as f:
        for dir_path in remaining_dirs:
            f.write(str(dir_path))
            f.write("\n")

        if errors:
            f.write("\n# Errors\n")
            for error in errors:
                f.write(error)
                f.write("\n")

    print()
    print(f"Deleted .ctxr files: {deleted_ctxr_count}")
    print(f"Converted .dds to .png: {converted_dds_count}")
    print(f"Deleted .dds files: {deleted_dds_count}")
    print(f"Remaining folders with .ctxr: {len(remaining_dirs)}")
    print(f"Folder log written to: {LOG_PATH}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nAborted by user.")
        raise SystemExit(1)