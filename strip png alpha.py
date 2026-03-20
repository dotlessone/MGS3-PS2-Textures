from __future__ import annotations
BREAK
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from PIL import Image


SCRIPT_DIR = Path(__file__).resolve().parent
TEMP_DIR = SCRIPT_DIR / "_tmp_rgb_strip"
MAX_WORKERS = max(1, min(32, (os.cpu_count() or 4)))


def is_png(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() == ".png"


def iter_pngs(folder: Path) -> list[Path]:
    return sorted(path for path in folder.iterdir() if is_png(path))


def process_png(path: Path) -> tuple[Path, str]:
    temp_output = TEMP_DIR / path.name

    with Image.open(path) as img:
        rgba = img.convert("RGBA")
        rgb, _alpha = rgba.convert("RGB"), rgba.getchannel("A")

        rgb.save(temp_output, format="PNG", optimize=False)

    shutil.move(str(temp_output), str(path))
    return path, "converted"


def main() -> None:
    png_files = iter_pngs(SCRIPT_DIR)

    if not png_files:
        print("No PNG files found in the script folder.")
        return

    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)

    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    converted = 0
    failed = 0

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {
                executor.submit(process_png, path): path
                for path in png_files
            }

            for future in as_completed(future_map):
                path = future_map[future]

                try:
                    _, status = future.result()
                    converted += 1
                    print(f"[OK] {path.name} -> {status}")
                except Exception as exc:
                    failed += 1
                    print(f"[FAIL] {path.name} -> {exc}")

    finally:
        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR, ignore_errors=True)

    print()
    print(f"Done. Converted: {converted}, Failed: {failed}, Total: {len(png_files)}")


if __name__ == "__main__":
    main()