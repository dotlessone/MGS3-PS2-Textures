import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

MAX_WORKERS = max(4, (os.cpu_count() or 4))

def extract_one(extractor: Path, img: Path) -> tuple[Path, bool, str]:
    out_dir = img.parent

    cmd = [
        sys.executable,
        str(extractor),
        "-o",
        str(out_dir),
        str(img),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        error_text = result.stderr.strip() or result.stdout.strip() or f"Exit code {result.returncode}"
        return img, False, error_text

    return img, True, str(out_dir)

def main():
    script_dir = Path(__file__).resolve().parent
    extractor = script_dir / "extract img.py"

    if not extractor.is_file():
        print(f"Extractor not found: {extractor}")
        input("Press ENTER to exit...")
        return

    img_files = sorted(script_dir.rglob("*.img"))

    if not img_files:
        print("No .img files found.")
        input("Press ENTER to exit...")
        return

    print(f"Found {len(img_files)} .img files.")
    print(f"Using {MAX_WORKERS} workers.\n")

    errors = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(extract_one, extractor, img): img
            for img in img_files
        }

        done = 0

        for future in as_completed(futures):
            done += 1

            img, ok, message = future.result()

            if ok:
                print(f"[{done}/{len(img_files)}] OK: {img} -> {message}")
            else:
                print(f"[{done}/{len(img_files)}] FAILED: {img}")
                errors.append((img, message))

    print("\nDone.")

    if errors:
        print(f"\n{len(errors)} errors:\n")

        for path, err in errors:
            print(path)
            print(err)
            print("-" * 80)

    input("Press ENTER to exit...")

if __name__ == "__main__":
    main()