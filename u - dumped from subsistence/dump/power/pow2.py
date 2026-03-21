from __future__ import annotations

from pathlib import Path

from PIL import Image


SCRIPT_DIR = Path(__file__).resolve().parent
VALID_EXTENSIONS = {".tga"}


def is_power_of_two(value: int) -> bool:
    if value <= 0:
        return False

    return (value & (value - 1)) == 0


def is_one_more_than_power_of_two(value: int) -> bool:
    if value <= 1:
        return False

    return is_power_of_two(value - 1)


def get_trimmed_size(width: int, height: int) -> tuple[int, int]:
    new_width = width - 1 if is_one_more_than_power_of_two(width) else width
    new_height = height - 1 if is_one_more_than_power_of_two(height) else height
    return new_width, new_height


def process_tga_file(file_path: Path) -> tuple[bool, bool]:
    try:
        with Image.open(file_path) as image:
            image.load()

            width, height = image.size
            new_width, new_height = get_trimmed_size(width, height)

            was_cropped = (new_width != width) or (new_height != height)

            if was_cropped:
                image = image.crop((0, 0, new_width, new_height))

            image.save(file_path)

        if was_cropped:
            trimmed_width = width != new_width
            trimmed_height = height != new_height

            if trimmed_width and trimmed_height:
                action = "trimmed rightmost column and bottom row"
            elif trimmed_width:
                action = "trimmed rightmost column"
            else:
                action = "trimmed bottom row"

            print(f"[FIXED] {file_path}")
            print(f"        {width}x{height} -> {new_width}x{new_height} ({action})")
        else:
            print(f"[RESAVED] {file_path}")
            print(f"          {width}x{height} unchanged")

        return True, was_cropped
    except Exception as exc:
        print(f"[ERROR] {file_path}")
        print(f"        {exc}")
        return False, False


def iter_tga_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in VALID_EXTENSIONS
    )


def main() -> None:
    files = iter_tga_files(SCRIPT_DIR)

    if not files:
        print("No TGA files found.")
        return

    print(f"Found {len(files)} TGA file(s).")
    print()

    processed_count = 0
    cropped_count = 0
    failed_count = 0

    for file_path in files:
        success, was_cropped = process_tga_file(file_path)

        if success:
            processed_count += 1
            if was_cropped:
                cropped_count += 1
        else:
            failed_count += 1

    print()
    print(f"Done. Successfully processed {processed_count} file(s).")
    print(f"Cropped {cropped_count} file(s).")
    print(f"Failed {failed_count} file(s).")


if __name__ == "__main__":
    main()