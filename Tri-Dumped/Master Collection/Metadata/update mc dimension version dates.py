from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Dict, List, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent

VERSION_DATES_CSV = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_ps2_sha1_version_dates.csv"
)

TARGET_CSVS = [
    SCRIPT_DIR / "mgs3_mc_dimensions.csv",
    SCRIPT_DIR / "mgs3_mc_dimensions_including_override_folders.csv",
]


def pause_and_exit(code: int = 0) -> None:
    try:
        input("Press ENTER to exit...")
    except EOFError:
        pass

    raise SystemExit(code)


def load_version_dates(csv_path: Path) -> Dict[str, Tuple[str, str]]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Version dates CSV not found: {csv_path}")

    version_map: Dict[str, Tuple[str, str]] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)

        required_columns = {"sha1", "game", "version", "first_seen_unix"}
        missing_columns = required_columns.difference(reader.fieldnames or set())
        if missing_columns:
            raise ValueError(
                f"Version dates CSV is missing required columns: {sorted(missing_columns)}"
            )

        for row in reader:
            sha1_value = (row.get("sha1") or "").strip().lower()
            game = (row.get("game") or "").strip()
            version = (row.get("version") or "").strip()
            first_seen_unix = (row.get("first_seen_unix") or "").strip()

            if not sha1_value:
                continue

            origin_version = f"{game} {version}".strip()
            version_map[sha1_value] = (origin_version, first_seen_unix)

    return version_map


def update_target_csv(
    csv_path: Path,
    version_map: Dict[str, Tuple[str, str]],
) -> Tuple[int, int]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Target CSV not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])

        required_columns = {
            "mc_resaved_sha1",
            "origin_version",
            "version_unix_time",
        }
        missing_columns = required_columns.difference(fieldnames)
        if missing_columns:
            raise ValueError(
                f"Target CSV is missing required columns: {sorted(missing_columns)}"
            )

        rows: List[dict[str, str]] = []
        updated_count = 0
        matched_count = 0

        for row in reader:
            sha1_value = (row.get("mc_resaved_sha1") or "").strip().lower()

            if sha1_value:
                version_info = version_map.get(sha1_value)
                if version_info is not None:
                    matched_count += 1

                    new_origin_version, new_version_unix_time = version_info
                    old_origin_version = row.get("origin_version") or ""
                    old_version_unix_time = row.get("version_unix_time") or ""

                    if (
                        old_origin_version != new_origin_version
                        or old_version_unix_time != new_version_unix_time
                    ):
                        row["origin_version"] = new_origin_version
                        row["version_unix_time"] = new_version_unix_time
                        updated_count += 1

            rows.append(row)

    temp_path = csv_path.with_name(csv_path.name + ".tmp")

    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    temp_path.replace(csv_path)
    return matched_count, updated_count


def main() -> int:
    try:
        version_map = load_version_dates(VERSION_DATES_CSV)
    except Exception as exc:
        print(f"ERROR loading version map: {exc}")
        return 1

    print(f"Loaded {len(version_map)} SHA1 mappings from:")
    print(f"  {VERSION_DATES_CSV}")
    print()

    had_error = False

    for csv_path in TARGET_CSVS:
        try:
            matched_count, updated_count = update_target_csv(csv_path, version_map)
            print(csv_path)
            print(f"  SHA1 matches found: {matched_count}")
            print(f"  Rows updated: {updated_count}")
            print()
        except Exception as exc:
            had_error = True
            print(csv_path)
            print(f"  ERROR: {exc}")
            print()

    if had_error:
        print("Completed with errors.")
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    exit_code = main()

    if exit_code != 0:
        pause_and_exit(exit_code)