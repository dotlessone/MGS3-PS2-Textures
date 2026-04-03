import csv
import ctypes
import hashlib
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ==========================================================
# CONFIG
# ==========================================================
ROOT_DIR = Path(__file__).parent.resolve()
TARGET_FILENAME = "Compilation Dates.csv"
MAX_WORKERS = os.cpu_count() or 8

SKIP_FOLDERS = {
    "master collection",
    # add more here
}

# True = only process the first region job that needs outputs
# False = process all region jobs
TEST_MODE = False

HASH_CHUNK_SIZE = 1024 * 1024 * 8
PROGRESS_BAR_WIDTH = 32

# ==========================================================
# GLOBALS
# ==========================================================
print_lock = threading.Lock()
found_compilation_csvs = []
errors = []

last_progress_update = 0.0

# ==========================================================
# WINDOWS FILETIME HELPERS
# ==========================================================
if os.name == "nt":
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
    FILE_WRITE_ATTRIBUTES = 0x0100
    FILE_SHARE_READ = 0x00000001
    FILE_SHARE_WRITE = 0x00000002
    FILE_SHARE_DELETE = 0x00000004
    OPEN_EXISTING = 3
    FILE_FLAG_BACKUP_SEMANTICS = 0x02000000

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    class FILETIME(ctypes.Structure):
        _fields_ = [
            ("dwLowDateTime", ctypes.c_uint32),
            ("dwHighDateTime", ctypes.c_uint32),
        ]

    CreateFileW = kernel32.CreateFileW
    CreateFileW.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    CreateFileW.restype = ctypes.c_void_p

    SetFileTime = kernel32.SetFileTime
    SetFileTime.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
    ]
    SetFileTime.restype = ctypes.c_int

    CloseHandle = kernel32.CloseHandle
    CloseHandle.argtypes = [ctypes.c_void_p]
    CloseHandle.restype = ctypes.c_int


# ==========================================================
# HELPERS
# ==========================================================
def fail(message: str):
    with print_lock:
        errors.append(message)
        print(f"\n[ERROR] {message}")


def info(message: str):
    with print_lock:
        print(message)


def normalize_name(name: str) -> str:
    return name.strip().lower()


def sha1_file(path: Path) -> str:
    hasher = hashlib.sha1()

    with path.open("rb") as f:
        while True:
            chunk = f.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)

    return hasher.hexdigest()


def parse_gmt_offset(offset_text: str) -> timezone:
    value = offset_text.strip()
    if len(value) != 6 or value[0] not in "+-" or value[3] != ":":
        raise ValueError(f"Invalid GMT_OFFSET: {offset_text}")

    sign = 1 if value[0] == "+" else -1
    hours = int(value[1:3])
    minutes = int(value[4:6])

    if hours > 23 or minutes > 59:
        raise ValueError(f"Invalid GMT_OFFSET: {offset_text}")

    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def parse_region_timestamp(date_text: str, time_text: str, gmt_offset_text: str) -> datetime:
    clean_time = time_text.strip()
    if "." in clean_time:
        clean_time = clean_time.split(".", 1)[0]

    dt = datetime.strptime(f"{date_text.strip()} {clean_time}", "%Y-%m-%d %H:%M:%S")
    tz = parse_gmt_offset(gmt_offset_text)
    return dt.replace(tzinfo=tz)


def datetime_to_epoch_seconds(dt: datetime) -> float:
    return dt.timestamp()


def epoch_seconds_to_filetime(epoch_seconds: float) -> int:
    return int((epoch_seconds + 11644473600) * 10000000)


def set_creation_time_windows(path: Path, epoch_seconds: float):
    handle = CreateFileW(
        str(path),
        FILE_WRITE_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        None,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,
        None,
    )

    if handle == INVALID_HANDLE_VALUE:
        raise ctypes.WinError(ctypes.get_last_error())

    try:
        ft_value = epoch_seconds_to_filetime(epoch_seconds)
        creation_time = FILETIME(
            dwLowDateTime=ft_value & 0xFFFFFFFF,
            dwHighDateTime=(ft_value >> 32) & 0xFFFFFFFF,
        )

        if not SetFileTime(handle, ctypes.byref(creation_time), None, None):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        CloseHandle(handle)


def set_file_times(path: Path, epoch_seconds: float):
    os.utime(path, (epoch_seconds, epoch_seconds))

    if os.name == "nt":
        set_creation_time_windows(path, epoch_seconds)


def read_csv_regions(csv_path: Path) -> dict[str, dict[str, str]]:
    regions: dict[str, dict[str, str]] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        expected_fields = ["REGION", "DATE", "TIME", "GMT_OFFSET"]
        if reader.fieldnames != expected_fields:
            raise ValueError(f"{csv_path}: expected header {expected_fields}, got {reader.fieldnames}")

        for row_num, row in enumerate(reader, start=2):
            raw_region = (row.get("REGION") or "").strip()
            date_value = (row.get("DATE") or "").strip()
            time_value = (row.get("TIME") or "").strip()
            gmt_offset = (row.get("GMT_OFFSET") or "").strip()

            if not raw_region:
                raise ValueError(f"{csv_path}: blank REGION at row {row_num}")

            if "," in raw_region:
                raise ValueError(f"{csv_path}: REGION contains comma at row {row_num}: {raw_region}")

            if not date_value:
                raise ValueError(f"{csv_path}: blank DATE at row {row_num}")

            if not time_value:
                raise ValueError(f"{csv_path}: blank TIME at row {row_num}")

            if not gmt_offset:
                raise ValueError(f"{csv_path}: blank GMT_OFFSET at row {row_num}")

            parse_region_timestamp(date_value, time_value, gmt_offset)

            key = normalize_name(raw_region)

            if key in regions:
                raise ValueError(f"{csv_path}: duplicate REGION at row {row_num}: {raw_region}")

            regions[key] = {
                "REGION": raw_region,
                "DATE": date_value,
                "TIME": time_value,
                "GMT_OFFSET": gmt_offset,
            }

    return regions


def get_immediate_subfolders(folder: Path) -> dict[str, Path]:
    subfolders: dict[str, Path] = {}

    for child in folder.iterdir():
        if not child.is_dir():
            continue

        if "," in child.name:
            raise ValueError(f"{folder}: comma in folder name: {child.name}")

        key = normalize_name(child.name)

        if key in subfolders:
            raise ValueError(f"{folder}: duplicate folder after normalization: {child.name}")

        subfolders[key] = child

    return subfolders


def find_pngs(region_folder: Path) -> list[Path]:
    return [
        p for p in region_folder.rglob("*")
        if p.is_file() and p.suffix.lower() == ".png"
    ]


def render_progress(prefix: str, completed: int, total: int, start_time: float):
    global last_progress_update

    if total <= 0:
        return

    now = time.perf_counter()

    # Only update once per second OR if we're finished
    if completed != total and (now - last_progress_update) < 1.0:
        return

    last_progress_update = now

    elapsed = max(now - start_time, 0.000001)
    rate = completed / elapsed if completed > 0 else 0.0
    remaining = total - completed
    eta_seconds = remaining / rate if rate > 0 else 0.0
    percent = completed / total

    filled = int(PROGRESS_BAR_WIDTH * percent)
    bar = "#" * filled + "-" * (PROGRESS_BAR_WIDTH - filled)

    eta_text = format_eta(eta_seconds)
    line = (
        f"\r{prefix} [{bar}] "
        f"{completed}/{total} "
        f"({percent * 100:6.2f}%) "
        f"ETA: {eta_text}"
    )

    with print_lock:
        sys.stdout.write(line)
        sys.stdout.flush()


def finish_progress():
    with print_lock:
        sys.stdout.write("\n")
        sys.stdout.flush()


def format_eta(seconds: float) -> str:
    total_seconds = max(int(round(seconds)), 0)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    if hours > 0:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"

    return f"{minutes:02d}:{secs:02d}"


def build_row_and_set_times(
    region_folder: Path,
    png_path: Path,
    game_version: str,
    region: str,
    epoch_seconds: float,
) -> dict[str, str]:
    set_file_times(png_path, epoch_seconds)
    sha1 = sha1_file(png_path)

    rel = png_path.relative_to(region_folder)
    parent = rel.parent

    return {
        "sha1": sha1,
        "game_version": game_version,
        "region": region,
        "relative_path_from_region_folder": "" if str(parent) == "." else parent.as_posix(),
        "texture_strcode": png_path.stem,
    }


def get_region_output_paths(game_folder: Path, region_folder: Path) -> tuple[Path, Path]:
    base_name = f"{game_folder.name}_{region_folder.name}"
    csv_path = region_folder / f"{base_name}_FULL_FILE_STRUCTURE.csv"
    txt_path = region_folder / f"{base_name}_ALL_SHA1s.txt"
    return csv_path, txt_path


def get_missing_jobs(
    game_folder: Path,
    csv_regions: dict[str, dict[str, str]],
    subfolders: dict[str, Path],
) -> list[tuple[Path, Path, Path, Path, dict[str, str]]]:
    jobs = []

    for region_key, region_row in csv_regions.items():
        region_folder = subfolders.get(region_key)
        if region_folder is None:
            continue

        csv_output_path, txt_output_path = get_region_output_paths(game_folder, region_folder)

        if csv_output_path.exists() and txt_output_path.exists():
            continue

        jobs.append((game_folder, region_folder, csv_output_path, txt_output_path, region_row))

    return jobs


def write_full_file_structure_csv(csv_path: Path, rows: list[dict[str, str]]):
    if csv_path.exists():
        raise RuntimeError(f"Refusing to overwrite existing file: {csv_path}")

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "sha1",
                "game_version",
                "region",
                "relative_path_from_region_folder",
                "texture_strcode",
            ],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def write_all_sha1s_txt(txt_path: Path, sha1_values: set[str]):
    if txt_path.exists():
        raise RuntimeError(f"Refusing to overwrite existing file: {txt_path}")

    with txt_path.open("w", encoding="utf-8", newline="") as f:
        for sha1_value in sorted(sha1_values):
            f.write(sha1_value + "\n")


def generate_outputs(
    game_folder: Path,
    region_folder: Path,
    csv_output_path: Path,
    txt_output_path: Path,
    region_row: dict[str, str],
):
    if csv_output_path.exists() and txt_output_path.exists():
        info(f"[SKIP] Both outputs already exist: {region_folder}")
        return

    region_timestamp = parse_region_timestamp(
        region_row["DATE"],
        region_row["TIME"],
        region_row["GMT_OFFSET"],
    )
    epoch_seconds = datetime_to_epoch_seconds(region_timestamp)

    pngs = find_pngs(region_folder)

    info(
        f"[BUILD] {region_folder} -> "
        f"{csv_output_path.name if not csv_output_path.exists() else '[exists]'} | "
        f"{txt_output_path.name if not txt_output_path.exists() else '[exists]'} "
        f"({len(pngs)} PNGs)"
    )

    rows = []
    sha1_set = set()
    start_time = time.perf_counter()
    prefix = f"[PROCESS] {game_folder.name}/{region_folder.name}"

    if pngs:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    build_row_and_set_times,
                    region_folder,
                    png_path,
                    game_folder.name,
                    region_folder.name,
                    epoch_seconds,
                ): png_path
                for png_path in pngs
            }

            completed = 0

            for future in as_completed(futures):
                png_path = futures[future]

                try:
                    row = future.result()
                except Exception as e:
                    finish_progress()
                    raise RuntimeError(f"{region_folder}: failed processing {png_path} -> {e}") from e

                rows.append(row)
                sha1_set.add(row["sha1"])
                completed += 1
                render_progress(prefix, completed, len(pngs), start_time)

        finish_progress()
    else:
        info(f"[PROCESS] {game_folder.name}/{region_folder.name} [no PNGs found]")

    rows.sort(
        key=lambda r: (
            r["relative_path_from_region_folder"].lower(),
            r["texture_strcode"].lower(),
        )
    )

    if not csv_output_path.exists():
        write_full_file_structure_csv(csv_output_path, rows)
        info(f"[WROTE] {csv_output_path}")

    if not txt_output_path.exists():
        write_all_sha1s_txt(txt_output_path, sha1_set)
        info(f"[WROTE] {txt_output_path}")


# ==========================================================
# WORKER
# ==========================================================
def process_folder(folder: Path) -> list[tuple[Path, Path, Path, Path, dict[str, str]]]:
    jobs = []

    try:
        compilation_csv_path = folder / TARGET_FILENAME
        if not compilation_csv_path.is_file():
            return jobs

        with print_lock:
            found_compilation_csvs.append(compilation_csv_path)
            print(f"[FOUND] {compilation_csv_path}")

        regions = read_csv_regions(compilation_csv_path)
        subfolders = get_immediate_subfolders(folder)

        missing_disk = []
        missing_csv = []

        for region_key, region_row in regions.items():
            if region_key not in subfolders:
                missing_disk.append(region_row["REGION"])

        for subfolder_key, subfolder_path in subfolders.items():
            if subfolder_key not in regions:
                missing_csv.append(subfolder_path.name)

        if missing_disk:
            fail(f"{folder}: CSV region(s) missing matching subfolder(s): {', '.join(sorted(missing_disk, key=str.lower))}")

        if missing_csv:
            fail(f"{folder}: subfolder(s) missing from CSV REGION column: {', '.join(sorted(missing_csv, key=str.lower))}")

        if missing_disk or missing_csv:
            return []

        with print_lock:
            print(f"[OK] {folder} (Expected Versions: {len(regions)} | Found Versions: {len(subfolders)})")

        jobs.extend(get_missing_jobs(folder, regions, subfolders))

    except Exception as e:
        fail(f"{folder} -> {e}")

    return jobs


# ==========================================================
# MAIN
# ==========================================================
def main():
    folders = [
        p for p in ROOT_DIR.iterdir()
        if p.is_dir() and normalize_name(p.name) not in SKIP_FOLDERS
    ]

    print(f"Scanning {len(folders)} folders...\n")

    jobs: list[tuple[Path, Path, Path, Path, dict[str, str]]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_folder, folder) for folder in folders]

        for future in as_completed(futures):
            try:
                jobs.extend(future.result())
            except Exception as e:
                fail(f"Unhandled worker error -> {e}")

    if errors:
        print("\n[ABORT] Structural validation failed. Fix errors first.")
        sys.exit(1)

    if jobs:
        print(f"\nMissing outputs: {len(jobs)}")

        if TEST_MODE:
            jobs = jobs[:1]
            print("[TEST MODE] Processing one region only.\n")
        else:
            print("[LIVE MODE] Processing all missing regions.\n")

        for game_folder, region_folder, csv_output_path, txt_output_path, region_row in jobs:
            try:
                generate_outputs(
                    game_folder,
                    region_folder,
                    csv_output_path,
                    txt_output_path,
                    region_row,
                )
            except Exception as e:
                fail(f"{region_folder} -> {e}")

    else:
        print("\nNo missing outputs.")

    print("\nDone.")
    print(f"Found {len(found_compilation_csvs)} '{TARGET_FILENAME}' file(s).")
    print(f"Errors: {len(errors)}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()