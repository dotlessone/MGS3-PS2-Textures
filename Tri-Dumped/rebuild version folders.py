from __future__ import annotations

import csv
import ctypes
import hashlib
import os
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from ctypes import wintypes
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Set, Tuple


REQUIRED_COMPILATION_DATES = "Compilation Dates.csv"
MAX_WORKERS = max(4, os.cpu_count() or 4)
SHA1_BUFFER_SIZE = 8 * 1024 * 1024

METADATA_CSV = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_mc_tri_dumped_metadata.csv"
)

DIMENSIONS_CSV = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_mc_dimensions.csv"
)

STRCODE_MAPPING_CSV = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_img_strcode_mappings.csv"
)

BLASTLIST = {
    Path(r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection").resolve(),
    Path(r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\HD Collection").resolve(),
    Path(r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\todo").resolve(),
}

TEST_MODE = False
PRINT_MATCHES = True
PRINT_SKIPS = True
PROGRESS_BAR_WIDTH = 32


if os.name == "nt":
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    FILE_WRITE_ATTRIBUTES = 0x0100
    OPEN_EXISTING = 3
    FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
    INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

    class FILETIME(ctypes.Structure):
        _fields_ = [
            ("dwLowDateTime", wintypes.DWORD),
            ("dwHighDateTime", wintypes.DWORD),
        ]

    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = wintypes.HANDLE

    kernel32.SetFileTime.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
    ]
    kernel32.SetFileTime.restype = wintypes.BOOL

    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL


class ProgressTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.phase = ""
        self.total = 0
        self.completed = 0
        self.start_time = 0.0
        self.last_render_length = 0

    def start(self, phase: str, total: int) -> None:
        with self._lock:
            self.phase = phase
            self.total = max(0, total)
            self.completed = 0
            self.start_time = time.perf_counter()
            self.last_render_length = 0
            self._render_locked()

    def advance(self, amount: int = 1) -> None:
        with self._lock:
            self.completed += amount
            if self.completed > self.total:
                self.completed = self.total
            self._render_locked()

    def finish(self) -> None:
        with self._lock:
            self.completed = self.total
            self._render_locked()
            sys.stdout.write("\n")
            sys.stdout.flush()
            self.last_render_length = 0

    def _render_locked(self) -> None:
        if self.total <= 0:
            line = f"[{self.phase}] 0/0"
        else:
            fraction = self.completed / self.total
            filled = int(PROGRESS_BAR_WIDTH * fraction)
            if filled > PROGRESS_BAR_WIDTH:
                filled = PROGRESS_BAR_WIDTH

            bar = "#" * filled + "-" * (PROGRESS_BAR_WIDTH - filled)

            elapsed = max(0.0, time.perf_counter() - self.start_time)
            rate = self.completed / elapsed if elapsed > 0 and self.completed > 0 else 0.0

            if rate > 0.0 and self.completed < self.total:
                remaining = self.total - self.completed
                eta_seconds = remaining / rate
                eta_text = format_duration(eta_seconds)
            else:
                eta_text = "--:--:--"

            percent = fraction * 100.0
            line = (
                f"[{self.phase}] [{bar}] "
                f"{self.completed}/{self.total} "
                f"({percent:6.2f}%) "
                f"ETA {eta_text}"
            )

        pad = ""
        if len(line) < self.last_render_length:
            pad = " " * (self.last_render_length - len(line))

        sys.stdout.write("\r" + line + pad)
        sys.stdout.flush()
        self.last_render_length = len(line)


def format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(SHA1_BUFFER_SIZE), b""):
            h.update(chunk)

    return h.hexdigest().lower()


def parse_gmt_offset(offset_text: str) -> timezone:
    text = offset_text.strip()
    if len(text) != 6 or text[0] not in {"+", "-"} or text[3] != ":":
        raise ValueError(f"Invalid GMT_OFFSET format: {offset_text!r}")

    sign = 1 if text[0] == "+" else -1
    hours = int(text[1:3])
    minutes = int(text[4:6])

    delta = timedelta(hours=hours, minutes=minutes) * sign
    return timezone(delta)


def parse_compilation_datetime(date_text: str, time_text: str, gmt_offset_text: str) -> datetime:
    clean_time = time_text.strip()

    parsed_naive = None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed_naive = datetime.strptime(f"{date_text.strip()} {clean_time}", fmt)
            break
        except ValueError:
            continue

    if parsed_naive is None:
        raise ValueError(
            f"Invalid DATE/TIME combination: DATE={date_text!r}, TIME={time_text!r}"
        )

    tz = parse_gmt_offset(gmt_offset_text)
    return parsed_naive.replace(tzinfo=tz)


def datetime_to_unix_timestamp(dt: datetime) -> float:
    return dt.timestamp()


def unix_timestamp_to_filetime(timestamp: float) -> FILETIME:
    windows_epoch_offset = 11644473600
    value = int(round((timestamp + windows_epoch_offset) * 10_000_000))
    low = value & 0xFFFFFFFF
    high = (value >> 32) & 0xFFFFFFFF
    return FILETIME(low, high)


def set_creation_time_windows(path: Path, timestamp: float) -> None:
    if os.name != "nt":
        return

    handle = kernel32.CreateFileW(
        str(path),
        FILE_WRITE_ATTRIBUTES,
        0,
        None,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,
        None,
    )

    if handle == INVALID_HANDLE_VALUE:
        raise OSError(ctypes.get_last_error(), f"CreateFileW failed for {path}")

    try:
        filetime = unix_timestamp_to_filetime(timestamp)
        if not kernel32.SetFileTime(handle, ctypes.byref(filetime), None, None):
            raise OSError(ctypes.get_last_error(), f"SetFileTime failed for {path}")
    finally:
        kernel32.CloseHandle(handle)


def set_file_times(path: Path, timestamp: float) -> None:
    os.utime(path, (timestamp, timestamp))
    set_creation_time_windows(path, timestamp)


def load_compilation_region_timestamps(csv_path: Path) -> Dict[str, float]:
    out: Dict[str, float] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)

        expected_columns = {"REGION", "DATE", "TIME", "GMT_OFFSET"}
        if reader.fieldnames is None:
            raise RuntimeError(f"CSV has no header: {csv_path}")

        missing_columns = expected_columns - set(reader.fieldnames)
        if missing_columns:
            raise RuntimeError(
                f"CSV is missing required columns {sorted(missing_columns)}: {csv_path}"
            )

        for row_number, row in enumerate(reader, start=2):
            region = (row.get("REGION") or "").strip()
            date_value = (row.get("DATE") or "").strip()
            time_value = (row.get("TIME") or "").strip()
            gmt_offset = (row.get("GMT_OFFSET") or "").strip()

            if not region:
                raise RuntimeError(f"Blank REGION at {csv_path}:{row_number}")
            if not date_value:
                raise RuntimeError(f"Blank DATE for REGION '{region}' at {csv_path}:{row_number}")
            if not time_value:
                raise RuntimeError(f"Blank TIME for REGION '{region}' at {csv_path}:{row_number}")
            if not gmt_offset:
                raise RuntimeError(f"Blank GMT_OFFSET for REGION '{region}' at {csv_path}:{row_number}")

            if region in out:
                raise RuntimeError(f"Duplicate REGION '{region}' in {csv_path}:{row_number}")

            dt = parse_compilation_datetime(date_value, time_value, gmt_offset)
            out[region] = datetime_to_unix_timestamp(dt)

    if not out:
        raise RuntimeError(f"No regions found in: {csv_path}")

    return out


def discover_region_folders(game_folder: Path) -> Tuple[Path, List[str], List[Tuple[Path, float]]]:
    errors: List[str] = []
    region_folders: List[Tuple[Path, float]] = []

    compilation_dates_path = game_folder / REQUIRED_COMPILATION_DATES
    if not compilation_dates_path.is_file():
        errors.append(f"Missing required file: {compilation_dates_path}")
        return game_folder, errors, region_folders

    try:
        region_timestamps = load_compilation_region_timestamps(compilation_dates_path)
    except Exception as exc:
        errors.append(f"Failed to parse {compilation_dates_path}: {exc}")
        return game_folder, errors, region_folders

    csv_regions = list(region_timestamps.keys())
    csv_region_set = set(csv_regions)

    actual_region_folders = sorted(
        path for path in game_folder.iterdir()
        if path.is_dir()
    )
    actual_region_names = {path.name for path in actual_region_folders}

    missing_folders = sorted(csv_region_set - actual_region_names)
    extra_folders = sorted(actual_region_names - csv_region_set)

    for region in missing_folders:
        errors.append(f"Missing required region folder: {game_folder / region}")

    for region in extra_folders:
        errors.append(f"Region folder not present in {compilation_dates_path.name}: {game_folder / region}")

    if errors:
        return game_folder, errors, region_folders

    region_folders = [
        (game_folder / region, region_timestamps[region])
        for region in csv_regions
    ]
    return game_folder, errors, region_folders


def load_metadata(csv_path: Path) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames is None:
            raise RuntimeError(f"No header in {csv_path}")

        required_columns = {
            "texture_name",
            "mc_tri_dumped_sha1",
            "mc_tri_dumped_alpha_levels",
            "mc_tri_dumped_width",
            "mc_tri_height",
            "mc_tri_width_ciel2",
            "mc_tri_height_ciel2",
        }
        missing_columns = required_columns - set(reader.fieldnames)
        if missing_columns:
            raise RuntimeError(
                f"Metadata CSV is missing required columns {sorted(missing_columns)}: {csv_path}"
            )

        for row_number, row in enumerate(reader, start=2):
            texture_name = (row.get("texture_name") or "").strip()
            sha1 = (row.get("mc_tri_dumped_sha1") or "").strip().lower()

            if not texture_name and not sha1:
                continue

            if not texture_name:
                raise RuntimeError(f"Blank texture_name at {csv_path}:{row_number}")

            if not sha1:
                raise RuntimeError(
                    f"Blank mc_tri_dumped_sha1 for '{texture_name}' at {csv_path}:{row_number}"
                )

            if len(sha1) != 40 or any(ch not in "0123456789abcdef" for ch in sha1):
                raise RuntimeError(
                    f"Invalid mc_tri_dumped_sha1 for '{texture_name}' at {csv_path}:{row_number}: {sha1}"
                )

            out.setdefault(sha1, []).append(texture_name)

    if not out:
        raise RuntimeError(f"No metadata rows found in: {csv_path}")

    return out


def load_dimensions_texture_names(csv_path: Path) -> Set[str]:
    out: Set[str] = set()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames is None:
            raise RuntimeError(f"No header in {csv_path}")

        required_columns = {"texture_name"}
        missing_columns = required_columns - set(reader.fieldnames)
        if missing_columns:
            raise RuntimeError(
                f"Dimensions CSV is missing required columns {sorted(missing_columns)}: {csv_path}"
            )

        for row_number, row in enumerate(reader, start=2):
            texture_name = (row.get("texture_name") or "").strip()

            if not texture_name:
                raise RuntimeError(f"Blank texture_name at {csv_path}:{row_number}")

            out.add(texture_name)

    if not out:
        raise RuntimeError(f"No texture_name rows found in: {csv_path}")

    return out


def load_strcode_texture_stems(csv_path: Path) -> Set[str]:
    out: Set[str] = set()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames is None:
            raise RuntimeError(f"No header in {csv_path}")

        required_columns = {"texture_stem"}
        missing_columns = required_columns - set(reader.fieldnames)
        if missing_columns:
            raise RuntimeError(
                f"Strcode mapping CSV is missing required columns {sorted(missing_columns)}: {csv_path}"
            )

        for row_number, row in enumerate(reader, start=2):
            stem = (row.get("texture_stem") or "").strip()

            if not stem:
                raise RuntimeError(f"Blank texture_stem at {csv_path}:{row_number}")

            out.add(stem)

    return out


def find_pngs_under_region_subfolders(region_folder: Path) -> List[Path]:
    pngs: List[Path] = []

    for path in region_folder.rglob("*.png"):
        if not path.is_file():
            continue

        try:
            relative_parts = path.relative_to(region_folder).parts
        except ValueError:
            continue

        if len(relative_parts) < 2:
            continue

        pngs.append(path)

    pngs.sort()
    return pngs


def build_sha1_index_for_region(
    png_paths: List[Path],
    progress: ProgressTracker,
) -> Tuple[Dict[str, List[Path]], List[str]]:
    sha1_to_paths: Dict[str, List[Path]] = {}
    errors: List[str] = []

    if not png_paths:
        return sha1_to_paths, errors

    progress.start("Hashing PNGs", len(png_paths))

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(png_paths))) as executor:
        future_map = {
            executor.submit(sha1_of_file, path): path
            for path in png_paths
        }

        for future in as_completed(future_map):
            path = future_map[future]

            try:
                sha1 = future.result()
            except Exception as exc:
                errors.append(f"Failed to hash {path}: {exc}")
                progress.advance()
                continue

            sha1_to_paths.setdefault(sha1, []).append(path)
            progress.advance()

    progress.finish()
    return sha1_to_paths, errors


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_missing_dimensions_report(
    region_folder: Path,
    dimensions_texture_names: Set[str],
    strcode_texture_stems: Set[str],
) -> Tuple[str, int]:
    root_png_stems = {
        path.stem
        for path in region_folder.glob("*.png")
        if path.is_file()
    }

    missing_names = sorted(
        texture_name
        for texture_name in dimensions_texture_names
        if texture_name not in root_png_stems
        and texture_name not in strcode_texture_stems
    )

    report_path = region_folder / "missing_from_region_folder.txt"

    with report_path.open("w", encoding="utf-8", newline="\n") as handle:
        for name in missing_names:
            handle.write(f"{name}\n")

    return str(report_path), len(missing_names)


def remove_empty_directories(root: Path) -> Tuple[int, List[str], List[str]]:
    removed = 0
    logs: List[str] = []
    errors: List[str] = []

    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        path = Path(dirpath)

        if path == root:
            continue

        try:
            if not any(path.iterdir()):
                path.rmdir()
                removed += 1
                logs.append(f"[RMDIR] Removed empty folder: {path}")
        except Exception as exc:
            errors.append(f"Failed to remove empty folder {path}: {exc}")

    return removed, logs, errors


def process_region_folder(
    region_folder: Path,
    region_timestamp: float,
    metadata_map: Dict[str, List[str]],
    dimensions_texture_names: Set[str],
    strcode_texture_stems: Set[str],
    progress: ProgressTracker,
) -> Tuple[List[str], List[str]]:
    logs: List[str] = []
    errors: List[str] = []

    pngs = find_pngs_under_region_subfolders(region_folder)
    logs.append(f"[SCAN] {region_folder} -> {len(pngs)} recursive subfolder PNG(s) found")

    sha1_to_paths, hash_errors = build_sha1_index_for_region(pngs, progress)
    errors.extend(hash_errors)

    if hash_errors:
        return logs, errors

    matched_sha1_count = 0
    matched_file_count = 0
    completed_copies = 0
    deleted_sources = 0

    sha1_items = sorted(sha1_to_paths.items())
    progress.start("Processing SHA1s", len(sha1_items))

    for sha1, source_paths in sha1_items:
        texture_names = metadata_map.get(sha1)
        if texture_names is None:
            progress.advance()
            continue

        matched_sha1_count += 1
        matched_file_count += len(source_paths)

        source_paths = sorted(source_paths)
        primary_source = source_paths[0]
        all_targets_ok = True

        if len(source_paths) > 1:
            logs.append(
                f"[DUPLICATE SOURCE SHA1] {sha1} -> using {primary_source} as copy source, "
                f"will delete {len(source_paths)} matched duplicate source file(s)"
            )

        for texture_name in texture_names:
            dest_path = region_folder / f"{texture_name}.png"

            if primary_source.resolve() == dest_path.resolve():
                if PRINT_SKIPS:
                    logs.append(f"[SKIP] Already in place: {primary_source}")
                continue

            if dest_path.exists():
                if dest_path.is_dir():
                    errors.append(f"Destination exists as a directory: {dest_path}")
                    all_targets_ok = False
                    continue

                dest_sha1 = sha1_of_file(dest_path)
                if dest_sha1 != sha1:
                    errors.append(
                        f"Destination already exists with different contents: "
                        f"{dest_path} (dest_sha1={dest_sha1}, expected={sha1})"
                    )
                    all_targets_ok = False
                    continue

                if PRINT_SKIPS:
                    logs.append(
                        f"[SKIP] Destination already exists with matching contents: "
                        f"{primary_source} -> {dest_path}"
                    )
                continue

            if PRINT_MATCHES:
                logs.append(
                    f"[MATCH] {primary_source} -> {dest_path} "
                    f"(sha1={sha1}, texture_name={texture_name})"
                )

            try:
                ensure_parent_dir(dest_path)
                shutil.copy2(str(primary_source), str(dest_path))
                set_file_times(dest_path, region_timestamp)
                completed_copies += 1
            except Exception as exc:
                errors.append(f"Failed to copy/set dates {primary_source} -> {dest_path}: {exc}")
                all_targets_ok = False

        if all_targets_ok:
            for source_path in source_paths:
                try:
                    source_path.unlink()
                    deleted_sources += 1
                    logs.append(f"[DELETE] Removed matched source: {source_path}")
                except Exception as exc:
                    errors.append(f"Failed to delete source {source_path}: {exc}")

        progress.advance()

    progress.finish()

    removed_dirs, dir_logs, dir_errors = remove_empty_directories(region_folder)
    logs.extend(dir_logs)
    errors.extend(dir_errors)
    logs.append(f"[CLEANUP] Removed {removed_dirs} empty folder(s)")

    report_path, missing_count = write_missing_dimensions_report(
        region_folder=region_folder,
        dimensions_texture_names=dimensions_texture_names,
        strcode_texture_stems=strcode_texture_stems,
    )
    logs.append(
        f"[MISSING FROM REGION] Wrote {missing_count} texture name(s) missing from the region folder to: {report_path}"
    )

    logs.append(
        f"[SUMMARY] {region_folder} -> "
        f"{matched_sha1_count} matched SHA1(s), "
        f"{matched_file_count} matched source file(s), "
        f"{completed_copies} completed cop(y/ies), "
        f"{deleted_sources} deleted source file(s), "
        f"{removed_dirs} empty folder(s) removed"
    )

    return logs, errors


def main() -> int:
    script_dir = Path(__file__).resolve().parent

    if not METADATA_CSV.is_file():
        raise FileNotFoundError(f"Metadata CSV not found: {METADATA_CSV}")

    if not DIMENSIONS_CSV.is_file():
        raise FileNotFoundError(f"Dimensions CSV not found: {DIMENSIONS_CSV}")

    if not STRCODE_MAPPING_CSV.is_file():
        raise FileNotFoundError(f"Strcode mapping CSV not found: {STRCODE_MAPPING_CSV}")

    all_game_folders = sorted(
        path.resolve()
        for path in script_dir.iterdir()
        if path.is_dir()
    )

    game_folders = [
        path for path in all_game_folders
        if path not in BLASTLIST
    ]

    if not game_folders:
        raise RuntimeError(f"No non-blastlisted subfolders found in: {script_dir}")

    discovery_errors: Dict[Path, List[str]] = {}
    all_region_folders: List[Tuple[Path, float]] = []

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(game_folders))) as executor:
        futures = [executor.submit(discover_region_folders, game_folder) for game_folder in game_folders]

        for future in as_completed(futures):
            game_folder, errors, region_folders = future.result()

            if errors:
                discovery_errors[game_folder] = errors
                continue

            all_region_folders.extend(region_folders)

    if discovery_errors:
        lines: List[str] = ["Validation failed."]
        for game_folder in sorted(discovery_errors):
            lines.append("")
            lines.append(f"[{game_folder}]")
            lines.extend(discovery_errors[game_folder])
        raise RuntimeError("\n".join(lines))

    if not all_region_folders:
        raise RuntimeError("No valid region folders discovered.")

    metadata_map = load_metadata(METADATA_CSV)
    dimensions_texture_names = load_dimensions_texture_names(DIMENSIONS_CSV)
    strcode_texture_stems = load_strcode_texture_stems(STRCODE_MAPPING_CSV)

    all_region_folders.sort(key=lambda item: str(item[0]).lower())

    if TEST_MODE:
        all_region_folders = all_region_folders[:1]
        print(f"[TEST MODE] Only processing first region folder: {all_region_folders[0][0]}")

    all_logs: List[str] = []
    all_errors: List[str] = []
    progress = ProgressTracker()

    for region_folder, region_timestamp in all_region_folders:
        logs, errors = process_region_folder(
            region_folder=region_folder,
            region_timestamp=region_timestamp,
            metadata_map=metadata_map,
            dimensions_texture_names=dimensions_texture_names,
            strcode_texture_stems=strcode_texture_stems,
            progress=progress,
        )
        all_logs.extend(logs)
        all_errors.extend(errors)

    for line in all_logs:
        print(line)

    if all_errors:
        print()
        print("ERRORS:")
        for line in all_errors:
            print(line)
        raise RuntimeError(f"Encountered {len(all_errors)} error(s).")

    print()
    if TEST_MODE:
        print("Test mode complete. One region folder processed.")
    else:
        print("Done.")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise