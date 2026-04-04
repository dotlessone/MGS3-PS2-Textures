from __future__ import annotations

import csv
import ctypes
import hashlib
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable

# ==========================================================
# CONFIG
# ==========================================================
ROOT_DIR = Path(__file__).resolve().parent

CSV_PATH = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_mc_dimensions_including_override_folders.csv"
)

MAX_WORKERS = max(4, os.cpu_count() or 4)
SHA1_BUFFER_SIZE = 8 * 1024 * 1024

SET_CTXR_TIMESTAMPS = True

# ==========================================================
# EXECUTABLE GUARD
# ==========================================================
TARGET_EXE = "METAL GEAR SOLID3.exe"


def validate_exe_presence(start: Path) -> None:
    candidates = [
        start,
        start.parent,
        start.parent.parent,
        start.parent.parent.parent,
    ]

    for path in candidates:
        if (path / TARGET_EXE).is_file():
            print(f"Found {TARGET_EXE} in: {path}")
            return

    print(f"[ERROR] {TARGET_EXE} not found within 3 parent levels. Aborting.")
    input("Press ENTER to exit...")
    sys.exit(1)


# ==========================================================
# WINDOWS FILETIME SETUP
# ==========================================================
if os.name != "nt":
    print("This script is Windows-only because it sets creation timestamps.")
    sys.exit(1)

kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

GENERIC_WRITE = 0x40000000
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
FILE_SHARE_DELETE = 0x00000004
OPEN_EXISTING = 3
FILE_ATTRIBUTE_NORMAL = 0x00000080
INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

EPOCH_AS_FILETIME = 116444736000000000
HUNDREDS_OF_NANOSECONDS = 10_000_000


class FILETIME(ctypes.Structure):
    _fields_ = [
        ("dwLowDateTime", ctypes.c_uint32),
        ("dwHighDateTime", ctypes.c_uint32),
    ]


kernel32.CreateFileW.argtypes = [
    ctypes.c_wchar_p,
    ctypes.c_uint32,
    ctypes.c_uint32,
    ctypes.c_void_p,
    ctypes.c_uint32,
    ctypes.c_uint32,
    ctypes.c_void_p,
]
kernel32.CreateFileW.restype = ctypes.c_void_p

kernel32.SetFileTime.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(FILETIME),
    ctypes.POINTER(FILETIME),
    ctypes.POINTER(FILETIME),
]
kernel32.SetFileTime.restype = ctypes.c_int

kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
kernel32.CloseHandle.restype = ctypes.c_int


# ==========================================================
# GLOBALS
# ==========================================================
print_lock = threading.Lock()
ctxr_matched_count = 0
ctxr_unmatched_count = 0
error_count = 0


# ==========================================================
# HELPERS
# ==========================================================
def filetime_from_unix(unix_time: int) -> FILETIME:
    value = int(unix_time) * HUNDREDS_OF_NANOSECONDS + EPOCH_AS_FILETIME
    return FILETIME(
        dwLowDateTime=value & 0xFFFFFFFF,
        dwHighDateTime=(value >> 32) & 0xFFFFFFFF,
    )


def set_creation_and_modified_time(path: Path, unix_time: int) -> None:
    ft = filetime_from_unix(unix_time)

    handle = kernel32.CreateFileW(
        str(path),
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        None,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        None,
    )

    if handle == INVALID_HANDLE_VALUE:
        raise ctypes.WinError(ctypes.get_last_error())

    try:
        if not kernel32.SetFileTime(handle, ctypes.byref(ft), ctypes.byref(ft), ctypes.byref(ft)):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        kernel32.CloseHandle(handle)

    os.utime(path, (unix_time, unix_time))


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(SHA1_BUFFER_SIZE), b""):
            h.update(chunk)

    return h.hexdigest()


def load_sha1_to_unix_time_map(csv_path: Path) -> Dict[str, int]:
    sha1_to_unix_times: Dict[str, set[int]] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            sha1_val = (row.get("mc_ctxr_sha1") or "").strip().lower()
            time_raw = (row.get("version_unix_time") or "").strip()

            if not sha1_val or not time_raw:
                continue

            try:
                t = int(time_raw)
            except ValueError:
                continue

            sha1_to_unix_times.setdefault(sha1_val, set()).add(t)

    result: Dict[str, int] = {}
    for sha1_val, times in sha1_to_unix_times.items():
        result[sha1_val] = min(times)

    return result


def find_files(root_dir: Path, suffix: str) -> list[Path]:
    return [p for p in root_dir.rglob(f"*{suffix}") if p.is_file()]


def process_ctxr(path: Path, sha1_map: Dict[str, int]) -> tuple[str, Path, str]:
    try:
        file_sha1 = sha1_of_file(path).lower()
        unix_time = sha1_map.get(file_sha1)

        if unix_time is None:
            return ("unmatched", path, file_sha1)

        set_creation_and_modified_time(path, unix_time)
        return ("matched", path, f"{file_sha1},{unix_time}")

    except Exception as e:
        return ("error", path, str(e))


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    global ctxr_matched_count
    global ctxr_unmatched_count
    global error_count

    validate_exe_presence(ROOT_DIR)

    print("Loading CSV...")
    sha1_map = load_sha1_to_unix_time_map(CSV_PATH)
    print(f"Loaded {len(sha1_map):,} entries\n")

    ctxr_files = find_files(ROOT_DIR, ".ctxr")
    total = len(ctxr_files)

    print(f"Found {total:,} CTXR files\n")

    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_ctxr, p, sha1_map) for p in ctxr_files]

        for f in as_completed(futures):
            status, path, detail = f.result()
            completed += 1

            with print_lock:
                if status == "matched":
                    ctxr_matched_count += 1
                    print(f"[{completed}/{total}] MATCH {path} -> {detail}")
                elif status == "unmatched":
                    ctxr_unmatched_count += 1
                    print(f"[{completed}/{total}] NO MATCH {path}")
                else:
                    error_count += 1
                    print(f"[{completed}/{total}] ERROR {path} -> {detail}")

    print("\nDone.")
    print(f"Matched: {ctxr_matched_count:,}")
    print(f"Unmatched: {ctxr_unmatched_count:,}")
    print(f"Errors: {error_count:,}")

    input("Press ENTER to exit...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())