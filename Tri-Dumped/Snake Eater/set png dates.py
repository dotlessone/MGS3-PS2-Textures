import os
import sys
import ctypes
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================================
# CONFIG
# ==========================================================
ROOT_DIR = Path(__file__).resolve().parent
MAX_WORKERS = min(32, (os.cpu_count() or 1) * 2)

# 2005-12-01 12:52:46 GMT+09:00
TARGET_DT = datetime(2005, 12, 1, 12, 52, 46, tzinfo=timezone(timedelta(hours=9)))
TARGET_TS = TARGET_DT.timestamp()

# Windows constants
FILE_WRITE_ATTRIBUTES = 0x0100
OPEN_EXISTING = 3
FILE_FLAG_BACKUP_SEMANTICS = 0x02000000

INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

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
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.c_void_p,
]
SetFileTime.restype = ctypes.c_int

CloseHandle = kernel32.CloseHandle
CloseHandle.argtypes = [ctypes.c_void_p]
CloseHandle.restype = ctypes.c_int


class FILETIME(ctypes.Structure):
    _fields_ = [
        ("dwLowDateTime", ctypes.c_uint32),
        ("dwHighDateTime", ctypes.c_uint32),
    ]


def unix_timestamp_to_filetime(unix_ts: float) -> FILETIME:
    ft = int((unix_ts + 11644473600) * 10000000)
    return FILETIME(
        dwLowDateTime=ft & 0xFFFFFFFF,
        dwHighDateTime=(ft >> 32) & 0xFFFFFFFF,
    )


def set_creation_time(path: Path, unix_ts: float) -> None:
    handle = CreateFileW(
        str(path),
        FILE_WRITE_ATTRIBUTES,
        0,
        None,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,
        None,
    )

    if handle == INVALID_HANDLE_VALUE:
        raise ctypes.WinError(ctypes.get_last_error())

    try:
        ft = unix_timestamp_to_filetime(unix_ts)
        result = SetFileTime(handle, ctypes.byref(ft), None, None)
        if result == 0:
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        CloseHandle(handle)


def set_times(path: Path) -> tuple[bool, str]:
    try:
        os.utime(path, (TARGET_TS, TARGET_TS))
        set_creation_time(path, TARGET_TS)
        return True, str(path)
    except Exception as e:
        return False, f"{path} -> {e}"


def gather_pngs(root: Path) -> list[Path]:
    return [p for p in root.rglob("*.png") if p.is_file()]


def main() -> int:
    if os.name != "nt":
        print("Error: this script only supports setting creation time on Windows.")
        return 1

    png_files = gather_pngs(ROOT_DIR)
    total = len(png_files)

    if total == 0:
        print("No PNG files found.")
        return 0

    print(f"Found {total} PNG files.")
    print(f"Setting creation, access, and modified time to {TARGET_DT.isoformat()}")

    error_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(set_times, path) for path in png_files]

        for index, future in enumerate(as_completed(futures), 1):
            ok, message = future.result()
            if not ok:
                error_count += 1
                print(f"[ERROR] {message}")

            if index % 500 == 0 or index == total:
                print(f"Progress: {index}/{total}")

    print()
    print(f"Done. Processed: {total}, Errors: {error_count}")
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())