import os
import re
import csv
import hashlib
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image

# ==========================================================
# CONFIG
# ==========================================================
ROOT_DIR = Path(r"C:\Development\Git\MGS3-PS2-Textures\PCSX2 Dumped\d - subsistence")

MC_CSV = Path(r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_mc_tri_dumped_metadata.csv")
PS2_CSV = Path(r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_ps2_sha1_version_dates.csv")

MATCH_LOG_CSV = Path("matched_post_save_sha1s.csv")

MAX_WORKERS = os.cpu_count() or 8
DELETE_MODE = True

# ==========================================================
# HARD CODED REGEX PATTERNS (MATCH AGAINST STEM ONLY)
# ==========================================================
PATTERNS = [
    r"-r640x96-00002694$",
    r"-r1016x0-00002a93$",
    r"^3eb591817c1ea309-.+-00001dd4$",
    r"-c08affed504498f1-r0x320-00002654$",
    r"^...............-80c02202$",
    r"-20d9773182cf07ff-r553x148-000022ac$",
    r"-r511x447-80c02641$",
]

COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in PATTERNS]

# ==========================================================
# GLOBALS
# ==========================================================
print_lock = threading.Lock()
log_lock = threading.Lock()

processed_count = 0
regex_deleted_count = 0
sha1_matched_count = 0
sha1_deleted_count = 0
leftover_duplicate_deleted_count = 0
failed = []

mc_sha1_set = set()
ps2_sha1_set = set()

existing_log_rows = []
existing_log_keys = set()
new_log_rows = []

leftover_candidates = []

# ==========================================================
# HELPERS
# ==========================================================
def matches_any_pattern(stem: str) -> bool:
    for pattern in COMPILED_PATTERNS:
        if pattern.search(stem):
            return True
    return False


def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(8 * 1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def load_reference_sha1s() -> None:
    global mc_sha1_set, ps2_sha1_set

    with MC_CSV.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "mc_tri_dumped_sha1" not in reader.fieldnames:
            raise ValueError(f"Missing 'mc_tri_dumped_sha1' column in {MC_CSV}")
        for row in reader:
            sha1 = row.get("mc_tri_dumped_sha1", "").strip().lower()
            if sha1:
                mc_sha1_set.add(sha1)

    with PS2_CSV.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "sha1" not in reader.fieldnames:
            raise ValueError(f"Missing 'sha1' column in {PS2_CSV}")
        for row in reader:
            sha1 = row.get("sha1", "").strip().lower()
            if sha1:
                ps2_sha1_set.add(sha1)


def make_log_key(filename: str, post_save_sha1: str, action: str, kept_filename: str) -> tuple[str, str, str, str]:
    return (
        filename.strip(),
        post_save_sha1.strip().lower(),
        action.strip(),
        kept_filename.strip(),
    )


def add_log_row(filename: str, post_save_sha1: str, action: str, kept_filename: str = "") -> None:
    global new_log_rows

    row = {
        "filename": filename.strip(),
        "post_save_sha1": post_save_sha1.strip().lower(),
        "action": action.strip(),
        "kept_filename": kept_filename.strip(),
    }
    key = make_log_key(
        row["filename"],
        row["post_save_sha1"],
        row["action"],
        row["kept_filename"],
    )

    with log_lock:
        if key in existing_log_keys:
            return

        for existing_row in new_log_rows:
            existing_key = make_log_key(
                existing_row["filename"],
                existing_row["post_save_sha1"],
                existing_row["action"],
                existing_row["kept_filename"],
            )
            if existing_key == key:
                return

        new_log_rows.append(row)


def load_existing_match_log() -> None:
    global existing_log_rows, existing_log_keys

    existing_log_rows = []
    existing_log_keys = set()

    if not MATCH_LOG_CSV.exists():
        return

    with MATCH_LOG_CSV.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return

        if "filename" not in reader.fieldnames or "post_save_sha1" not in reader.fieldnames:
            raise ValueError(
                f"Missing required columns in {MATCH_LOG_CSV}: filename, post_save_sha1"
            )

        for row in reader:
            filename = row.get("filename", "").strip()
            post_save_sha1 = row.get("post_save_sha1", "").strip().lower()
            action = row.get("action", "").strip()
            kept_filename = row.get("kept_filename", "").strip()

            if not filename or not post_save_sha1:
                continue

            normalized_row = {
                "filename": filename,
                "post_save_sha1": post_save_sha1,
                "action": action,
                "kept_filename": kept_filename,
            }

            existing_log_rows.append(normalized_row)
            existing_log_keys.add(
                make_log_key(filename, post_save_sha1, action, kept_filename)
            )


def write_match_log() -> None:
    merged_rows = list(existing_log_rows)
    merged_rows.extend(new_log_rows)

    merged_rows.sort(
        key=lambda row: (
            row["filename"].lower(),
            row["post_save_sha1"],
            row["action"].lower(),
            row["kept_filename"].lower(),
        )
    )

    tmp_path = MATCH_LOG_CSV.with_suffix(MATCH_LOG_CSV.suffix + ".tmp")

    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["filename", "post_save_sha1", "action", "kept_filename"])
        for row in merged_rows:
            writer.writerow([
                row["filename"],
                row["post_save_sha1"],
                row["action"],
                row["kept_filename"],
            ])

    tmp_path.replace(MATCH_LOG_CSV)


def resave_png_no_optimize(path: Path) -> None:
    temp_path = path.with_name(path.name + ".tmp")

    with Image.open(path) as img:
        img.load()

        save_kwargs = {
            "format": "PNG",
            "optimize": False,
        }

        if img.mode in ("P", "PA"):
            save_kwargs["bits"] = 8

        img.save(temp_path, **save_kwargs)

    temp_path.replace(path)

# ==========================================================
# WORKER
# ==========================================================
def process_file(path: Path) -> None:
    global processed_count, regex_deleted_count, sha1_matched_count, sha1_deleted_count

    try:
        stem = path.stem

        # --------------------------------------------------
        # PATH 1: REGEX DELETE
        # --------------------------------------------------
        if matches_any_pattern(stem):
            with print_lock:
                print(f"[REGEX MATCH] {path}")

            if DELETE_MODE:
                path.unlink()
                with print_lock:
                    regex_deleted_count += 1
                    print(f"[REGEX DELETED] {path}")

            with print_lock:
                processed_count += 1
            return

        # --------------------------------------------------
        # PATH 2: RESAVE -> POST-SAVE SHA1
        # --------------------------------------------------
        resave_png_no_optimize(path)
        post_save_sha1 = sha1_file(path)

        is_sha1_match = (
            post_save_sha1 in mc_sha1_set or
            post_save_sha1 in ps2_sha1_set
        )

        if is_sha1_match:
            add_log_row(
                filename=stem,
                post_save_sha1=post_save_sha1,
                action="reference_sha1_deleted",
                kept_filename="",
            )

            with print_lock:
                sha1_matched_count += 1
                print(f"[SHA1 MATCH] {path} -> {post_save_sha1}")

            if DELETE_MODE:
                path.unlink()
                with print_lock:
                    sha1_deleted_count += 1
                    print(f"[SHA1 DELETED] {path}")

            with print_lock:
                processed_count += 1
            return

        # --------------------------------------------------
        # PATH 3: LEFTOVER CANDIDATE FOR DEDUPE PASS
        # --------------------------------------------------
        with log_lock:
            leftover_candidates.append((path, stem, post_save_sha1))

        with print_lock:
            print(f"[LEFTOVER] {path} -> {post_save_sha1}")

        with print_lock:
            processed_count += 1

    except Exception as e:
        with print_lock:
            failed.append((str(path), str(e)))
            print(f"[FAILED] {path} -> {e}")


def deduplicate_leftovers() -> None:
    global leftover_duplicate_deleted_count

    grouped = {}

    for path, stem, post_save_sha1 in leftover_candidates:
        grouped.setdefault(post_save_sha1, []).append((path, stem))

    duplicate_groups = 0
    duplicate_files = 0

    for post_save_sha1, entries in sorted(grouped.items(), key=lambda x: x[0]):
        if len(entries) <= 1:
            continue

        duplicate_groups += 1

        entries.sort(key=lambda item: (item[1].lower(), str(item[0]).lower()))
        kept_path, kept_stem = entries[0]

        with print_lock:
            print(f"[DEDUPE KEEP] {kept_path} -> {post_save_sha1}")

        for duplicate_path, duplicate_stem in entries[1:]:
            duplicate_files += 1

            add_log_row(
                filename=duplicate_stem,
                post_save_sha1=post_save_sha1,
                action="leftover_duplicate_deleted",
                kept_filename=kept_stem,
            )

            with print_lock:
                print(f"[DEDUPE DUPLICATE] {duplicate_path} -> {post_save_sha1} | keeping {kept_path}")

            if DELETE_MODE:
                try:
                    duplicate_path.unlink()
                    with print_lock:
                        leftover_duplicate_deleted_count += 1
                        print(f"[DEDUPE DELETED] {duplicate_path}")
                except Exception as e:
                    with print_lock:
                        failed.append((str(duplicate_path), str(e)))
                        print(f"[FAILED] {duplicate_path} -> {e}")

    print(f"Leftover duplicate SHA1 groups: {duplicate_groups}")
    print(f"Leftover duplicate PNGs found: {duplicate_files}")

# ==========================================================
# MAIN
# ==========================================================
def main() -> None:
    if not ROOT_DIR.exists():
        print(f"[ERROR] Missing folder: {ROOT_DIR}")
        return

    if not MC_CSV.exists():
        print(f"[ERROR] Missing CSV: {MC_CSV}")
        return

    if not PS2_CSV.exists():
        print(f"[ERROR] Missing CSV: {PS2_CSV}")
        return

    load_reference_sha1s()
    load_existing_match_log()

    png_files = sorted(ROOT_DIR.glob("*.png"), key=lambda p: p.name.lower())
    total_files = len(png_files)

    print(f"Loaded {len(mc_sha1_set)} MC sha1s")
    print(f"Loaded {len(ps2_sha1_set)} PS2 sha1s")
    print(f"Loaded {len(existing_log_rows)} existing logged rows")
    print(f"Found {total_files} PNG files")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, path) for path in png_files]

        completed = 0
        for _ in as_completed(futures):
            completed += 1
            if completed % 100 == 0 or completed == total_files:
                print(f"Progress: {completed}/{total_files}")

    deduplicate_leftovers()
    write_match_log()

    print("\n===== SUMMARY =====")
    print(f"Processed: {processed_count}")
    print(f"Regex deleted: {regex_deleted_count}")
    print(f"SHA1 matched: {sha1_matched_count}")
    print(f"SHA1 deleted: {sha1_deleted_count}")
    print(f"Leftover duplicate deleted: {leftover_duplicate_deleted_count}")
    print(f"Leftover candidates kept after reference filtering: {len(leftover_candidates)}")
    print(f"New rows added to log: {len(new_log_rows)}")
    print(f"Final log path: {MATCH_LOG_CSV.resolve()}")

    if failed:
        print(f"Failed: {len(failed)}")
        for path, err in failed:
            print(f"  {path} -> {err}")


if __name__ == "__main__":
    main()