from __future__ import annotations

import csv
import hashlib
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# ==========================================================
# CONFIG
# ==========================================================
DUMP_ROOT = Path(r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Subsistence\US\Need ident")
FINAL_REBUILT_DIR = DUMP_ROOT / "Final Rebuilt"

TEXTURE_MAP_CSV = Path(r"C:\Development\Git\MGS-Tri-Dumper\mgs3_texture_map.csv")
SLOT_NUMBER_TO_NAME_CSV = Path(r"C:\Development\Git\MGS-Tri-Dumper\mgs3\extracted\SLOT\slot_number_to_name.csv")

MAX_WORKERS = min(32, max(4, (os.cpu_count() or 4)))
SHA1_BUFFER_SIZE = 4 * 1024 * 1024

LOG_FILE = DUMP_ROOT / "_final_rebuild_log.txt"
AMBIGUOUS_LOG_FILE = DUMP_ROOT / "_final_rebuild_ambiguous_log.txt"
UNMATCHED_LOG_FILE = DUMP_ROOT / "_final_rebuild_unmatched_log.txt"


# ==========================================================
# DATA TYPES
# ==========================================================
@dataclass(frozen=True)
class SourceCandidate:
    source_path: Path
    raw_stage_folder: str
    resolved_stage_name: str
    tri_strcode: str
    texture_strcode: str
    target_texture_filename: str
    target_path: Path


@dataclass(frozen=True)
class HashedSourceCandidate:
    candidate: SourceCandidate
    sha1: str


# ==========================================================
# HELPERS
# ==========================================================
def normalize_token(value: str) -> str:
    return value.strip().lower()


def sha1_file(file_path: Path) -> str:
    digest = hashlib.sha1()
    with file_path.open("rb") as f:
        while True:
            chunk = f.read(SHA1_BUFFER_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def is_probably_slot_stage(stage_folder_name: str) -> bool:
    stripped = stage_folder_name.strip()
    return stripped.isdigit() and len(stripped) <= 3


def pad_slot_stage(stage_folder_name: str) -> str:
    return stage_folder_name.strip().zfill(3)


def parse_slot_stage_name(raw_value: str) -> Optional[str]:
    line = raw_value.strip()
    if not line:
        return None

    comment_pos = line.find("//")
    if comment_pos != -1:
        line = line[:comment_pos].rstrip()

    if ":" not in line:
        return None

    number_part, name_part = line.split(":", 1)
    number_part = number_part.strip()
    name_part = name_part.strip()

    if not number_part or not name_part:
        return None

    return f"{number_part}:{name_part}"


def load_slot_number_to_name(csv_path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        for raw_line in f:
            parsed = parse_slot_stage_name(raw_line)
            if not parsed:
                continue

            number_part, stage_name = parsed.split(":", 1)
            mapping[number_part] = stage_name

    return mapping


def load_texture_map(csv_path: Path) -> Tuple[Dict[Tuple[str, str, str], Set[str]], List[str]]:
    mapping: Dict[Tuple[str, str, str], Set[str]] = {}
    issues: List[str] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for row_index, row in enumerate(reader, start=1):
            if not row:
                continue

            first = row[0].strip()
            if not first or first.startswith(";"):
                continue

            if len(row) < 4:
                if len(row) == 3:
                    continue

                issues.append(f"Line {row_index}: expected 4 columns, got {len(row)}")
                continue

            texture_filename = row[0].strip()
            stage = normalize_token(row[1])
            tri_strcode = normalize_token(row[2])
            texture_strcode = normalize_token(row[3])

            if not texture_filename or not stage or not tri_strcode or not texture_strcode:
                issues.append(f"Line {row_index}: missing required field(s)")
                continue

            key = (stage, tri_strcode, texture_strcode)
            mapping.setdefault(key, set()).add(texture_filename)

    return mapping, issues


def iter_candidate_png_files(root: Path) -> List[Path]:
    files: List[Path] = []

    for current_root, dirnames, filenames in os.walk(root):
        current_path = Path(current_root)

        dirnames[:] = [d for d in dirnames if d.lower() != "final rebuilt"]

        for filename in filenames:
            if filename.lower().endswith(".png"):
                files.append(current_path / filename)

    return files


def resolve_stage_name(
    raw_stage_folder: str,
    slot_number_to_name: Dict[str, str],
) -> Optional[str]:
    if is_probably_slot_stage(raw_stage_folder):
        padded = pad_slot_stage(raw_stage_folder)
        mapped = slot_number_to_name.get(padded)
        if mapped:
            return normalize_token(mapped)
        return None

    return normalize_token(raw_stage_folder)


def parse_source_candidate(
    file_path: Path,
    slot_number_to_name: Dict[str, str],
    texture_map: Dict[Tuple[str, str, str], Set[str]],
) -> Tuple[Optional[SourceCandidate], Optional[str], Optional[str]]:
    parent = file_path.parent
    tri_folder = parent.name
    stage_folder = parent.parent.name if parent.parent else ""

    if not stage_folder:
        return None, f"Invalid path structure, missing stage folder: {file_path}", None

    if not tri_folder:
        return None, f"Invalid path structure, missing tri folder: {file_path}", None

    texture_strcode = normalize_token(file_path.stem)
    tri_strcode = normalize_token(tri_folder)
    resolved_stage_name = resolve_stage_name(stage_folder, slot_number_to_name)

    if not resolved_stage_name:
        return None, None, (
            f"Unmatched stage mapping | source={file_path} | "
            f"raw_stage={stage_folder} | tri={tri_strcode} | texture={texture_strcode}"
        )

    key = (resolved_stage_name, tri_strcode, texture_strcode)
    candidates = texture_map.get(key)

    if not candidates:
        return None, None, (
            f"No CSV match | source={file_path} | "
            f"raw_stage={stage_folder} | resolved_stage={resolved_stage_name} | "
            f"tri={tri_strcode} | texture={texture_strcode}"
        )

    if len(candidates) > 1:
        candidate_str = " | ".join(sorted(candidates))
        return None, (
            f"Ambiguous CSV match | source={file_path} | "
            f"raw_stage={stage_folder} | resolved_stage={resolved_stage_name} | "
            f"tri={tri_strcode} | texture={texture_strcode} | "
            f"candidates={candidate_str}"
        ), None

    texture_filename = next(iter(candidates))
    target_path = FINAL_REBUILT_DIR / f"{texture_filename}.png"

    return (
        SourceCandidate(
            source_path=file_path,
            raw_stage_folder=stage_folder,
            resolved_stage_name=resolved_stage_name,
            tri_strcode=tri_strcode,
            texture_strcode=texture_strcode,
            target_texture_filename=texture_filename,
            target_path=target_path,
        ),
        None,
        None,
    )


def hash_candidate(candidate: SourceCandidate) -> HashedSourceCandidate:
    return HashedSourceCandidate(
        candidate=candidate,
        sha1=sha1_file(candidate.source_path),
    )


# ==========================================================
# GROUP PROCESSING
# ==========================================================
def process_target_group(
    texture_filename: str,
    hashed_candidates: List[HashedSourceCandidate],
) -> List[str]:
    lines: List[str] = []

    if not hashed_candidates:
        return lines

    target_path = hashed_candidates[0].candidate.target_path
    target_path.parent.mkdir(parents=True, exist_ok=True)

    sha1_to_candidates: Dict[str, List[HashedSourceCandidate]] = {}
    for hashed in hashed_candidates:
        sha1_to_candidates.setdefault(hashed.sha1, []).append(hashed)

    unique_sha1s = sorted(sha1_to_candidates.keys())

    if len(unique_sha1s) > 1:
        lines.append(
            f"HASH_MISMATCH_GROUP | target={target_path} | texture_filename={texture_filename} | "
            f"unique_sha1s={len(unique_sha1s)}"
        )

        for sha1_value in unique_sha1s:
            for hashed in sorted(sha1_to_candidates[sha1_value], key=lambda x: str(x.candidate.source_path).lower()):
                lines.append(
                    f"HASH_MISMATCH_SOURCE | target={target_path} | texture_filename={texture_filename} | "
                    f"source={hashed.candidate.source_path} | sha1={hashed.sha1} | "
                    f"raw_stage={hashed.candidate.raw_stage_folder} | resolved_stage={hashed.candidate.resolved_stage_name} | "
                    f"tri={hashed.candidate.tri_strcode} | texture={hashed.candidate.texture_strcode}"
                )

        return lines

    only_sha1 = unique_sha1s[0]
    group_candidates = sorted(
        sha1_to_candidates[only_sha1],
        key=lambda x: str(x.candidate.source_path).lower(),
    )

    chosen = group_candidates[0]
    duplicates = group_candidates[1:]

    if target_path.exists():
        target_sha1 = sha1_file(target_path)

        if target_sha1 == only_sha1:
            for hashed in group_candidates:
                if hashed.candidate.source_path.exists():
                    hashed.candidate.source_path.unlink()
                    lines.append(
                        f"DELETE_DUPLICATE_TO_EXISTING_TARGET | source={hashed.candidate.source_path} | "
                        f"target={target_path} | sha1={only_sha1}"
                    )
            return lines

        lines.append(
            f"EXISTING_TARGET_CONFLICT | target={target_path} | texture_filename={texture_filename} | "
            f"group_sha1={only_sha1} | target_sha1={target_sha1}"
        )

        for hashed in group_candidates:
            lines.append(
                f"EXISTING_TARGET_CONFLICT_SOURCE | source={hashed.candidate.source_path} | "
                f"target={target_path} | source_sha1={hashed.sha1}"
            )

        return lines

    shutil.move(str(chosen.candidate.source_path), str(target_path))
    lines.append(
        f"MOVE_GROUP_PRIMARY | source={chosen.candidate.source_path} | target={target_path} | "
        f"sha1={only_sha1} | duplicate_count={len(duplicates)}"
    )

    for hashed in duplicates:
        if hashed.candidate.source_path.exists():
            hashed.candidate.source_path.unlink()
            lines.append(
                f"DELETE_GROUP_DUPLICATE | source={hashed.candidate.source_path} | "
                f"target={target_path} | sha1={only_sha1}"
            )

    return lines


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    if not DUMP_ROOT.is_dir():
        print(f"ERROR: Dump root does not exist: {DUMP_ROOT}")
        return 1

    if not TEXTURE_MAP_CSV.is_file():
        print(f"ERROR: Texture map CSV does not exist: {TEXTURE_MAP_CSV}")
        return 1

    if not SLOT_NUMBER_TO_NAME_CSV.is_file():
        print(f"ERROR: SLOT stage mapping CSV does not exist: {SLOT_NUMBER_TO_NAME_CSV}")
        return 1

    FINAL_REBUILT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Loading SLOT mapping: {SLOT_NUMBER_TO_NAME_CSV}")
    slot_number_to_name = load_slot_number_to_name(SLOT_NUMBER_TO_NAME_CSV)
    print(f"Loaded {len(slot_number_to_name):,} SLOT stage mappings")

    print(f"Loading texture map: {TEXTURE_MAP_CSV}")
    texture_map, texture_map_issues = load_texture_map(TEXTURE_MAP_CSV)
    print(f"Loaded {len(texture_map):,} unique texture map keys")

    if texture_map_issues:
        print(f"Texture map load issues: {len(texture_map_issues):,}")

    print(f"Scanning for .png files under: {DUMP_ROOT}")
    candidate_files = iter_candidate_png_files(DUMP_ROOT)
    print(f"Found {len(candidate_files):,} .png files to inspect\n")

    source_candidates: List[SourceCandidate] = []
    ambiguous_lines: List[str] = []
    unmatched_lines: List[str] = []

    for file_path in candidate_files:
        candidate, ambiguous, unmatched = parse_source_candidate(
            file_path=file_path,
            slot_number_to_name=slot_number_to_name,
            texture_map=texture_map,
        )

        if candidate is not None:
            source_candidates.append(candidate)

        if ambiguous:
            ambiguous_lines.append(ambiguous)

        if unmatched:
            unmatched_lines.append(unmatched)

    print(f"Matched source candidates: {len(source_candidates):,}")
    print(f"Ambiguous matches: {len(ambiguous_lines):,}")
    print(f"Unmatched files: {len(unmatched_lines):,}\n")

    hashed_candidates: List[HashedSourceCandidate] = []

    if source_candidates:
        print("Hashing matched source candidates...")
        completed = 0
        total = len(source_candidates)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(hash_candidate, candidate) for candidate in source_candidates]

            for future in as_completed(futures):
                completed += 1
                hashed_candidates.append(future.result())

                if completed % 100 == 0 or completed == total:
                    print(f"[{completed:,}/{total:,}] hashed")

    grouped_by_texture_filename: Dict[str, List[HashedSourceCandidate]] = {}
    for hashed in hashed_candidates:
        grouped_by_texture_filename.setdefault(
            hashed.candidate.target_texture_filename,
            [],
        ).append(hashed)

    group_keys = sorted(grouped_by_texture_filename.keys())
    print(f"\nGrouped into {len(group_keys):,} target texture_filename buckets")

    results: List[str] = []
    processed_groups = 0
    total_groups = len(group_keys)

    for texture_filename in group_keys:
        processed_groups += 1
        group_lines = process_target_group(
            texture_filename=texture_filename,
            hashed_candidates=grouped_by_texture_filename[texture_filename],
        )
        results.extend(group_lines)

        if processed_groups % 100 == 0 or processed_groups == total_groups:
            print(f"[{processed_groups:,}/{total_groups:,}] target groups processed")

    log_lines: List[str] = []
    log_lines.append(f"Dump root: {DUMP_ROOT}")
    log_lines.append(f"Final rebuilt dir: {FINAL_REBUILT_DIR}")
    log_lines.append(f"Texture map CSV: {TEXTURE_MAP_CSV}")
    log_lines.append(f"SLOT mapping CSV: {SLOT_NUMBER_TO_NAME_CSV}")
    log_lines.append(f"Max workers: {MAX_WORKERS}")
    log_lines.append("")
    log_lines.append(f"Total scanned .png files: {len(candidate_files):,}")
    log_lines.append(f"Matched source candidates: {len(source_candidates):,}")
    log_lines.append(f"Hashed source candidates: {len(hashed_candidates):,}")
    log_lines.append(f"Target groups: {len(group_keys):,}")
    log_lines.append(f"Ambiguous matches: {len(ambiguous_lines):,}")
    log_lines.append(f"Unmatched files: {len(unmatched_lines):,}")
    log_lines.append("")

    if texture_map_issues:
        log_lines.append("=== TEXTURE MAP LOAD ISSUES ===")
        log_lines.extend(texture_map_issues)
        log_lines.append("")

    log_lines.append("=== ACTIONS ===")
    log_lines.extend(results)
    LOG_FILE.write_text("\n".join(log_lines) + "\n", encoding="utf-8")

    AMBIGUOUS_LOG_FILE.write_text(
        "\n".join(sorted(ambiguous_lines)) + ("\n" if ambiguous_lines else ""),
        encoding="utf-8",
    )

    UNMATCHED_LOG_FILE.write_text(
        "\n".join(sorted(unmatched_lines)) + ("\n" if unmatched_lines else ""),
        encoding="utf-8",
    )

    move_count = sum(1 for line in results if line.startswith("MOVE_GROUP_PRIMARY | "))
    delete_group_duplicate_count = sum(1 for line in results if line.startswith("DELETE_GROUP_DUPLICATE | "))
    delete_existing_duplicate_count = sum(1 for line in results if line.startswith("DELETE_DUPLICATE_TO_EXISTING_TARGET | "))
    hash_mismatch_group_count = sum(1 for line in results if line.startswith("HASH_MISMATCH_GROUP | "))
    existing_target_conflict_count = sum(1 for line in results if line.startswith("EXISTING_TARGET_CONFLICT | "))

    # ==========================================================
    # CLEANUP: REMOVE EMPTY FOLDERS
    # ==========================================================
    print("\nCleaning up empty folders...")

    removed_dirs = 0

    for current_root, dirnames, filenames in os.walk(DUMP_ROOT, topdown=False):
        current_path = Path(current_root)

        if current_path == FINAL_REBUILT_DIR:
            continue

        if FINAL_REBUILT_DIR in current_path.parents:
            continue

        try:
            if not any(current_path.iterdir()):
                current_path.rmdir()
                removed_dirs += 1
        except Exception:
            pass

    print(f"Removed empty folders: {removed_dirs:,}\n")

    print("Done.\n")
    print(f"Moved to Final Rebuilt: {move_count:,}")
    print(f"Deleted group duplicates: {delete_group_duplicate_count:,}")
    print(f"Deleted duplicates against existing targets: {delete_existing_duplicate_count:,}")
    print(f"Hash mismatch groups: {hash_mismatch_group_count:,}")
    print(f"Existing target conflicts: {existing_target_conflict_count:,}")
    print(f"Ambiguous CSV matches: {len(ambiguous_lines):,}")
    print(f"Unmatched files: {len(unmatched_lines):,}")
    print("")
    print(f"Main log: {LOG_FILE}")
    print(f"Ambiguous log: {AMBIGUOUS_LOG_FILE}")
    print(f"Unmatched log: {UNMATCHED_LOG_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())