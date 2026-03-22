from __future__ import annotations

import csv
import os
import shutil
import sys
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


# ==========================================================
# HELPERS
# ==========================================================
def normalize_token(value: str) -> str:
    return value.strip().lower()


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


# ==========================================================
# GROUP PROCESSING
# ==========================================================
def process_target_group(
    texture_filename: str,
    candidates: List[SourceCandidate],
) -> List[str]:
    lines: List[str] = []

    if not candidates:
        return lines

    sorted_candidates = sorted(candidates, key=lambda x: str(x.source_path).lower())
    target_path = sorted_candidates[0].target_path
    target_path.parent.mkdir(parents=True, exist_ok=True)

    if target_path.exists():
        lines.append(
            f"TARGET_ALREADY_EXISTS | target={target_path} | texture_filename={texture_filename} | "
            f"incoming_count={len(sorted_candidates)}"
        )

        for candidate in sorted_candidates:
            if candidate.source_path.exists():
                candidate.source_path.unlink()
                lines.append(
                    f"DELETE_DUPLICATE_TO_EXISTING_TARGET | source={candidate.source_path} | "
                    f"target={target_path} | raw_stage={candidate.raw_stage_folder} | "
                    f"resolved_stage={candidate.resolved_stage_name} | tri={candidate.tri_strcode} | "
                    f"texture={candidate.texture_strcode}"
                )

        return lines

    chosen = sorted_candidates[0]
    duplicates = sorted_candidates[1:]

    shutil.move(str(chosen.source_path), str(target_path))
    lines.append(
        f"MOVE_GROUP_PRIMARY | source={chosen.source_path} | target={target_path} | "
        f"duplicate_count={len(duplicates)} | raw_stage={chosen.raw_stage_folder} | "
        f"resolved_stage={chosen.resolved_stage_name} | tri={chosen.tri_strcode} | "
        f"texture={chosen.texture_strcode}"
    )

    for candidate in duplicates:
        if candidate.source_path.exists():
            candidate.source_path.unlink()
            lines.append(
                f"DELETE_GROUP_DUPLICATE | source={candidate.source_path} | "
                f"target={target_path} | raw_stage={candidate.raw_stage_folder} | "
                f"resolved_stage={candidate.resolved_stage_name} | tri={candidate.tri_strcode} | "
                f"texture={candidate.texture_strcode}"
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

    print(f"Scanning for .png files under: {DUMP_ROOT}")
    candidate_files = iter_candidate_png_files(DUMP_ROOT)
    print(f"Found {len(candidate_files):,} .png files to inspect\n")

    # rest unchanged...