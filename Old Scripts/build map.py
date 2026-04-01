from __future__ import annotations

import csv
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# ==========================================================
# CONFIG
# ==========================================================
SLOT_ROOT = Path(r"C:\Development\Git\MGS-Tri-Dumper\mgs3\extracted\SLOT")

INPUT_CSV_NAME = "slot_texture_map_no_img.csv"
SUB_USA_DISC1_CSV_NAME = "sub_usa_disc1.csv"
ALIASES_CSV_NAME = "sv_mapgen_aliases.csv"

# Intentionally plain text output (not a real CSV)
OUTPUT_MAPPING_TXT = "slot_number_to_name.csv"
OUTPUT_ALIAS_MAPPING_TXT = "slot.map"

OUTPUT_CONFLICTS_CSV = "slot_number_to_name_conflicts.csv"
OUTPUT_MISSING_CSV = "slot_number_to_name_missing.csv"

# Conflict resolver rule:
# If a numeric folder contains "03a157.tga" with this SHA1:
#   - ONLY allow candidate slot_names that end with "_vr"
# If the file is missing OR SHA1 does NOT match:
#   - candidate slot_names ending with "_vr" are DISALLOWED
RESOLVE_SENTINEL_FILENAME = "03a157.tga"
RESOLVE_SENTINEL_SHA1 = "bd46ece03e5960d52db4ce4740bbb3b109d0ae5b"
RESOLVE_NAME_SUFFIX = "_vr"

SV_MODEL_NAME_PREFIX = "sv_model-"

# Additional memcard resolver from sub_usa_disc1.csv:
# If a SLOT subfolder has 552e16.ico listed in sub_usa_disc1.csv, prefer
# memcard_mg-normal over memcard-normal for that folder.
MEMCARD_SENTINEL_FILE = "552e16.ico"
MEMCARD_NORMAL_NAME = "memcard-normal"
MEMCARD_MG_NORMAL_NAME = "memcard_mg-normal"

# Additional camo-family resolver:
# 765dfa.tga identifies the camo family, but some hashes may map to more than
# one family. The VR rule still runs first.
CAMO_SENTINEL_FILENAME = "765dfa.tga"

CAMO_SHA1_TO_FAMILIES: Dict[str, Set[str]] = {
    "4e46dfb24d411cc66715865f3fd00eb69a613869": {"animal"},
    "d5c906ea89a6cc8288db9963d887b398ac070666": {"auscam"},
    "9a3e52133e49efec4941bae9cf59a04019e49163": {"banana"},
    "4d7a30e6f6eb8dc901cd20f598724231c038ae3e": {"black"},
    "87cd4d6b7aa521bf635a0877ad2233de1b68eedb": {"choco_chip"},
    "33f5cd7645b2d95a8264232b37402f8e47e4d285": {"cold_war"},
    "050bfe9274b5c3da18266301cdaa462637875015": {"desert"},
    "a43a21e51d5180f1311ee6ea96d2b0603f896695": {"dpm"},
    "0722b062f6f351560e79baf6ba40e17772e14aec": {"fire"},
    "01c0e0057e377f18bc6a0904f33838491ac5286c": {"flecktarn"},
    "77e0c428f3399e243820f6cc5f54f22265576625": {"fly"},
    "3a881fbe6747b8469cea25573c083b7bc25e2843": {"garco"},
    "2bf6a00647662ac4ed187ef7d3c6f9f00e3fa99d": {"grenade"},
    "544919548fa32994f0f86d7bda0282223898a6a9": {"hornet_stripe"},
    "0cb3d5154e41a4967ce09d1c1da074036ae89c60": {"leaf"},
    "67ec9643dc5bb02412df7080e3880b5bbbadc05c": {"moss"},
    "6c5dce4d05e5609aaa2bba9f5ef9759fdda77f27": {"mummy"},
    "ecc5c611ea596b43f122934f77eb6faf53d1c4b3": {"normal"},
    "01d8a9b2841821798a2848332c7699ef9fd56e66": {"rain_stroke"},
    "13b4426f30da7bdc8aa72ac81a9fea7f3bfa8c6f": {"snake", "hebi"},
    "cd3ca3329f8a9b091ed627d54c1d2fe68ff29c44": {"snow"},
    "f177494f6477f9bb5e9f0e27ab22dc3fc7850b9c": {"spider"},
    "a103d06a4fee4bddb6776dcc64035149e4fa6f26": {"spirit"},
    "5179398511407800e2ec58e4fb1b92ae51fadfd0": {"splitter"},
    "fbbabbd3a0b7c65f013f4f91a39d0420d4a7c3b3": {"squares"},
    "211547352242eb8f059a7ac68d7c712a3fceee8a": {"tiger_stripe"},
    "7a5a1e78dfc2104066964bd7cb083f3a905908de": {"tree_bark"},
    "92d067012e6891864073458298bde34ad22772b0": {"water"},
}

# Forced unique assignments for fully ambiguous groups.
# For each exact candidate-set match, unresolved folders are sorted numerically,
# then assigned names in this exact order, one per folder.
FORCED_UNIQUE_GROUP_ORDERS: Dict[frozenset[str], List[str]] = {
    frozenset({"animals-hebi_i", "animals-hebi_j", "animals-hebi_k"}): [
        "animals-hebi_i",
        "animals-hebi_j",
        "animals-hebi_k",
    ],
    frozenset({"sv_model-liquid", "sv_model-solid", "sv_model-solidus"}): [
        "sv_model-liquid",
        "sv_model-solid",
        "sv_model-solidus",
    ],
}


# ==========================================================
# Helpers
# ==========================================================
def norm(s: str) -> str:
    return (s or "").strip().lower()


def parse_bool(value: str) -> bool:
    return norm(value) in {"1", "true", "yes", "y"}


def list_files_in_dir(d: Path) -> List[Path]:
    try:
        return [p for p in d.iterdir() if p.is_file()]
    except OSError:
        return []


def get_existing_numeric_folders() -> List[int]:
    nums: List[int] = []
    for p in SLOT_ROOT.iterdir():
        if p.is_dir() and p.name.isdigit():
            nums.append(int(p.name))
    nums.sort()
    return nums


def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha1_folder_tree(root: Path) -> Optional[str]:
    """
    Deterministic hash of a folder tree:
    - walks all files under root
    - hashes (relative_path + NUL + file_sha1) into a master sha1
    Returns None on IO failure.
    """
    if not root.is_dir():
        return None

    h = hashlib.sha1()

    try:
        files: List[Path] = []
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                files.append(Path(dirpath) / fn)

        files.sort(key=lambda p: str(p.relative_to(root)).lower())
    except OSError:
        return None

    for p in files:
        try:
            rel = str(p.relative_to(root)).replace("\\", "/").lower()
            digest = sha1_file(p).lower()
        except OSError:
            return None

        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(digest.encode("utf-8"))
        h.update(b"\0")

    return h.hexdigest()


def get_slot_folder_hash(folder_num: int, cache: Dict[int, Optional[str]]) -> Optional[str]:
    if folder_num in cache:
        return cache[folder_num]

    digest = sha1_folder_tree(SLOT_ROOT / str(folder_num))
    cache[folder_num] = digest
    return digest


def find_file_recursive(base: Path, filename_lower: str) -> Optional[Path]:
    # Only traverses 2 levels: <num>\<tri>\<files>
    try:
        tri_dirs = [p for p in base.iterdir() if p.is_dir()]
    except OSError:
        return None

    for tri_dir in tri_dirs:
        try:
            for f in tri_dir.iterdir():
                if f.is_file() and f.name.lower() == filename_lower:
                    return f
        except OSError:
            continue

    return None


def ends_with_suffix(name: str, suffix: str) -> bool:
    return norm(name).endswith(suffix)


def starts_with_prefix(name: str, prefix: str) -> bool:
    return norm(name).startswith(prefix)


def has_camoufla_candidate(cands: Set[str]) -> bool:
    for x in cands:
        if norm(x).startswith("camoufla-"):
            return True
    return False


def strip_vr_suffix(name: str) -> str:
    n = norm(name)
    if n.endswith("_vr"):
        return n[:-3]
    return n


def is_camoufla_dc_name(name: str) -> bool:
    n = strip_vr_suffix(name)
    return n.startswith("camoufla-dc_")


def is_camoufla_non_dc_name(name: str) -> bool:
    n = strip_vr_suffix(name)
    return n.startswith("camoufla-") and not n.startswith("camoufla-dc_")


def camo_base_key(name: str) -> Optional[str]:
    """
    Normalize camo names so these map to the same key:
      camoufla-mummy
      camoufla-dc_mummy
      camoufla-mummy_vr
      camoufla-dc_mummy_vr
    -> "mummy"
    """
    n = strip_vr_suffix(name)

    if not n.startswith("camoufla-"):
        return None

    rest = n[len("camoufla-"):]

    if rest.startswith("dc_"):
        rest = rest[len("dc_"):]

    return rest or None


def camo_family_from_name(name: str) -> Optional[str]:
    """
    Converts:
      camoufla-moss -> moss
      camoufla-dc_moss -> moss
      camoufla-moss_vr -> moss
      camoufla-dc_moss_vr -> moss
    Returns None if not a camoufla name.
    """
    return camo_base_key(name)


def get_camo_families_for_folder(
    folder_num: int,
    cache: Dict[int, Optional[Set[str]]],
) -> Optional[Set[str]]:
    if folder_num in cache:
        return cache[folder_num]

    base = SLOT_ROOT / str(folder_num)
    if not base.is_dir():
        cache[folder_num] = None
        return None

    sentinel = find_file_recursive(base, CAMO_SENTINEL_FILENAME.lower())
    if sentinel is None:
        cache[folder_num] = None
        return None

    try:
        digest = sha1_file(sentinel).lower()
    except OSError:
        cache[folder_num] = None
        return None

    families = CAMO_SHA1_TO_FAMILIES.get(digest)
    cache[folder_num] = set(families) if families else None
    return cache[folder_num]


def filter_candidates_by_camo_family(
    folder_num: int,
    candidates: Set[str],
    camo_family_cache: Dict[int, Optional[Set[str]]],
) -> Set[str]:
    """
    If 765dfa.tga exists and matches a known SHA1, keep only camoufla candidates
    whose family matches one of the allowed families for that SHA1.

    This supports cases where the same hash is legitimately shared by multiple
    camo families, such as snake/hebi.
    """
    if not candidates:
        return set()

    if not has_camoufla_candidate(candidates):
        return set(candidates)

    families = get_camo_families_for_folder(folder_num, camo_family_cache)
    if not families:
        return set(candidates)

    filtered: Set[str] = set()
    for cand in candidates:
        cand_family = camo_family_from_name(cand)
        if cand_family is None:
            filtered.add(cand)
            continue

        if cand_family in families:
            filtered.add(cand)

    return filtered if filtered else set(candidates)


def filter_candidates_prefer_non_dc_camoufla(candidates: Set[str]) -> Set[str]:
    """
    If a conflict set contains both dc_ and non-dc versions of the same camo family,
    prefer the non-dc version.
    """
    if len(candidates) <= 1:
        return set(candidates)

    grouped: Dict[str, Set[str]] = {}
    non_camo: Set[str] = set()

    for cand in candidates:
        key = camo_base_key(cand)
        if key is None:
            non_camo.add(cand)
            continue

        grouped.setdefault(key, set()).add(cand)

    out: Set[str] = set(non_camo)

    for _, group in grouped.items():
        non_dc = {x for x in group if is_camoufla_non_dc_name(x)}
        dc = {x for x in group if is_camoufla_dc_name(x)}

        if non_dc and dc:
            out.update(non_dc)
        else:
            out.update(group)

    return out if out else set(candidates)


def filter_candidates_by_unique_pairs(
    folder_num: int,
    candidates: Set[str],
    expected_pairs_by_name: Dict[str, Set[Tuple[str, str]]],
    pair_index: Dict[Tuple[str, str], Set[int]],
) -> Set[str]:
    """
    If candidates conflict, try to eliminate names that are missing their unique tri/tex pairs.

    For each candidate name, compute:
      unique_pairs = expected_pairs[name] - union(expected_pairs[other candidates])

    If unique_pairs is non-empty, require ALL those pairs exist in this folder.
    Names without any unique pairs are kept.
    """
    if len(candidates) <= 1:
        return set(candidates)

    expected: Dict[str, Set[Tuple[str, str]]] = {}
    for name in candidates:
        expected[name] = set(expected_pairs_by_name.get(name, set()))

    out: Set[str] = set()

    for name in candidates:
        other_union: Set[Tuple[str, str]] = set()
        for other in candidates:
            if other == name:
                continue
            other_union |= expected.get(other, set())

        unique_pairs = expected.get(name, set()) - other_union
        if not unique_pairs:
            out.add(name)
            continue

        ok = True
        for key in unique_pairs:
            if folder_num not in pair_index.get(key, set()):
                ok = False
                break

        if ok:
            out.add(name)

    return out


def filter_candidates_by_sv_model_flag(
    folder_num: int,
    candidates: Set[str],
    sv_model_subfolders: Set[int],
) -> Set[str]:
    if not candidates:
        return set()

    folder_is_sv_model = folder_num in sv_model_subfolders

    if folder_is_sv_model:
        filtered = {x for x in candidates if starts_with_prefix(x, SV_MODEL_NAME_PREFIX)}
    else:
        filtered = {x for x in candidates if not starts_with_prefix(x, SV_MODEL_NAME_PREFIX)}

    return filtered if filtered else set(candidates)


def filter_candidates_by_memcard_flag(
    folder_num: int,
    candidates: Set[str],
    memcard_mg_subfolders: Set[int],
) -> Set[str]:
    """
    If a conflict set contains both memcard-normal and memcard_mg-normal,
    use sub_usa_disc1.csv presence of 552e16.ico to choose memcard_mg-normal.
    """
    if len(candidates) <= 1:
        return set(candidates)

    has_plain = MEMCARD_NORMAL_NAME in candidates
    has_mg = MEMCARD_MG_NORMAL_NAME in candidates

    if not (has_plain and has_mg):
        return set(candidates)

    if folder_num in memcard_mg_subfolders:
        return {x for x in candidates if x != MEMCARD_NORMAL_NAME}

    return {x for x in candidates if x != MEMCARD_MG_NORMAL_NAME}


def read_sub_usa_disc1_metadata(csv_path: Path) -> Tuple[Set[int], Set[int]]:
    """
    Reads sub_usa_disc1.csv and returns:
      - numeric SLOT subfolders whose dict_folder_contains_sv_model column is true
      - numeric SLOT subfolders that contain 552e16.ico according to subfolder_file
    """
    sv_model_subfolders: Set[int] = set()
    memcard_mg_subfolders: Set[int] = set()

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required = {
            "stage_or_slot",
            "strcode_only_or_dict",
            "subfolder",
            "subfolder_file",
            "dict_folder_contains_sv_model",
        }
        if not required.issubset(set(reader.fieldnames or [])):
            raise RuntimeError(
                f"sub_usa_disc1.csv missing columns. Found: {reader.fieldnames}"
            )

        for row in reader:
            if norm(row.get("stage_or_slot", "")) != "slot":
                continue

            subfolder_raw = (row.get("subfolder", "") or "").strip()
            if not subfolder_raw.isdigit():
                continue

            folder_num = int(subfolder_raw)

            if norm(row.get("strcode_only_or_dict", "")) == "dict":
                if parse_bool(row.get("dict_folder_contains_sv_model", "")):
                    sv_model_subfolders.add(folder_num)

            if norm(row.get("subfolder_file", "")) == norm(MEMCARD_SENTINEL_FILE):
                memcard_mg_subfolders.add(folder_num)

    return sv_model_subfolders, memcard_mg_subfolders


def read_aliases(csv_path: Path) -> Dict[str, str]:
    """
    Reads sv_mapgen_aliases.csv and returns:
      { original_stage_name -> alias }
    """
    if not csv_path.is_file():
        return {}

    out: Dict[str, str] = {}

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required = {"alias", "original_stage_name"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise RuntimeError(
                f"{csv_path.name} missing columns. Found: {reader.fieldnames}"
            )

        for row in reader:
            alias = (row.get("alias", "") or "").strip()
            original = (row.get("original_stage_name", "") or "").strip()

            if not alias or not original:
                continue

            out[original] = alias

    return out


def build_forced_unique_comment(chosen: str, ordered_names: List[str]) -> str:
    others = [x for x in ordered_names if x != chosen]
    if not others:
        return ""
    return f"\t\t\t// may be swapped with: {', '.join(others)}"


def append_comment(base_comment: str, extra_comment: str) -> str:
    if not extra_comment:
        return base_comment
    if not base_comment:
        return extra_comment
    return f"{base_comment}; {extra_comment}"


# ==========================================================
# Build index: (tri_strcode, texture_strcode) -> {folder_numbers}
# Folder structure: <num>\<tri_strcode>\<texture_strcode>.<ext>
# Example: 715\1d26f7\4a5fe3.tga
# ==========================================================
def scan_numeric_folder(folder_num: int) -> Dict[Tuple[str, str], Set[int]]:
    base = SLOT_ROOT / str(folder_num)
    if not base.is_dir():
        return {}

    out: Dict[Tuple[str, str], Set[int]] = {}

    try:
        tri_dirs = [p for p in base.iterdir() if p.is_dir()]
    except OSError:
        return out

    for tri_dir in tri_dirs:
        tri_strcode = norm(tri_dir.name)
        if not tri_strcode:
            continue

        for f in list_files_in_dir(tri_dir):
            tex_strcode = norm(f.stem)
            if not tex_strcode:
                continue

            key = (tri_strcode, tex_strcode)
            out.setdefault(key, set()).add(folder_num)

    return out


def build_pair_index(folder_nums: List[int], max_workers: int) -> Dict[Tuple[str, str], Set[int]]:
    index: Dict[Tuple[str, str], Set[int]] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(scan_numeric_folder, n): n for n in folder_nums}

        done = 0
        for fut in as_completed(futures):
            done += 1
            n = futures[fut]
            try:
                partial = fut.result()
            except Exception as exc:
                print(f"[WARN] Failed scanning folder {n}: {exc}")
                continue

            for k, nums in partial.items():
                index.setdefault(k, set()).update(nums)

            if done % 25 == 0 or done == len(folder_nums):
                print(f"[INFO] Scanned {done}/{len(folder_nums)} folders")

    total_keys = len(index)
    multi = sum(1 for v in index.values() if len(v) > 1)
    print(f"[INFO] Pair index keys: {total_keys:,} (multi-folder keys: {multi:,})")
    return index


# ==========================================================
# Read CSV
# ==========================================================
def read_rows(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"texture_stem", "slot_name", "tri_strcode", "texture_strcode"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise RuntimeError(f"Input CSV missing columns. Found: {reader.fieldnames}")
        return list(reader)


# ==========================================================
# Conflict resolution
# ==========================================================
def folder_allows_vr_names(folder_num: int) -> Optional[bool]:
    """
    Returns:
      True  -> sentinel exists and SHA1 matches (VR names allowed; non-VR disallowed)
      False -> sentinel missing or SHA1 mismatch (VR names disallowed; non-VR allowed)
      None  -> could not compute due to IO issues; treat like False
    """
    base = SLOT_ROOT / str(folder_num)
    if not base.is_dir():
        return None

    sentinel = find_file_recursive(base, RESOLVE_SENTINEL_FILENAME.lower())
    if sentinel is None:
        return False

    try:
        digest = sha1_file(sentinel).lower()
    except OSError:
        return None

    return digest == RESOLVE_SENTINEL_SHA1.lower()


def resolve_candidates_by_rule(folder_num: int, candidate_names: Set[str]) -> Set[str]:
    allow_vr = folder_allows_vr_names(folder_num)

    if allow_vr is True:
        return {n for n in candidate_names if ends_with_suffix(n, RESOLVE_NAME_SUFFIX)}

    return {n for n in candidate_names if not ends_with_suffix(n, RESOLVE_NAME_SUFFIX)}


# ==========================================================
# Main
# ==========================================================
def main() -> int:
    script_dir = Path(__file__).resolve().parent
    csv_path = script_dir / INPUT_CSV_NAME
    sub_usa_disc1_csv_path = script_dir / SUB_USA_DISC1_CSV_NAME
    aliases_csv_path = script_dir / ALIASES_CSV_NAME

    if not SLOT_ROOT.is_dir():
        print(f"[ERROR] SLOT_ROOT not found: {SLOT_ROOT}")
        return 1

    if not csv_path.is_file():
        print(f"[ERROR] Input CSV not found next to script: {csv_path}")
        return 1

    if not sub_usa_disc1_csv_path.is_file():
        print(f"[ERROR] sub_usa_disc1.csv not found next to script: {sub_usa_disc1_csv_path}")
        return 1

    existing_folders = get_existing_numeric_folders()
    if not existing_folders:
        print("[ERROR] No numeric slot folders found")
        return 1

    print(f"[INFO] Found {len(existing_folders)} numeric slot folders")

    print("[INFO] Reading sub_usa_disc1.csv...")
    try:
        sv_model_subfolders, memcard_mg_subfolders = read_sub_usa_disc1_metadata(sub_usa_disc1_csv_path)
    except Exception as exc:
        print(f"[ERROR] Failed reading {sub_usa_disc1_csv_path.name}: {exc}")
        return 1

    print(f"[INFO] SLOT folders marked sv_model in {sub_usa_disc1_csv_path.name}: {len(sv_model_subfolders):,}")
    print(f"[INFO] SLOT folders containing {MEMCARD_SENTINEL_FILE} in {sub_usa_disc1_csv_path.name}: {len(memcard_mg_subfolders):,}")

    print("[INFO] Reading alias CSV...")
    try:
        alias_by_original_name = read_aliases(aliases_csv_path)
    except Exception as exc:
        print(f"[ERROR] Failed reading {aliases_csv_path.name}: {exc}")
        return 1

    print(f"[INFO] Aliases loaded from {ALIASES_CSV_NAME}: {len(alias_by_original_name):,}")

    max_workers = max(4, os.cpu_count() or 8)
    print(f"[INFO] Using {max_workers} threads")

    print("[INFO] Building (tri_strcode, texture_strcode) -> {folder_numbers} index...")
    pair_index = build_pair_index(existing_folders, max_workers)

    print("[INFO] Reading input CSV...")
    rows = read_rows(csv_path)
    print(f"[INFO] Input rows: {len(rows):,}")

    number_to_names: Dict[int, Set[str]] = {}
    expected_pairs_by_name: Dict[str, Set[Tuple[str, str]]] = {}

    missing_rows: List[Dict[str, str]] = []

    for row in rows:
        slot_name_raw = (row.get("slot_name", "") or "").strip()
        tri = norm(row.get("tri_strcode", ""))
        tex = norm(row.get("texture_strcode", ""))

        if not slot_name_raw or not tri or not tex:
            missing_rows.append(row)
            continue

        expected_pairs_by_name.setdefault(slot_name_raw, set()).add((tri, tex))

        nums = pair_index.get((tri, tex))
        if not nums:
            missing_rows.append(row)
            continue

        for n in nums:
            number_to_names.setdefault(n, set()).add(slot_name_raw)

    # ==========================================================
    # Resolve number->name mapping with UNIQUE name enforcement:
    # - Build candidate sets
    #     1) apply VR rule filtering
    #     2) apply camo-family filtering via 765dfa.tga SHA1
    #     3) prefer non-dc camoufla names when both exist
    #     4) apply sv_model folder filtering from sub_usa_disc1.csv
    #     5) apply memcard 552e16.ico filtering from sub_usa_disc1.csv
    #     6) apply "unique tri/tex pairs must exist" filtering
    # - Then:
    #     A) local greedy propagation: if a number has exactly 1 candidate, assign it
    #     B) global uniqueness pass: if a candidate appears in only 1 unresolved slot,
    #        and that slot has exactly one such unique candidate, assign it
    #     C) SHA1 identical-folder split using sub_usa_disc1.csv sv_model flags
    #     D) forced unique assignment for known impossible-to-disambiguate groups
    # ==========================================================
    candidates_by_number: Dict[int, Set[str]] = {}
    camo_family_cache: Dict[int, Optional[Set[str]]] = {}

    for n, names in number_to_names.items():
        cands = set(names)

        if len(cands) > 1:
            cands = set(resolve_candidates_by_rule(n, cands))

        if len(cands) > 1:
            cands = set(filter_candidates_by_camo_family(n, cands, camo_family_cache))

        if len(cands) > 1:
            cands = set(filter_candidates_prefer_non_dc_camoufla(cands))

        if len(cands) > 1:
            cands = set(filter_candidates_by_sv_model_flag(n, cands, sv_model_subfolders))

        if len(cands) > 1:
            cands = set(filter_candidates_by_memcard_flag(n, cands, memcard_mg_subfolders))

        if len(cands) > 1:
            cands = set(filter_candidates_by_unique_pairs(n, cands, expected_pairs_by_name, pair_index))

        candidates_by_number[n] = cands

    resolved_number_to_name: Dict[int, str] = {}
    resolved_number_comments: Dict[int, str] = {}
    unresolved_numbers: Dict[int, Set[str]] = {}

    taken_names: Set[str] = set()
    remaining_numbers: Set[int] = set(candidates_by_number.keys())

    # ----------------------------------------------------------
    # A) Local greedy propagation until stable.
    # ----------------------------------------------------------
    while True:
        progress = False

        for n in sorted(remaining_numbers):
            cands = candidates_by_number.get(n, set())
            if not cands:
                continue

            cands = {x for x in cands if x not in taken_names}
            candidates_by_number[n] = cands

            if len(cands) == 1:
                chosen = next(iter(cands))
                resolved_number_to_name[n] = chosen
                taken_names.add(chosen)
                progress = True

        if not progress:
            break

        remaining_numbers = {n for n in remaining_numbers if n not in resolved_number_to_name}

    # ----------------------------------------------------------
    # B) Global uniqueness pass until stable.
    # ----------------------------------------------------------
    while True:
        progress = False

        counts: Dict[str, int] = {}
        for n in remaining_numbers:
            for name in candidates_by_number.get(n, set()):
                if name in taken_names:
                    continue
                counts[name] = counts.get(name, 0) + 1

        for n in sorted(remaining_numbers):
            cands = {x for x in candidates_by_number.get(n, set()) if x not in taken_names}
            if len(cands) <= 1:
                continue

            unique_here = [x for x in cands if counts.get(x, 0) == 1]
            if len(unique_here) != 1:
                continue

            chosen = unique_here[0]
            resolved_number_to_name[n] = chosen
            taken_names.add(chosen)
            progress = True

        if not progress:
            break

        remaining_numbers = {n for n in remaining_numbers if n not in resolved_number_to_name}

    # ----------------------------------------------------------
    # C) Special-case SHA1 split:
    # If two unresolved numbers have the exact same 2 candidates,
    # the SLOT folder trees hash identically, and exactly one of the
    # two numeric folders is marked sv_model in sub_usa_disc1.csv,
    # then assign:
    #   - marked sv_model folder  -> sv_model-* candidate
    #   - unmarked partner folder -> non-sv_model candidate
    #
    # Also track which folder numbers got resolved via this split so we can
    # suppress pair_key_in_multiple_folders noise for those.
    # ----------------------------------------------------------
    folder_hash_cache: Dict[int, Optional[str]] = {}
    sha1_split_resolved_numbers: Set[int] = set()

    groups: Dict[frozenset[str], List[int]] = {}
    for n in remaining_numbers:
        cands = set(candidates_by_number.get(n, set()))
        if len(cands) != 2:
            continue

        sv_names = [x for x in cands if starts_with_prefix(x, SV_MODEL_NAME_PREFIX)]
        non_sv_names = [x for x in cands if not starts_with_prefix(x, SV_MODEL_NAME_PREFIX)]

        if len(sv_names) != 1 or len(non_sv_names) != 1:
            continue

        groups.setdefault(frozenset(cands), []).append(n)

    for cset, nums in groups.items():
        if len(nums) != 2:
            continue

        n_a, n_b = sorted(nums)

        h_a = get_slot_folder_hash(n_a, folder_hash_cache)
        h_b = get_slot_folder_hash(n_b, folder_hash_cache)
        if not h_a or not h_b:
            continue

        if h_a != h_b:
            continue

        is_a_sv_folder = n_a in sv_model_subfolders
        is_b_sv_folder = n_b in sv_model_subfolders

        if is_a_sv_folder == is_b_sv_folder:
            continue

        sv_name: Optional[str] = None
        non_sv_name: Optional[str] = None
        for x in cset:
            if starts_with_prefix(x, SV_MODEL_NAME_PREFIX):
                sv_name = x
            else:
                non_sv_name = x

        if not sv_name or not non_sv_name:
            continue

        if sv_name in taken_names or non_sv_name in taken_names:
            continue

        sv_folder_num = n_a if is_a_sv_folder else n_b
        non_sv_folder_num = n_b if is_a_sv_folder else n_a

        resolved_number_to_name[sv_folder_num] = sv_name
        resolved_number_to_name[non_sv_folder_num] = non_sv_name
        taken_names.add(sv_name)
        taken_names.add(non_sv_name)

        sha1_split_resolved_numbers.add(sv_folder_num)
        sha1_split_resolved_numbers.add(non_sv_folder_num)

    remaining_numbers = {n for n in remaining_numbers if n not in resolved_number_to_name}

    # ----------------------------------------------------------
    # D) Forced unique assignment for known impossible-to-disambiguate groups.
    # Numbers sharing the same exact group are assigned in sorted order.
    # This intentionally bypasses the normal taken_names uniqueness guard
    # only insofar as it assigns each name exactly once within the group.
    # ----------------------------------------------------------
    forced_group_to_numbers: Dict[frozenset[str], List[int]] = {}
    for n in remaining_numbers:
        cands = set(candidates_by_number.get(n, set()))
        key = frozenset(cands)
        if key in FORCED_UNIQUE_GROUP_ORDERS:
            forced_group_to_numbers.setdefault(key, []).append(n)

    for group_key, nums in sorted(forced_group_to_numbers.items(), key=lambda item: sorted(item[1])):
        ordered_names = FORCED_UNIQUE_GROUP_ORDERS[group_key]
        nums = sorted(nums)

        if len(nums) != len(ordered_names):
            continue

        if any(name in taken_names for name in ordered_names):
            continue

        for n, chosen in zip(nums, ordered_names):
            resolved_number_to_name[n] = chosen
            resolved_number_comments[n] = build_forced_unique_comment(chosen, ordered_names)
            taken_names.add(chosen)

    remaining_numbers = {n for n in remaining_numbers if n not in resolved_number_to_name}

    for n in sorted(remaining_numbers):
        unresolved_numbers[n] = set(candidates_by_number.get(n, set()))

    resolved_name_to_numbers: Dict[str, Set[int]] = {}
    for n, name in resolved_number_to_name.items():
        resolved_name_to_numbers.setdefault(name, set()).add(n)

    # ==========================================================
    # Write mapping
    # ==========================================================
    out_map = script_dir / OUTPUT_MAPPING_TXT
    with out_map.open("w", encoding="utf-8") as f:
        for n in existing_folders:
            name = resolved_number_to_name.get(n)
            if name:
                comment = resolved_number_comments.get(n, "")
                f.write(f"{n:03d}:{name}{comment}\n")
            else:
                f.write(f"{n:03d}:\n")

    # ==========================================================
    # Write alias mapping
    # Same numeric mapping, but if a name has an alias, write the alias instead
    # and preserve the original resolved name in the comment.
    # ==========================================================
    out_alias_map = script_dir / OUTPUT_ALIAS_MAPPING_TXT
    with out_alias_map.open("w", encoding="utf-8") as f:
        for n in existing_folders:
            original_name = resolved_number_to_name.get(n)
            if not original_name:
                f.write(f"{n:03d}:\n")
                continue

            alias_name = alias_by_original_name.get(original_name)
            base_comment = resolved_number_comments.get(n, "")

            if alias_name:
                alias_comment = f"\t\t\t// {original_name}"
                full_comment = append_comment(base_comment, alias_comment)
                if full_comment:
                    f.write(f"{n:03d}:{alias_name}{full_comment}\n")
                else:
                    f.write(f"{n:03d}:{alias_name}\n")
            else:
                if base_comment:
                    f.write(f"{n:03d}:{original_name}{base_comment}\n")
                else:
                    f.write(f"{n:03d}:{original_name}\n")

    # ==========================================================
    # Conflicts CSV
    # ==========================================================
    out_conflicts = script_dir / OUTPUT_CONFLICTS_CSV
    with out_conflicts.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["type", "key", "values"])

        unresolved_sorted = sorted(
            unresolved_numbers.keys(),
            key=lambda n: (0 if has_camoufla_candidate(unresolved_numbers[n]) else 1, n),
        )

        for n in unresolved_sorted:
            vals = unresolved_numbers[n]
            w.writerow(
                [
                    "number_to_multiple_names_unresolved_filtered",
                    f"{n:03d}",
                    "|".join(sorted(vals)),
                ]
            )

        for name, nums in sorted(resolved_name_to_numbers.items()):
            if len(nums) > 1:
                w.writerow(
                    ["name_to_multiple_numbers", name, ",".join(f"{x:03d}" for x in sorted(nums))]
                )

        for (tri, tex), nums in sorted(pair_index.items()):
            if len(nums) <= 1:
                continue

            if all(n in sha1_split_resolved_numbers for n in nums):
                continue

            w.writerow(
                [
                    "pair_key_in_multiple_folders",
                    f"{tri},{tex}",
                    ",".join(f"{x:03d}" for x in sorted(nums)),
                ]
            )

    # ==========================================================
    # Missing rows CSV
    # ==========================================================
    out_missing = script_dir / OUTPUT_MISSING_CSV
    if missing_rows:
        fieldnames: List[str] = []
        for r in missing_rows:
            for k in r:
                if k not in fieldnames:
                    fieldnames.append(k)

        with out_missing.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in missing_rows:
                w.writerow(r)

    print(f"[INFO] Mapping written -> {out_map.name}")
    print(f"[INFO] Alias mapping written -> {out_alias_map.name}")
    print(f"[INFO] Conflicts written -> {out_conflicts.name}")
    print(f"[INFO] Missing rows: {len(missing_rows):,} -> {out_missing.name if missing_rows else '(none)'}")
    print(f"[INFO] Unresolved number->names conflicts (after filtering): {len(unresolved_numbers):,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())