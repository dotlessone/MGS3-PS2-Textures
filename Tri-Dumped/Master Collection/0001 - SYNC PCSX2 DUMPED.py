from __future__ import annotations

import csv
import hashlib
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import subprocess
import sys

# ==========================================================
# CONFIG
# ==========================================================
MAPPINGS_CSV = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_img_strcode_mappings.csv"
)
USA_ROOT = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\PCSX2 Dumped\Subsistence\USA"
)
MC_ROOT = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection"
)

MAX_WORKERS = max(4, os.cpu_count() or 4)
SHA1_BUFFER_SIZE = 8 * 1024 * 1024

SKIP_MANAGED_STEMS = {
    "sna_face_def.bmp_bbe58170874ef112ad7f8269143d4430",
    "sna_face_def.bmp",
}

SCRIPT_DIR = Path(__file__).resolve().parent

# ==========================================================
# HELPERS
# ==========================================================
def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(SHA1_BUFFER_SIZE), b""):
            h.update(chunk)

    return h.hexdigest()


def iter_pngs_recursive(folder: Path):
    for path in folder.rglob("*.png"):
        if path.is_file():
            yield path


def texture_stem_from_png(path: Path) -> str:
    return path.stem.lower()


def read_managed_stems(path: Path) -> set[str]:
    managed_stems: set[str] = set()

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required = {"texture_stem", "region_folder", "stage", "img_strcode"}
        missing = required.difference(reader.fieldnames or [])

        if missing:
            raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

        for csv_row in reader:
            texture_stem = (csv_row["texture_stem"] or "").strip().lower()

            if not texture_stem:
                raise ValueError(f"Invalid row in mappings CSV: {csv_row}")

            if texture_stem in SKIP_MANAGED_STEMS:
                continue

            managed_stems.add(texture_stem)

    return managed_stems


def atomic_copy2(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        dir=str(dst.parent),
        prefix=f"{dst.name}.",
        suffix=".tmp",
        delete=False,
    ) as tmp:
        tmp_path = Path(tmp.name)

    try:
        shutil.copyfile(src, tmp_path)
        shutil.copystat(src, tmp_path)
        os.replace(tmp_path, dst)
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        raise


def build_unique_usa_index(root: Path) -> dict[str, Path]:
    pngs = list(iter_pngs_recursive(root))

    if not pngs:
        return {}

    stem_to_paths: dict[str, list[Path]] = {}

    for path in pngs:
        stem = texture_stem_from_png(path)
        stem_to_paths.setdefault(stem, []).append(path)

    duplicates = {stem: paths for stem, paths in stem_to_paths.items() if len(paths) > 1}

    if duplicates:
        lines = ["Duplicate USA texture_stem values found:"]

        for stem in sorted(duplicates):
            lines.append(f"  {stem}")

            for dup_path in sorted(duplicates[stem]):
                lines.append(f"    {dup_path}")

        raise RuntimeError("\n".join(lines))

    return {stem: paths[0] for stem, paths in stem_to_paths.items()}


def build_mc_index(root: Path) -> tuple[dict[str, list[Path]], dict[Path, str]]:
    pngs = list(iter_pngs_recursive(root))
    by_stem: dict[str, list[Path]] = {}
    by_path: dict[Path, str] = {}

    for path in pngs:
        stem = texture_stem_from_png(path)
        by_stem.setdefault(stem, []).append(path)
        by_path[path] = stem

    return by_stem, by_path


def choose_new_mc_target_path(stem: str, mc_by_stem: dict[str, list[Path]]) -> Path:
    existing_paths = mc_by_stem.get(stem, [])
    if existing_paths:
        return sorted(existing_paths, key=lambda p: str(p).lower())[0]

    return MC_ROOT / f"{stem}.png"


def sync_target(source_path: Path, target_path: Path) -> str:
    if not target_path.exists():
        atomic_copy2(source_path, target_path)
        return "created"

    source_sha1 = sha1_of_file(source_path)
    target_sha1 = sha1_of_file(target_path)

    if source_sha1 == target_sha1:
        return "unchanged"

    atomic_copy2(source_path, target_path)
    return "updated"


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    if not MAPPINGS_CSV.is_file():
        raise FileNotFoundError(f"Missing mappings CSV: {MAPPINGS_CSV}")

    if not USA_ROOT.is_dir():
        raise FileNotFoundError(f"Missing USA root: {USA_ROOT}")

    if not MC_ROOT.is_dir():
        raise FileNotFoundError(f"Missing Master Collection root: {MC_ROOT}")

    managed_stems = read_managed_stems(MAPPINGS_CSV)
    usa_by_stem = build_unique_usa_index(USA_ROOT)
    mc_by_stem, mc_by_path = build_mc_index(MC_ROOT)

    errors: list[str] = []

    # ------------------------------------------------------
    # 1. Remove managed MC PNGs whose stem is no longer in USA
    # ------------------------------------------------------
    orphan_paths = sorted(
        path
        for path, stem in mc_by_path.items()
        if stem in managed_stems and stem not in usa_by_stem
    )

    removed_count = 0

    for orphan_path in orphan_paths:
        try:
            orphan_path.unlink()
            removed_count += 1
            print(f"[REMOVED ORPHAN] {orphan_path}")
        except Exception as exc:
            errors.append(f"Failed to remove orphan: {orphan_path} -> {exc}")

    # Rebuild MC index after deletions
    mc_by_stem, mc_by_path = build_mc_index(MC_ROOT)

    # ------------------------------------------------------
    # 2. For managed stems present in USA:
    #    - copy to MC if absent
    #    - replace MC if different
    #    - leave alone if same
    # ------------------------------------------------------
    created_count = 0
    updated_count = 0
    unchanged_count = 0

    futures = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for stem in sorted(managed_stems):
            source_path = usa_by_stem.get(stem)

            if source_path is None:
                continue

            target_paths = mc_by_stem.get(stem, [])

            if not target_paths:
                target_path = choose_new_mc_target_path(stem, mc_by_stem)
                futures[executor.submit(sync_target, source_path, target_path)] = (
                    stem,
                    source_path,
                    target_path,
                )
                continue

            for target_path in target_paths:
                futures[executor.submit(sync_target, source_path, target_path)] = (
                    stem,
                    source_path,
                    target_path,
                )

        for future in as_completed(futures):
            stem, source_path, target_path = futures[future]

            try:
                result = future.result()

                if result == "created":
                    created_count += 1
                    print(f"[CREATED] {target_path} <- {source_path}")
                    continue

                if result == "updated":
                    updated_count += 1
                    print(f"[UPDATED] {target_path} <- {source_path}")
                    continue

                if result == "unchanged":
                    unchanged_count += 1
                    continue

                errors.append(f"Unexpected sync result for {target_path}: {result}")

            except Exception as exc:
                errors.append(f"Failed to sync {target_path} from {source_path}: {exc}")

    print("\nDone.")
    print(f"Managed stems:     {len(managed_stems)}")
    print(f"Skipped stems:     {len(SKIP_MANAGED_STEMS)}")
    print(f"USA source PNGs:   {len(usa_by_stem)}")
    print(f"MC PNGs indexed:   {len(mc_by_path)}")
    print(f"Removed orphans:   {removed_count}")
    print(f"Created targets:   {created_count}")
    print(f"Updated targets:   {updated_count}")
    print(f"Unchanged targets: {unchanged_count}")
    print(f"Errors:            {len(errors)}")

    if errors:
        print("\nErrors:")
        for msg in errors:
            print(msg)
        return 1
        
    # ------------------------------------------------------
    # 4. Run metadata generation script
    # ------------------------------------------------------
    script_path = SCRIPT_DIR / "generate_tri_dumped_metadata.py"

    if not script_path.is_file():
        print(f"\n[WARNING] Missing script: {script_path}")
    else:
        print("\nRunning generate_tri_dumped_metadata.py...")

        try:
            result = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(SCRIPT_DIR),
            )

            if result.returncode != 0:
                print("[ERROR] generate_tri_dumped_metadata.py failed")
                return result.returncode

            print("[OK] Metadata generation complete")

        except Exception as exc:
            print(f"[ERROR] Failed to run metadata script: {exc}")
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())