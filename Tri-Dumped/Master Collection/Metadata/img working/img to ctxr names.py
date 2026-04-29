from __future__ import annotations

import csv
import hashlib
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock


IMG_MAPPINGS_CSV = Path(r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_img_mappings.csv")
IMG_STRCODE_MAPPINGS_CSV = Path(r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_img_strcode_mappings.csv")
IMG_WORKING_ROOT = Path(r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\img working")

SCRIPT_DIR = Path(__file__).resolve().parent

DRY_RUN = False
MAX_WORKERS = os.cpu_count() or 8

LOG_CSV = SCRIPT_DIR / "rename_and_collect_img_working_pngs_log.csv"

log_lock = Lock()
hash_lock = Lock()
move_lock = Lock()
sha1_cache: dict[Path, str] = {}


def wait_for_input_and_exit(message: str, code: int = 1) -> None:
    print(f"\n[ERROR] {message}")
    input("Press Enter to exit...")
    sys.exit(code)


def norm(value: str) -> str:
    return value.strip().lower().replace("\\", "/")


def sha1_file(path: Path) -> str:
    resolved = path.resolve()

    with hash_lock:
        cached = sha1_cache.get(resolved)
        if cached is not None:
            return cached

    h = hashlib.sha1()
    with resolved.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)

    digest = h.hexdigest()

    with hash_lock:
        sha1_cache[resolved] = digest

    return digest


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def img_strcode_region_for_img_mapping_region(region: str) -> str:
    if norm(region) == "us":
        return "sp"

    return norm(region)


def build_target_map() -> dict[tuple[str, str], set[str]]:
    img_rows = read_csv(IMG_MAPPINGS_CSV)
    strcode_rows = read_csv(IMG_STRCODE_MAPPINGS_CSV)

    strcode_lookup: dict[tuple[str, str, str], set[str]] = {}

    for row in strcode_rows:
        region = norm(row["region_folder"])
        stage = norm(row["stage"])
        img_strcode = norm(row["img_strcode"])
        texture_stem = row["texture_stem"].strip()

        strcode_lookup.setdefault((region, stage, img_strcode), set()).add(texture_stem)

    target_map: dict[tuple[str, str], set[str]] = {}

    for row in img_rows:
        mc_image_file_name = row["mc_image_file_name"].strip()
        img_mapping_region = norm(row["region_folder"])
        strcode_mapping_region = img_strcode_region_for_img_mapping_region(img_mapping_region)
        stage = norm(row["stage"])
        texture_strcode = norm(row["texture_strcode"])

        targets = strcode_lookup.get((strcode_mapping_region, stage, texture_strcode), set())

        if targets:
            target_map.setdefault((img_mapping_region, norm(mc_image_file_name)), set()).update(targets)

    return target_map


def get_region_folder(path: Path) -> str:
    return path.relative_to(IMG_WORKING_ROOT).parts[0]


def append_log(row: dict[str, str]) -> None:
    with log_lock:
        exists = LOG_CSV.exists()

        with LOG_CSV.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "status",
                    "source",
                    "renamed_path",
                    "collected_path",
                    "source_sha1",
                    "conflict_sha1",
                    "region_folder",
                    "mc_image_file_name",
                    "resolved_texture_stem",
                    "message",
                ],
            )

            if not exists:
                writer.writeheader()

            writer.writerow(row)


def collect_or_dedupe(path: Path, region: str, mc_image_file_name: str, texture_stem: str) -> str:
    collected_path = SCRIPT_DIR / path.name

    if path.resolve() == collected_path.resolve():
        append_log({
            "status": "already_collected",
            "source": str(path),
            "renamed_path": str(path),
            "collected_path": str(collected_path),
            "source_sha1": "",
            "conflict_sha1": "",
            "region_folder": region,
            "mc_image_file_name": mc_image_file_name,
            "resolved_texture_stem": texture_stem,
            "message": "File is already in the script folder.",
        })
        return "already_collected"

    with move_lock:
        if collected_path.exists():
            source_sha1 = sha1_file(path)
            conflict_sha1 = sha1_file(collected_path)

            if source_sha1 != conflict_sha1:
                raise RuntimeError(
                    "\n".join([
                        "Non-SHA1-matched collection conflict:",
                        f"  Source:       {path}",
                        f"  Destination:  {collected_path}",
                        f"  Source SHA1:  {source_sha1}",
                        f"  Dest SHA1:    {conflict_sha1}",
                    ])
                )

            if not DRY_RUN:
                path.unlink()

            append_log({
                "status": "would_delete_duplicate" if DRY_RUN else "deleted_duplicate",
                "source": str(path),
                "renamed_path": str(path),
                "collected_path": str(collected_path),
                "source_sha1": source_sha1,
                "conflict_sha1": conflict_sha1,
                "region_folder": region,
                "mc_image_file_name": mc_image_file_name,
                "resolved_texture_stem": texture_stem,
                "message": "Destination already exists with matching SHA1.",
            })
            return "would_delete_duplicate" if DRY_RUN else "deleted_duplicate"

        if not DRY_RUN:
            path.rename(collected_path)

        append_log({
            "status": "would_collect" if DRY_RUN else "collected",
            "source": str(path),
            "renamed_path": str(path),
            "collected_path": str(collected_path),
            "source_sha1": "",
            "conflict_sha1": "",
            "region_folder": region,
            "mc_image_file_name": mc_image_file_name,
            "resolved_texture_stem": texture_stem,
            "message": "",
        })
        return "would_collect" if DRY_RUN else "collected"


def process_png(path: Path, target_map: dict[tuple[str, str], set[str]]) -> str:
    region = get_region_folder(path)
    mc_image_file_name = path.name[:-4]

    targets = target_map.get((norm(region), norm(mc_image_file_name)))

    if not targets:
        append_log({
            "status": "missing_mapping",
            "source": str(path),
            "renamed_path": "",
            "collected_path": "",
            "source_sha1": "",
            "conflict_sha1": "",
            "region_folder": region,
            "mc_image_file_name": mc_image_file_name,
            "resolved_texture_stem": "",
            "message": "No resolved target from CSV mappings. Left in place.",
        })
        return "missing_mapping"

    if len(targets) != 1:
        append_log({
            "status": "ambiguous_mapping",
            "source": str(path),
            "renamed_path": "",
            "collected_path": "",
            "source_sha1": "",
            "conflict_sha1": "",
            "region_folder": region,
            "mc_image_file_name": mc_image_file_name,
            "resolved_texture_stem": " | ".join(sorted(targets)),
            "message": "Multiple possible texture_stem targets. Left in place.",
        })
        return "ambiguous_mapping"

    texture_stem = next(iter(targets))
    renamed_path = path.with_name(f"{texture_stem}.png")

    if path != renamed_path:
        if renamed_path.exists():
            source_sha1 = sha1_file(path)
            conflict_sha1 = sha1_file(renamed_path)

            if source_sha1 != conflict_sha1:
                raise RuntimeError(
                    "\n".join([
                        "Non-SHA1-matched same-region rename conflict:",
                        f"  Region:       {region}",
                        f"  Source:       {path}",
                        f"  Target:       {renamed_path}",
                        f"  Source SHA1:  {source_sha1}",
                        f"  Target SHA1:  {conflict_sha1}",
                    ])
                )

            if not DRY_RUN:
                path.unlink()

            append_log({
                "status": "would_delete_same_region_duplicate" if DRY_RUN else "deleted_same_region_duplicate",
                "source": str(path),
                "renamed_path": str(renamed_path),
                "collected_path": "",
                "source_sha1": source_sha1,
                "conflict_sha1": conflict_sha1,
                "region_folder": region,
                "mc_image_file_name": mc_image_file_name,
                "resolved_texture_stem": texture_stem,
                "message": "Same-region target already exists with matching SHA1.",
            })

            return collect_or_dedupe(renamed_path, region, mc_image_file_name, texture_stem)

        if not DRY_RUN:
            path.rename(renamed_path)

        append_log({
            "status": "would_rename" if DRY_RUN else "renamed",
            "source": str(path),
            "renamed_path": str(renamed_path),
            "collected_path": "",
            "source_sha1": "",
            "conflict_sha1": "",
            "region_folder": region,
            "mc_image_file_name": mc_image_file_name,
            "resolved_texture_stem": texture_stem,
            "message": "",
        })
    else:
        renamed_path = path

    return collect_or_dedupe(renamed_path, region, mc_image_file_name, texture_stem)


def main() -> None:
    try:
        if LOG_CSV.exists():
            LOG_CSV.unlink()

        target_map = build_target_map()

        pngs = [
            p for p in IMG_WORKING_ROOT.rglob("*.png")
            if p.is_file() and p.parent != IMG_WORKING_ROOT
        ]

        print(f"[INFO] Found {len(pngs)} PNG files")
        print(f"[INFO] DRY_RUN = {DRY_RUN}")
        print(f"[INFO] SCRIPT_DIR = {SCRIPT_DIR}")
        print(f"[INFO] Log: {LOG_CSV}")

        counts: dict[str, int] = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_png, png, target_map) for png in pngs]

            for i, future in enumerate(as_completed(futures), 1):
                status = future.result()
                counts[status] = counts.get(status, 0) + 1

                if i % 100 == 0 or i == len(futures):
                    print(f"[INFO] Processed {i}/{len(futures)}")

        print("[DONE]")
        for status, count in sorted(counts.items()):
            print(f"{status}: {count}")

    except Exception as e:
        wait_for_input_and_exit(str(e))


if __name__ == "__main__":
    main()