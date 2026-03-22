from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


PS2_CSV = Path(r"C:\Development\Git\MGS3-PS2-Textures\u - dumped from subsistence\mgs3_ps2_dimensions.csv")
MC_CSV = Path(r"C:\Development\Git\MGS3-PS2-Textures\u - dumped from subsistence\mgs3_mc_dimensions.csv")
TEXTURE_MAP_CSV = Path(r"C:\Development\Git\MGS3-PS2-Textures\u - dumped from subsistence\mgs3_texture_map.csv")
SLOT_MAP_CSV = Path(r"C:\Development\Git\MGS3-PS2-Textures\u - dumped from subsistence\mgs3_us_sub_slot_mappings.csv")

OUTPUT_DIR = Path(__file__).resolve().parent

BP_REMADE_LOG = OUTPUT_DIR / "mgs3_bp_remade_textures.log"
MC_EXCEEDS_TRI_ACTUAL_LOG = OUTPUT_DIR / "mgs3_mc_stretched_npots.log"
MC_SMALLER_THAN_TRI_ACTUAL_LOG = OUTPUT_DIR / "mgs3_mc_mismatched.log"
MC_UI_LOG = OUTPUT_DIR / "mgs3_mc_ui.txt"
MC_MISSING_FROM_PS2_LOG = OUTPUT_DIR / "mgs3_mc_missing_from_ps2.log"


@dataclass(frozen=True)
class Ps2Row:
    texture_name: str
    tri_dumped_width: int
    tri_dumped_height: int
    tri_dumped_width_pow2: int
    tri_dumped_height_pow2: int


@dataclass(frozen=True)
class McRow:
    texture_name: str
    mc_width: int
    mc_height: int


@dataclass(frozen=True)
class TextureMapRow:
    texture_filename: str
    stage: str
    tri_strcode: str
    texture_strcode: str


def parse_int(row: dict[str, str], key: str, csv_path: Path) -> int:
    value = (row.get(key) or "").strip()
    if not value:
        raise ValueError(f"Missing integer field '{key}' in {csv_path}")
    return int(value)


def is_power_of_two(x: int) -> bool:
    return x > 0 and (x & (x - 1)) == 0


def is_npot(width: int, height: int) -> bool:
    return not (is_power_of_two(width) and is_power_of_two(height))


def load_ps2_rows(csv_path: Path) -> dict[str, Ps2Row]:
    rows: dict[str, Ps2Row] = {}

    with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw_row in reader:
            texture_name = (raw_row.get("texture_name") or "").strip()
            if not texture_name:
                continue

            rows[texture_name] = Ps2Row(
                texture_name=texture_name,
                tri_dumped_width=parse_int(raw_row, "tri_dumped_width", csv_path),
                tri_dumped_height=parse_int(raw_row, "tri_dumped_height", csv_path),
                tri_dumped_width_pow2=parse_int(raw_row, "tri_dumped_width_pow2", csv_path),
                tri_dumped_height_pow2=parse_int(raw_row, "tri_dumped_height_pow2", csv_path),
            )

    return rows


def load_mc_rows(csv_path: Path) -> dict[str, McRow]:
    rows: dict[str, McRow] = {}

    with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw_row in reader:
            texture_name = (raw_row.get("texture_name") or "").strip()
            if not texture_name:
                continue

            rows[texture_name] = McRow(
                texture_name=texture_name,
                mc_width=parse_int(raw_row, "mc_width", csv_path),
                mc_height=parse_int(raw_row, "mc_height", csv_path),
            )

    return rows


def load_texture_map_rows(csv_path: Path) -> list[TextureMapRow]:
    rows: list[TextureMapRow] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)

        for raw_row in reader:
            if not raw_row:
                continue

            first = (raw_row[0] or "").strip()
            if not first:
                continue

            if first.startswith(";"):
                continue

            if len(raw_row) < 4:
                continue

            texture_filename = raw_row[0].strip()
            stage = raw_row[1].strip()
            tri_strcode = raw_row[2].strip()
            texture_strcode = raw_row[3].strip()

            if not texture_filename or not stage or not tri_strcode or not texture_strcode:
                continue

            rows.append(
                TextureMapRow(
                    texture_filename=texture_filename,
                    stage=stage,
                    tri_strcode=tri_strcode,
                    texture_strcode=texture_strcode,
                )
            )

    return rows


def strip_slot_comment(value: str) -> str:
    return value.split("//", 1)[0].strip()


def load_slot_mappings(path: Path) -> dict[str, tuple[str, str]]:
    mapping: dict[str, tuple[str, str]] = {}

    with path.open("r", encoding="utf-8-sig") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or ":" not in line:
                continue

            left, right = line.split(":", 1)

            slot_id = left.strip()
            stage_name = strip_slot_comment(right)

            if not slot_id or not stage_name:
                continue

            mapping[stage_name] = (slot_id, stage_name)

    return mapping


def write_log(path: Path, names: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        for name in names:
            f.write(f"{name}\n")


def format_stage_header(
    stage: str,
    stage_count: int,
    slot_mappings: dict[str, tuple[str, str]],
) -> str:
    slot_info = slot_mappings.get(stage)
    if slot_info is not None:
        slot_id, stage_name = slot_info
        return f"=== SLOT: {slot_id} [{stage_name}] ({stage_count} mismatched) ==="

    return f"=== STAGE: {stage} ({stage_count} mismatched) ==="


def write_grouped_stage_section(
    f,
    title: str,
    stages_to_write: list[tuple[str, int]],
    grouped: dict[str, dict[str, list[TextureMapRow]]],
    slot_mappings: dict[str, tuple[str, str]],
) -> None:
    f.write(f"{title}:\n\n")

    if not stages_to_write:
        f.write("(none)\n\n")
        return

    for stage, stage_count in stages_to_write:
        f.write(f"{format_stage_header(stage, stage_count, slot_mappings)}\n")

        tris = grouped[stage]
        for tri in sorted(tris):
            entries = tris[tri]
            tri_count = len(entries)

            f.write(f"  -- TRI: {tri} ({tri_count}) --\n")

            entries_sorted = sorted(
                entries,
                key=lambda r: (r.texture_filename.lower(), r.texture_strcode),
            )

            for r in entries_sorted:
                f.write(f"    {r.texture_strcode} | {r.texture_filename}\n")

            f.write("\n")

        f.write("\n")


def write_mismatched_log(
    path: Path,
    mismatched_names: list[str],
    matching_texture_map_rows: list[TextureMapRow],
    slot_mappings: dict[str, tuple[str, str]],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        for name in mismatched_names:
            f.write(f"{name}\n")

        f.write("\n----------\n\n")

        grouped: dict[str, dict[str, list[TextureMapRow]]] = defaultdict(lambda: defaultdict(list))
        stage_counts: dict[str, int] = defaultdict(int)

        for row in matching_texture_map_rows:
            grouped[row.stage][row.tri_strcode].append(row)
            stage_counts[row.stage] += 1

        sorted_stage_counts = sorted(stage_counts.items(), key=lambda x: (-x[1], x[0]))

        normal_stages = [(stage, count) for stage, count in sorted_stage_counts if stage not in slot_mappings]
        slot_stages = [(stage, count) for stage, count in sorted_stage_counts if stage in slot_mappings]

        f.write("Stage mismatch counts:\n")
        for stage, count in normal_stages:
            f.write(f"  STAGE: {stage}: {count}\n")
        for stage, count in slot_stages:
            slot_id, stage_name = slot_mappings[stage]
            f.write(f"  SLOT: {slot_id} [{stage_name}]: {count}\n")

        f.write("\n----------\n\n")

        write_grouped_stage_section(
            f,
            "NORMAL STAGES",
            normal_stages,
            grouped,
            slot_mappings,
        )

        f.write("----------\n\n")

        write_grouped_stage_section(
            f,
            "SLOTS",
            slot_stages,
            grouped,
            slot_mappings,
        )


def main() -> None:
    if not PS2_CSV.is_file():
        raise FileNotFoundError(f"Missing PS2 CSV: {PS2_CSV}")

    if not MC_CSV.is_file():
        raise FileNotFoundError(f"Missing MC CSV: {MC_CSV}")

    if not TEXTURE_MAP_CSV.is_file():
        raise FileNotFoundError(f"Missing texture map CSV: {TEXTURE_MAP_CSV}")

    if not SLOT_MAP_CSV.is_file():
        raise FileNotFoundError(f"Missing slot mapping CSV: {SLOT_MAP_CSV}")

    ps2_rows = load_ps2_rows(PS2_CSV)
    mc_rows = load_mc_rows(MC_CSV)
    texture_map_rows = load_texture_map_rows(TEXTURE_MAP_CSV)
    slot_mappings = load_slot_mappings(SLOT_MAP_CSV)

    ps2_texture_names = set(ps2_rows.keys())
    mc_texture_names = set(mc_rows.keys())
    common_texture_names = sorted(ps2_texture_names & mc_texture_names)

    bp_remade_textures: list[str] = []
    mc_exceeds_tri_actual_dimensions: list[str] = []
    mc_smaller_than_tri_actual_dimensions: list[str] = []
    mc_ui_textures: list[str] = []
    mc_missing_from_ps2: list[str] = sorted(mc_texture_names - ps2_texture_names)

    for texture_name in common_texture_names:
        ps2 = ps2_rows[texture_name]
        mc = mc_rows[texture_name]

        mc_is_npot = is_npot(mc.mc_width, mc.mc_height)

        if mc_is_npot:
            mc_ui_textures.append(texture_name)

        exceeds_pow2 = (
            mc.mc_width > ps2.tri_dumped_width_pow2
            or mc.mc_height > ps2.tri_dumped_height_pow2
        )

        exceeds_actual = (
            mc.mc_width > ps2.tri_dumped_width
            or mc.mc_height > ps2.tri_dumped_height
        )

        smaller_than_actual = (
            mc.mc_width < ps2.tri_dumped_width
            or mc.mc_height < ps2.tri_dumped_height
        )

        if exceeds_pow2:
            bp_remade_textures.append(texture_name)

        if exceeds_actual and not exceeds_pow2 and not mc_is_npot:
            mc_exceeds_tri_actual_dimensions.append(texture_name)

        if smaller_than_actual:
            mc_smaller_than_tri_actual_dimensions.append(texture_name)

    mismatched_name_set = set(mc_smaller_than_tri_actual_dimensions)

    matching_texture_map_rows = [
        row
        for row in texture_map_rows
        if row.texture_filename in mismatched_name_set
    ]

    write_log(BP_REMADE_LOG, bp_remade_textures)
    write_log(MC_EXCEEDS_TRI_ACTUAL_LOG, mc_exceeds_tri_actual_dimensions)
    write_mismatched_log(
        MC_SMALLER_THAN_TRI_ACTUAL_LOG,
        mc_smaller_than_tri_actual_dimensions,
        matching_texture_map_rows,
        slot_mappings,
    )
    write_log(MC_UI_LOG, mc_ui_textures)
    write_log(MC_MISSING_FROM_PS2_LOG, mc_missing_from_ps2)

    missing_in_mc = sorted(ps2_texture_names - mc_texture_names)

    print(f"Matched texture names: {len(common_texture_names)}")
    print(f"Wrote: {BP_REMADE_LOG} ({len(bp_remade_textures)} entries)")
    print(f"Wrote: {MC_EXCEEDS_TRI_ACTUAL_LOG} ({len(mc_exceeds_tri_actual_dimensions)} entries)")
    print(
        f"Wrote: {MC_SMALLER_THAN_TRI_ACTUAL_LOG} "
        f"({len(mc_smaller_than_tri_actual_dimensions)} texture names, "
        f"{len(matching_texture_map_rows)} texture map rows)"
    )
    print(f"Wrote: {MC_UI_LOG} ({len(mc_ui_textures)} entries)")
    print(f"Wrote: {MC_MISSING_FROM_PS2_LOG} ({len(mc_missing_from_ps2)} entries)")
    print(f"Missing in MC CSV: {len(missing_in_mc)}")


if __name__ == "__main__":
    main()