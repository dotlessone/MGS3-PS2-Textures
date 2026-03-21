from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


PS2_CSV = Path(r"C:\Development\Git\MGS3-PS2-Textures\u - dumped from subsistence\mgs3_ps2_dimensions.csv")
MC_CSV = Path(r"C:\Development\Git\MGS3-PS2-Textures\u - dumped from subsistence\mgs3_mc_dimensions.csv")

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


def write_log(path: Path, names: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        for name in names:
            f.write(f"{name}\n")


def main() -> None:
    if not PS2_CSV.is_file():
        raise FileNotFoundError(f"Missing PS2 CSV: {PS2_CSV}")

    if not MC_CSV.is_file():
        raise FileNotFoundError(f"Missing MC CSV: {MC_CSV}")

    ps2_rows = load_ps2_rows(PS2_CSV)
    mc_rows = load_mc_rows(MC_CSV)

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

    write_log(BP_REMADE_LOG, bp_remade_textures)
    write_log(MC_EXCEEDS_TRI_ACTUAL_LOG, mc_exceeds_tri_actual_dimensions)
    write_log(MC_SMALLER_THAN_TRI_ACTUAL_LOG, mc_smaller_than_tri_actual_dimensions)
    write_log(MC_UI_LOG, mc_ui_textures)
    write_log(MC_MISSING_FROM_PS2_LOG, mc_missing_from_ps2)

    missing_in_mc = sorted(ps2_texture_names - mc_texture_names)

    print(f"Matched texture names: {len(common_texture_names)}")
    print(f"Wrote: {BP_REMADE_LOG} ({len(bp_remade_textures)} entries)")
    print(f"Wrote: {MC_EXCEEDS_TRI_ACTUAL_LOG} ({len(mc_exceeds_tri_actual_dimensions)} entries)")
    print(f"Wrote: {MC_SMALLER_THAN_TRI_ACTUAL_LOG} ({len(mc_smaller_than_tri_actual_dimensions)} entries)")
    print(f"Wrote: {MC_UI_LOG} ({len(mc_ui_textures)} entries)")
    print(f"Wrote: {MC_MISSING_FROM_PS2_LOG} ({len(mc_missing_from_ps2)} entries)")
    print(f"Missing in MC CSV: {len(missing_in_mc)}")


if __name__ == "__main__":
    main()