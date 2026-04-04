from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path
from typing import Dict, Tuple, Set


SCRIPT_DIR = Path(__file__).resolve().parent

MC_CSV = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_mc_dimensions.csv"
)
PS2_CSV = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_mc_tri_dumped_metadata.csv"
)

MANUAL_BP_REMADE_TXT = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_mc_manually_identified_bp_remade.txt"
)

NPOTS_TXT = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_mc_npots.txt"
)

OUTPUT_TXT = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_mc_bp_remade_textures.txt"
)

OUTPUT_NO_UI_TXT = Path(
    r"C:\Development\Git\MGS3-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs3_mc_bp_remade_textures_no_ui.txt"
)

IDENTIFY_MC_NPOTS_SCRIPT = SCRIPT_DIR / "identify mc npots.py"


def normalize(name: str) -> str:
    return name.strip().lower()


def ceil_pow2(value: int) -> int:
    if value <= 0:
        raise ValueError(f"Invalid dimension: {value}")
    return 1 << (value - 1).bit_length()


def load_mc(csv_path: Path) -> Dict[str, Tuple[int, int]]:
    out: Dict[str, Tuple[int, int]] = {}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = normalize(row["texture_name"])
            out[name] = (
                int(row["mc_width"]),
                int(row["mc_height"]),
            )

    return out


def load_ps2(csv_path: Path) -> Dict[str, Tuple[int, int]]:
    out: Dict[str, Tuple[int, int]] = {}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = normalize(row["texture_name"])
            out[name] = (
                int(row["mc_tri_dumped_width"]),
                int(row["mc_tri_height"]),
            )

    return out


def load_name_list(path: Path) -> Set[str]:
    out: Set[str] = set()

    if not path.exists():
        return out

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith(("#", ";", "//")):
                continue

            out.add(normalize(line))

    return out


def write_name_list(path: Path, names: Set[str]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for name in sorted(names):
            f.write(name + "\n")


def run_identify_mc_npots() -> None:
    if not IDENTIFY_MC_NPOTS_SCRIPT.exists():
        raise FileNotFoundError(
            f"Required script not found: {IDENTIFY_MC_NPOTS_SCRIPT}"
        )

    print(f"Running: {IDENTIFY_MC_NPOTS_SCRIPT}")

    subprocess.run(
        [sys.executable, str(IDENTIFY_MC_NPOTS_SCRIPT)],
        check=True,
        cwd=str(IDENTIFY_MC_NPOTS_SCRIPT.parent),
    )


def main() -> None:
    run_identify_mc_npots()

    mc_data = load_mc(MC_CSV)
    ps2_data = load_ps2(PS2_CSV)

    bp_remade_names: Set[str] = set()

    # normalize keys again defensively before intersection
    mc_keys = {normalize(k) for k in mc_data.keys()}
    ps2_keys = {normalize(k) for k in ps2_data.keys()}

    for name in mc_keys & ps2_keys:
        mc_w, mc_h = mc_data[name]
        ps2_w, ps2_h = ps2_data[name]

        if (
            mc_w != ceil_pow2(ps2_w)
            or mc_h != ceil_pow2(ps2_h)
        ):
            bp_remade_names.add(name)

    # Add manually identified BP remade textures
    manual_names = load_name_list(MANUAL_BP_REMADE_TXT)
    bp_remade_names |= manual_names

    # no_ui version excludes all stems listed in mgs3_mc_npots.txt
    npot_names = load_name_list(NPOTS_TXT)
    bp_remade_names_no_ui = bp_remade_names - npot_names

    write_name_list(OUTPUT_TXT, bp_remade_names)
    write_name_list(OUTPUT_NO_UI_TXT, bp_remade_names_no_ui)

    print(f"Wrote {len(bp_remade_names)} texture names to:")
    print(OUTPUT_TXT)
    print()
    print(f"Wrote {len(bp_remade_names_no_ui)} texture names to:")
    print(OUTPUT_NO_UI_TXT)


if __name__ == "__main__":
    main()