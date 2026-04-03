import re
from pathlib import Path

INPUT_FILE = Path("bp_assets_flatlist.txt")
MATCH_OUT = Path("mgs3_img_strcode_mappings.txt")
NON_MATCH_OUT = Path("mgs3_texture_strcode_mappings.txt")

# Matches /cache/ OR /resident/ + exactly 8 chars + .img
pattern = re.compile(r"/(cache|resident)/.{8}\.img", re.IGNORECASE)

def main():
    if not INPUT_FILE.exists():
        print(f"Missing input file: {INPUT_FILE}")
        return

    matched_lines = []
    non_matched_lines = []

    with INPUT_FILE.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            stripped = line.rstrip("\r\n")

            if pattern.search(stripped):
                matched_lines.append(stripped)
            else:
                non_matched_lines.append(stripped)

    MATCH_OUT.write_text(
        "\n".join(matched_lines) + ("\n" if matched_lines else ""),
        encoding="utf-8",
        newline="\n"
    )

    NON_MATCH_OUT.write_text(
        "\n".join(non_matched_lines) + ("\n" if non_matched_lines else ""),
        encoding="utf-8",
        newline="\n"
    )

    print("Done.")
    print(f"Matches: {len(matched_lines)}")
    print(f"Non-matches: {len(non_matched_lines)}")

if __name__ == "__main__":
    main()