import os

INPUT_FILE = "bp_assets_all.txt"
OUTPUT_FILE = "bp_assets_flatlist.txt"
MATCH = "textures/flatlist/"


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(script_dir, INPUT_FILE)
    output_path = os.path.join(script_dir, OUTPUT_FILE)

    if not os.path.isfile(input_path):
        print(f"Input file not found: {input_path}")
        return

    matched_lines = []

    with open(input_path, "r", encoding="utf-8", newline="") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue

            if MATCH in stripped:
                matched_lines.append(stripped)

    # Write output (LF endings, deterministic)
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        for line in matched_lines:
            f.write(line + "\n")

    print(f"Done.")
    print(f"Matched lines: {len(matched_lines)} -> {output_path}")


if __name__ == "__main__":
    main()