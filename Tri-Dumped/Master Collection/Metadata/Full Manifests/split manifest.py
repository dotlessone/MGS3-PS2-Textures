import os

INPUT_FILE = "manifest_all.txt"
OUTPUT_IMG = "manifest_img.txt"
OUTPUT_TRI = "manifest_tri.txt"


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(script_dir, INPUT_FILE)
    output_img_path = os.path.join(script_dir, OUTPUT_IMG)
    output_tri_path = os.path.join(script_dir, OUTPUT_TRI)

    if not os.path.isfile(input_path):
        print(f"Input file not found: {input_path}")
        return

    img_lines = []
    tri_lines = []

    with open(input_path, "r", encoding="utf-8", newline="") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue

            if stripped.startswith("assets/img/"):
                img_lines.append(stripped)
            elif stripped.startswith("assets/tri/"):
                tri_lines.append(stripped)

    # Write outputs (LF endings, deterministic)
    with open(output_img_path, "w", encoding="utf-8", newline="\n") as f:
        for line in img_lines:
            f.write(line + "\n")

    with open(output_tri_path, "w", encoding="utf-8", newline="\n") as f:
        for line in tri_lines:
            f.write(line + "\n")

    print(f"Done.")
    print(f"IMG lines: {len(img_lines)} -> {output_img_path}")
    print(f"TRI lines: {len(tri_lines)} -> {output_tri_path}")


if __name__ == "__main__":
    main()