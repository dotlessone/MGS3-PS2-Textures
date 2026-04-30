import shutil
from pathlib import Path


def move_contents(src: Path, dst: Path):
    for item in src.iterdir():
        target = dst / item.name

        if item.is_dir():
            if target.exists():
                move_contents(item, target)
                try:
                    item.rmdir()
                except OSError:
                    print(f"[WARN] Could not remove non-empty dir: {item}")
            else:
                shutil.move(str(item), str(target))

        else:
            if target.exists():
                print(f"[WARN] Skipping existing file: {target}")
            else:
                shutil.move(str(item), str(target))


def process_net_folder(net_path: Path):
    parent = net_path.parent

    print(f"[INFO] Processing: {net_path}")

    for item in net_path.iterdir():
        if item.is_dir():
            dest = parent / item.name

            if dest.exists():
                print(f"[INFO] Merging into: {dest}")
                move_contents(item, dest)
                try:
                    item.rmdir()
                except OSError:
                    print(f"[WARN] Still not empty after merge: {item}")
            else:
                shutil.move(str(item), str(dest))

    # Final cleanup attempt
    try:
        net_path.rmdir()
        print(f"[OK] Removed empty folder: {net_path}")
    except OSError:
        print(f"[WARN] NET not empty, leaving it: {net_path}")
        for r in net_path.rglob("*"):
            print(f"  Remaining: {r}")


def main():
    root = Path.cwd()

    for path in root.rglob("NET"):
        if path.is_dir() and path.parent.name == "MGS - Disc 2":
            process_net_folder(path)


if __name__ == "__main__":
    main()