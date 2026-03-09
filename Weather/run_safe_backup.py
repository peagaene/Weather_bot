from __future__ import annotations

import argparse
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parent

EXCLUDED_NAMES = {
    ".env",
    "export",
    "__pycache__",
    ".pytest_cache",
    ".git",
}

EXCLUDED_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".pyd",
    ".db",
}


def should_include(path: Path) -> bool:
    rel_parts = path.relative_to(ROOT).parts
    if any(part in EXCLUDED_NAMES for part in rel_parts):
        return False
    if path.suffix.lower() in EXCLUDED_SUFFIXES:
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a safe project backup without secrets or local state.")
    parser.add_argument(
        "--output",
        default=str(ROOT / "backup_safe.zip"),
        help="Output zip path.",
    )
    args = parser.parse_args()

    output = Path(args.output)
    if not output.is_absolute():
        output = ROOT / output
    output.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        for path in ROOT.rglob("*"):
            if not path.is_file():
                continue
            if output.resolve() == path.resolve():
                continue
            if not should_include(path):
                continue
            archive.write(path, arcname=str(path.relative_to(ROOT)))

    print(f"Backup seguro criado em: {output}")


if __name__ == "__main__":
    main()
