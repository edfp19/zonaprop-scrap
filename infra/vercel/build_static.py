"""Copy the shared dashboard assets into Vercel's public directory at build time."""

from pathlib import Path
from shutil import copy2


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    source = root / "src/zona_oportunidades/presentation/dashboard/static"
    public = root / "public"
    assets = public / "static"
    assets.mkdir(parents=True, exist_ok=True)
    copy2(source / "index.html", public / "index.html")
    for name in ("dashboard.css", "dashboard.js"):
        copy2(source / name, assets / name)


if __name__ == "__main__":
    main()
