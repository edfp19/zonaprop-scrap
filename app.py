"""Vercel entrypoint, sharing the local dashboard's HTML, CSS and JavaScript."""

from pathlib import Path

from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from zona_oportunidades.presentation import dashboard
from zona_oportunidades.presentation.api.main import app as app

STATIC_DIR = Path(dashboard.__file__).with_name("static")


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
