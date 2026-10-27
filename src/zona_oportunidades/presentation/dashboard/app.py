import os
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, send_from_directory

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
STATIC_DIR = Path(__file__).with_name("static")

# Leaflet comes from unpkg and tiles from OpenStreetMap; listing photos come from Zonaprop's CDN.
CONTENT_SECURITY_POLICY = (
    "default-src 'self'; script-src 'self' https://unpkg.com; "
    "style-src 'self' https://unpkg.com; img-src 'self' data: http: https:; "
    "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="/static")


@app.after_request
def add_security_headers(response: Response) -> Response:
    response.headers["Content-Security-Policy"] = CONTENT_SECURITY_POLICY
    response.headers["X-Content-Type-Options"] = "nosniff"
    # OSM's tile policy asks for a Referer; listing photos opt out per <img>.
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response


@app.get("/")
def index() -> Response:
    return send_from_directory(STATIC_DIR, "index.html", max_age=0)


@app.get("/favicon.ico")
def favicon() -> tuple[str, int]:
    return "", 204


@app.get("/api/mapa")
def map_data() -> tuple[Response, int] | Response:
    """Same-origin proxy: the browser cannot resolve the internal `api` hostname."""
    try:
        upstream = requests.get(f"{API_BASE_URL}/mapa", timeout=(3, 20))
    except requests.Timeout:
        return jsonify(error="La API tardó demasiado en responder."), 504
    except requests.RequestException:
        return jsonify(error="No fue posible conectar con la API."), 502
    if not upstream.ok:
        return jsonify(error=f"La API respondió con el estado {upstream.status_code}."), 502
    try:
        payload = upstream.json()
    except ValueError:
        return jsonify(error="La API devolvió una respuesta que no es JSON."), 502
    response = jsonify(payload)
    response.headers["Cache-Control"] = "no-store"
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8501)
