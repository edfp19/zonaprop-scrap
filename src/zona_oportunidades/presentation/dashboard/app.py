import os
from pathlib import Path

import requests
from taipy.gui import Gui

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

estado = "Consultando…"
bronze = 0
silver = 0
gold = 0
mensaje = ""


def on_init(state) -> None:
    try:
        response = requests.get(f"{API_BASE_URL}/estado-pipeline", timeout=5)
        response.raise_for_status()
        data = response.json()
        state.estado = data.get("status") or data.get("estado", "—")
        state.bronze = data.get("bronze_records", 0)
        state.silver = data.get("silver_records", 0)
        state.gold = data.get("gold_records", 0)
        state.mensaje = data.get("mensaje") or data.get("error_message") or ""
    except requests.RequestException as exc:
        state.estado = "ERROR"
        state.mensaje = f"No fue posible consultar el pipeline: {exc}"


PAGE = """
# Oportunidades inmobiliarias

Departamentos en CABA y AMBA · Proyecto educativo de ingeniería de datos

<|part|class_name=checkpoint|
**Checkpoint 2: Bronze + Silver.** Preservamos observaciones originales y las transformamos en
Parquet histórico. Todavía no calculamos oportunidades ni puntajes Gold.
|>

## Última ejecución

<|{mensaje}|text|>

<|layout|columns=1 1 1 1|
<|part|class_name=metric|
#### Estado
<|{estado}|text|>
|>
<|part|class_name=metric|
#### Registros Bronze
<|{bronze}|text|>
|>
<|part|class_name=metric|
#### Registros Silver
<|{silver}|text|>
|>
<|part|class_name=metric|
#### Registros Gold
<|{gold}|text|>
|>
|>

## Capas del proyecto

1. **Bronze — actual:** JSONL append-only con el contenido observado y su contexto.
2. **Silver — actual:** Parquet tipado, normalizado y deduplicado mediante Spark.
3. **Gold:** comparables y candidatos por debajo del promedio en PostgreSQL.
"""


def main() -> None:
    css_file = Path(__file__).with_name("dashboard.css")
    Gui(page=PAGE, css_file=str(css_file)).run(
        host="0.0.0.0",
        port=8501,
        title="Oportunidades inmobiliarias",
        use_reloader=False,
    )


if __name__ == "__main__":
    main()
