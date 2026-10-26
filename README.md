# Zona Oportunidades

Proyecto educativo de ingeniería de datos para recolectar departamentos publicados en Zonaprop,
conservar su historia y detectar avisos cuyo precio pedido está por debajo del promedio de avisos
comparables en CABA y AMBA.

> **Checkpoint actual: Bronze + Silver.** El proyecto recolecta observaciones mediante Selenium,
> las conserva como JSONL append-only y puede transformarlas con Spark en Parquet histórico. Gold
> todavía no se implementa.

## Arquitectura objetivo

```text
Zonaprop ── Selenium ──> Bronze/JSONL ── Spark ──> Silver/Parquet
                                                    │
                                                    └── Spark ──> Gold/PostgreSQL
                                                                       │
                                                               API + dashboard
```

- **Bronze:** observaciones originales, contexto de búsqueda y metadatos de ejecución.
- **Silver:** observaciones de avisos tipadas, normalizadas, deduplicadas e históricas en Parquet.
- **Gold:** comparables, promedios y candidatos listos para consulta en PostgreSQL.
- **Alcance inicial:** departamentos en venta y alquiler permanente, CABA y GBA Norte/Sur/Oeste.
- **Moneda:** el recolector intenta fijar USD y registra si pudo verificar el contexto. Nunca inventa
  una conversión; cada aviso conserva el precio y moneda informados por la fuente.

## Organización por dominio

```text
src/zona_oportunidades/
├── collection/       # Selenium, caso de uso de recolección y repositorio Bronze
├── listings/         # Transformación Silver de observaciones inmobiliarias
├── pipeline/         # Orquestación y estado de las ejecuciones
├── presentation/     # API y dashboard en español
└── shared/           # Configuración y logging transversal
infra/postgres/       # Esquemas operativos y Gold
tests/                # Pruebas unitarias por dominio
```

`listings/` es el dominio Silver. `opportunities/` se incorporará al implementar Gold; no se crean
módulos vacíos por anticipado.

## Puesta en marcha

Requisitos: Docker con Compose. No hace falta instalar Python, Chrome, Java ni PostgreSQL en el host.

```bash
cp .env.example .env
docker compose up --build
```

Servicios:

- Dashboard: <http://localhost:8501>
- Documentación de la API: <http://localhost:8000/docs>
- Selenium/noVNC para depuración: <http://localhost:7900> (contraseña `secret`)
- PostgreSQL: `localhost:5432`

El dashboard usa `taipy-gui`; cada servicio instala únicamente su grupo de dependencias con uv. Solo
la imagen `pipeline` instala Java 17 y PySpark 3.5.6; API y dashboard no incorporan esas dependencias.

Los datos del lakehouse sobreviven en el bind mount `./data`; PostgreSQL usa el volumen nombrado
`postgres_data`.

### Observar la primera ejecución

```bash
docker compose logs -f pipeline
docker compose exec pipeline find /data/bronze -maxdepth 5 -type f
docker compose exec pipeline python -c "import glob; print(open(glob.glob('/data/bronze/listings/**/*.jsonl', recursive=True)[-1]).readline())"
curl http://localhost:8000/estado-pipeline
```

El tamaño inicial se controla con `COLLECTION_MAX_PAGES`. El valor predeterminado es una página por
cada combinación de operación y región. `COLLECTION_DELAY_SECONDS` impone una pausa entre páginas.
El primer objetivo rota cada día para no favorecer siempre la misma región si el sitio limita una
sesión a mitad del recorrido.

## Checkpoint 1 — Bronze

### Por qué Bronze es JSONL

Bronze debe permitir reproducir transformaciones futuras. Cada línea contiene un aviso tal como fue
observado, más `run_id`, fecha, URL, operación, región, moneda solicitada y versión del extractor.
Una ejecución nueva agrega archivos; no modifica observaciones anteriores.

La estructura de particiones es:

```text
/data/bronze/listings/ingestion_date=AAAA-MM-DD/run_id=<uuid>/listings.jsonl
/data/bronze/manifests/<uuid>.json
```

El manifiesto permite distinguir una ejecución completa de una ejecución con errores parciales. La
escritura del JSONL usa un archivo temporal y un reemplazo atómico para no exponer archivos a medio
escribir.

### Por qué Selenium

El adaptador carga la página real y espera `window.__PRELOADED_STATE__`. Esa estructura contiene los
avisos renderizados y evita depender exclusivamente de nombres de clase generados. Los selectores de
interfaz se usan únicamente para intentar fijar USD. Si el portal cambia o presenta una verificación,
la ejecución falla de forma visible; no se intenta evadirla.

Una verificación puede aparecer después de páginas válidas. En ese caso se detiene la sesión, se
conservan las observaciones obtenidas y el manifiesto y el estado operativo muestran el error parcial.

## Checkpoint 2 — Silver

Silver lee Bronze sin modificarlo con un esquema explícito, explota las operaciones y precios en filas
tipadas y aplica banderas de calidad antes de deduplicar y publicar Parquet. Cada fila representa una
observación de fuente, operación y precio: `source_observation_id` es el hash del contenido de la línea
raw y `observation_id` además incorpora el `run_id` confiable de la partición y las posiciones de
operación y precio.

El dataset histórico es plano y único:

```text
/data/silver/listing_observations/ingestion_date=AAAA-MM-DD/run_id=<uuid>/posting_type=<tipo>/
```

Solo se eliminan líneas raw exactamente duplicadas. Un mismo aviso observado en timestamps distintos se
conserva como historial. La moneda informada por la fuente se retiene sin conversión; las filas que no
son USD no son elegibles para comparables en USD. Las coordenadas ausentes no se inventan.

La primera señal geográfica es una caja amplia, no un polígono administrativo exacto: latitud
`[-35.3, -34.2]` y longitud `[-59.3, -57.3]`. Los avisos incompletos o de desarrollo se conservan con
banderas y no forman parte de los comparables iniciales.

La publicación de cada ejecución es atómica. El manifiesto registra el digest de la fuente y la versión
de transformación: una entrada ya comprometida se omite y un cambio de entrada o versión falla sin
sobrescribir el resultado previo.

Las versiones de transformación no sobrescriben resultados existentes. La verificación inicial de
Checkpoint 2 se archivó deliberadamente antes de su replay final; futuras actualizaciones requieren una
estrategia explícita de archivo o versionado. Los archivos archivados quedan fuera del dataset activo y
no se leen automáticamente.

### Ejecutar o repetir Silver

El ciclo programado transforma a Silver inmediatamente después de Bronze. También se puede reprocesar
Bronze histórico sin red ni PostgreSQL usando el `.env` de Compose:

```bash
docker compose run --rm --no-deps pipeline python -m zona_oportunidades.listings --all
docker compose run --rm --no-deps pipeline python -m zona_oportunidades.listings --bronze-path /data/bronze/listings/ingestion_date=AAAA-MM-DD/run_id=<uuid>/listings.jsonl
```

Para inspeccionar el Parquet desde el contenedor (sin Java en el host):

```bash
docker compose run --rm --no-deps -T pipeline python - <<'PY'
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[2]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
df = spark.read.parquet("/data/silver/listing_observations")
df.createOrReplaceTempView("listing_observations")
print("rows:", spark.sql("SELECT count(*) AS count FROM listing_observations").first()["count"])
df.groupBy("posting_type", "operation", "currency").count().orderBy(
    "posting_type", "operation", "currency"
).show()
for field in df.schema.fields:
    if "map" in field.name.lower() and field.dataType.simpleString() == "boolean":
        df.groupBy(field.name).count().orderBy(field.name).show()
df.select(
    "listing_id", "title", "listing_url", "operation", "currency", "price", "rooms",
    "total_area_m2", "covered_area_m2", "location_name", "parent_area_name",
    "has_valid_coordinates", "map_eligible", "quality_flags"
).show(10, truncate=False)
spark.stop()
PY
```

El master predeterminado es `local[2]`, la zona horaria es UTC y la interfaz web de Spark está
deshabilitada. Para desarrollo local se pueden ejecutar las pruebas de Spark con Java 17 instalado y
`uv run --extra silver pytest tests/listings`, o con Compose.

### Ejercicios después de Silver

1. Encontrar un mismo `postingId` en dos ejecuciones distintas.
2. Inspeccionar `modified_date` desde `raw_json` con `get_json_object` sin alterar Bronze.
3. Revisar cuántos avisos tienen `currency_context_verified=false`.
4. Identificar en `payload.mainFeatures` los códigos de ambientes y superficies.
5. Revisar cuántas filas quedaron fuera de la caja geográfica amplia y cuántas no son elegibles para
   comparables USD.
6. Repetir un input ya comprometido y comprobar que se omite; para demostrar un conflicto, usar una
   copia temporal del fixture, nunca editar Bronze comprometido.

## Desarrollo local con uv

El proyecto usa **uv** y un lockfile reproducible:

```bash
uv sync --dev --all-extras
uv run pytest
uv run ruff check .
```

Para desarrollar solo Silver: `uv sync --extra collection --extra silver`. PySpark requiere Java 17;
el contenedor `pipeline` ya lo incluye.

## Uso responsable

Este proyecto es educativo. Antes de aumentar la frecuencia o el volumen, revisá los términos del
sitio y las normas aplicables. Usá límites bajos, pausas y recolecciones incrementales. Los resultados
futuros serán candidatos para investigar, no tasaciones ni asesoramiento financiero.
