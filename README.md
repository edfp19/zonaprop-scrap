# Zona Oportunidades

Proyecto educativo de ingeniería de datos para recolectar departamentos publicados en Zonaprop,
conservar su historia y detectar avisos cuyo precio pedido está por debajo del promedio de avisos
comparables en CABA y AMBA.

> **Checkpoint actual: Bronze + Silver + Gold.** Selenium conserva observaciones originales en
> JSONL; Spark produce Parquet histórico y calcula comparables. PostgreSQL sirve un snapshot Gold
> consultable desde la API y el dashboard. Una muestra pequeña puede no producir candidatos.

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
├── opportunities/    # Comparables Spark y publicación Gold transaccional
├── pipeline/         # Orquestación y estado de las ejecuciones
├── presentation/     # API y dashboard en español
└── shared/           # Configuración y logging transversal
infra/postgres/       # Esquemas operativos y Gold
tests/                # Pruebas unitarias por dominio
```

`listings/` contiene Silver; `opportunities/` contiene Gold. La API y el dashboard consultan
PostgreSQL y no necesitan Java ni Spark.

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

El dashboard sirve un mapa Leaflet desde Flask; cada servicio instala únicamente su grupo de
dependencias con uv. Solo
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

## Cloud deployment

The prepared website configuration uses Vercel for static files and FastAPI, and Neon for Gold.
See [the setup steps](infra/vercel/README.md). The initial Gold seed and Neon reader login are
verified; website deployment is still pending.
no cloud collection workflow or schedule is enabled. Local Compose remains the application bootstrap.

## Checkpoint 3 — Gold

Gold es un snapshot actual de avisos elegibles, incluidos los que todavía no tienen suficientes
comparables. Se reconstruye desde Silver después de cada recolección. No modifica Bronze ni Silver.

### Reglas de comparación

1. Elegir la última observación por `listing_id` y operación antes de filtrar calidad. Dentro de
   esa observación se prefiere el precio USD; si hay varios USD se elige la primera posición.
   Un precio nuevo inválido o una publicación nueva solo en ARS no revive un precio USD antiguo.
2. Retener observaciones de los últimos 35 días, sin fechas futuras. Exigir `PROPERTY`,
   `DEPARTAMENTO`, venta/alquiler, precio USD positivo, ambientes y superficie cubierta positivos,
   identificador/nombre de ubicación y título/URL disponibles. Excluir errores de esquema,
   linaje inconsistente, operación discordante y superficie cubierta mayor que la total.
3. Buscar otros avisos de la misma operación, tipo, `location_id` de fuente y cantidad de ambientes,
   con superficie cubierta entre ±15% de la del objetivo (configurable con
   `COMPARABLE_AREA_TOLERANCE`). Los límites son inclusivos y se comparan con decimales.
4. Excluir el propio aviso de sus comparables. Calcular cantidad, promedio y mediana exacta de
   **precios pedidos totales en USD**, no del precio por m². El precio por m² es informativo.
5. `discount_percent = (promedio - precio) / promedio * 100`. Un candidato exige al menos
   `COMPARABLE_MIN_COUNT` pares (10 por defecto) y precio estrictamente menor que el promedio.
   Igual al promedio no es candidato; una muestra insuficiente nunca produce candidato.

La falta de coordenadas o precisión no bloquea la comparación categórica por ubicación; esas
advertencias siguen visibles. La moneda USD viene del precio publicado, sin conversión ni exigencia
adicional de que la interfaz de búsqueda haya verificado USD. No hay ampliación automática de zona
ni relajación del mínimo cuando faltan pares. Repetir observaciones históricas no aumenta su peso.

### Publicación y consultas

`opportunities/transforms.py` selecciona y agrupa con Spark. `domain.py` contiene dataclasses y la
regla de descuento. `repository.py` reemplaza `gold.opportunities` en una única transacción:
si el cálculo/insert falla, los lectores conservan el snapshot anterior. Se usa `DELETE` transaccional,
no `TRUNCATE`. El resultado vacío válido elimina el snapshot anterior para no mantener avisos caducos.

La clave es `(listing_id, operation)`, para permitir venta y alquiler del mismo aviso. La publicación
migra automáticamente la clave anterior en volúmenes Compose existentes; no hay que borrar PostgreSQL.
Los valores monetarios de cálculo usan Decimal; el esquema de servicio existente almacena doubles,
suficientes para este proyecto de comparaciones aproximadas.

Para reconstruir Gold sin Selenium (requiere PostgreSQL disponible):

```bash
docker compose run --rm --no-deps pipeline python -m zona_oportunidades.opportunities
curl 'http://localhost:8000/oportunidades'
curl 'http://localhost:8000/oportunidades?only_opportunities=false&limit=10'
curl 'http://localhost:8000/oportunidades?operation=VENTA&area=Palermo&rooms=2&limit=10&offset=0'
```

`total` cuenta resultados con esos filtros; `evaluados` cuenta todo el snapshot Gold. La API devuelve
banderas como arrays y ordena por descuento descendente con desempate estable. El dashboard muestra
los avisos sobre un mapa con filtros y el total evaluado actual. La ejecución offline no cambia los contadores de la
última recolección en `/estado-pipeline`; el pipeline programado registra sus propias cantidades.

### Mapa de propiedades

El dashboard en `http://localhost:8501` usa el snapshot existente, sin iniciar Selenium ni el pipeline.
`GET /mapa` entrega todas las observaciones Gold con coordenadas, foto de portada y comparación;
el dashboard usa un proxy del mismo origen (`/api/mapa`) para consultar la API interna.

- Verde: precio por debajo del promedio de sus comparables.
- Rojo: precio por encima del promedio.
- Neutro: igual al promedio o sin referencia disponible.
- La tarjeta del marcador muestra foto, precio, promedio, diferencia y cantidad de comparables;
  la foto y el enlace abren el aviso en Zonaprop. Las muestras pequeñas siguen identificadas:
  estar debajo del promedio no equivale a cumplir el mínimo de una oportunidad formal.

La foto se extrae de `raw_json` retenido en Silver, sin modificar sus archivos ni recolectar otra vez.
No se inventan coordenadas para avisos que no las tienen. Los tiles y las imágenes requieren red;
la precisión de la ubicación se conserva desde la fuente. Leaflet y los tiles mantienen su atribución.
El dashboard Flask reemplaza Taipy y elimina sus dependencias y el override de Flask-SocketIO.

El join de comparables usa un rango por objetivo y puede crecer cuadráticamente dentro de una zona.
Es apropiado para la muestra educativa; medir antes de aumentar el volumen. Gold no es una tasación.

### Retención

La política acordada es una observación semanal por aviso, operación y moneda durante cinco semanas,
y borrar Bronze después de promoción verificada. **La limpieza física todavía no está implementada.**
El filtro de frescura Gold usa 35 días; no equivale a compactar o borrar el historial Silver.

## Desarrollo local con uv

El proyecto usa **uv** y un lockfile reproducible:

```bash
uv sync --dev --all-extras
uv run pytest
uv run ruff check .
uv run mypy
uv run mutmut run
uv run mutmut results
```

Para desarrollar solo Silver: `uv sync --extra collection --extra silver`. PySpark requiere Java 17;
el contenedor `pipeline` ya lo incluye.

Mypy comprueba de forma estricta el dominio Gold, la API, el servidor del dashboard y la orquestación; no implica que todo el
código existente ya tenga comprobación estática. Mutmut está limitado a `opportunities/domain.py`
y sus pruebas de reglas de precio. Tras cambiar pruebas, se puede forzar una nueva evaluación con
`uv run mutmut run 'zona_oportunidades.opportunities.domain.*'`; no aplicar mutantes al código fuente.
La configuración de mutmut sigue su [documentación oficial](https://mutmut.readthedocs.io/en/latest/).

Para ejecutar toda la suite, incluidas Spark, API y publicación PostgreSQL, en bases temporales aisladas:

```bash
docker compose run --rm --no-deps -e RUN_POSTGRES_TESTS=1 \
  -v "$PWD/tests:/app/tests:ro" -v "$PWD/infra:/app/infra:ro" pipeline \
  uv run --no-sync --with pytest --with fastapi --with httpx --with flask --with requests \
  python -m pytest tests -q
```

Este comando necesita PostgreSQL accesible y permiso para crear bases de pruebas. Sin
`RUN_POSTGRES_TESTS=1`, las pruebas de base se omiten explícitamente. Ninguna prueba recolecta del
sitio real. Las funciones de ventana/agregación usadas están descritas en la
[referencia de Spark 3.5.6](https://spark.apache.org/docs/3.5.6/sql-ref-functions-builtin.html).

## Uso responsable

Este proyecto es educativo. Antes de aumentar la frecuencia o el volumen, revisá los términos del
sitio y las normas aplicables. Usá límites bajos, pausas y recolecciones incrementales. Los resultados
futuros serán candidatos para investigar, no tasaciones ni asesoramiento financiero.
