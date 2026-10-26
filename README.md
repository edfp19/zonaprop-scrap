# Zona Oportunidades

Proyecto educativo de ingeniería de datos para recolectar departamentos publicados en Zonaprop,
conservar su historia y detectar avisos cuyo precio pedido está por debajo del promedio de avisos
comparables en CABA y AMBA.

> **Checkpoint actual: Bronze.** El proyecto recolecta observaciones mediante Selenium y las guarda
> como JSONL append-only. Spark se incorporará en el checkpoint Silver, cuando exista información
> real que transformar.

## Arquitectura objetivo

```text
Zonaprop ── Selenium ──> Bronze/JSONL ── Spark ──> Silver/Parquet
                                                    │
                                                    └── Spark ──> Gold/PostgreSQL
                                                                       │
                                                               API + dashboard
```

- **Bronze:** observaciones originales, contexto de búsqueda y metadatos de ejecución.
- **Silver:** avisos tipados, normalizados, deduplicados e históricos en Parquet.
- **Gold:** comparables, promedios y candidatos listos para consulta en PostgreSQL.
- **Alcance inicial:** departamentos en venta y alquiler permanente, CABA y GBA Norte/Sur/Oeste.
- **Moneda:** el recolector intenta fijar USD y registra si pudo verificar el contexto. Nunca inventa
  una conversión; cada aviso conserva el precio y moneda informados por la fuente.

## Organización por dominio

```text
src/zona_oportunidades/
├── collection/       # Selenium, caso de uso de recolección y repositorio Bronze
├── pipeline/         # Orquestación y estado de las ejecuciones
├── presentation/     # API y dashboard en español
└── shared/           # Configuración y logging transversal
infra/postgres/       # Esquemas operativos y Gold
tests/                # Pruebas unitarias por dominio
```

Las futuras carpetas `listings/` y `opportunities/` se agregarán al implementar Silver y Gold. No se
crean módulos vacíos por anticipado.

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

El dashboard usa `taipy-gui`; cada servicio instala únicamente su grupo de dependencias con uv.

Los datos sobreviven a la recreación de contenedores en los volúmenes `lakehouse_data` y
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

## Checkpoint 1 — qué aprender

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

### Ejercicios antes de Silver

1. Encontrar un mismo `postingId` en dos ejecuciones distintas.
2. Comparar su precio y `modified_date` sin alterar Bronze.
3. Revisar cuántos avisos tienen `currency_context_verified=false`.
4. Identificar en `payload.mainFeatures` los códigos de ambientes y superficies.

Cuando estos datos resulten claros, el checkpoint Silver introducirá Java/PySpark, esquema explícito,
reglas de calidad, deduplicación e historial en Parquet.

## Desarrollo local con uv

El proyecto usa **uv** y un lockfile reproducible:

```bash
uv sync --dev --all-extras
uv run pytest
uv run ruff check .
```

Las dependencias de Spark y Java se incorporarán recién en el checkpoint Silver para mantener este
checkpoint mínimo.

## Uso responsable

Este proyecto es educativo. Antes de aumentar la frecuencia o el volumen, revisá los términos del
sitio y las normas aplicables. Usá límites bajos, pausas y recolecciones incrementales. Los resultados
futuros serán candidatos para investigar, no tasaciones ni asesoramiento financiero.
