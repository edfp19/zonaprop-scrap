# Zona Oportunidades

Proyecto de ingeniería de datos que recolecta departamentos de Zonaprop y compara sus precios
pedidos en CABA y GBA Norte/Sur/Oeste. Incluye venta y alquiler permanente, historial de
observaciones, una API y un mapa de propiedades.

## Arquitectura

```text
Zonaprop → Selenium → Bronze/JSONL → Spark → Silver/Parquet
                                                ↓
                                       Gold/PostgreSQL
                                                ↓
                                         API + dashboard
```

Bronze guarda la respuesta original y el contexto de recolección. Silver normaliza los avisos
con un esquema explícito y conserva observaciones históricas. Gold reúne los avisos actuales
elegibles y sus comparables en un snapshot consultable.

El código está organizado en `src/zona_oportunidades/`:

- `collection/`: Selenium y escritura de Bronze.
- `listings/`: transformación Silver.
- `opportunities/`: selección de comparables y publicación Gold.
- `pipeline/`: ejecución de etapas y estado operativo.
- `presentation/`: API FastAPI y dashboard Flask con Leaflet.
- `shared/`: configuración y logging.

Los esquemas están en `infra/postgres/` y las pruebas en `tests/`. Las lecciones de Bronze,
Silver, Gold e infraestructura se conservan localmente en `lessons/`, fuera del repositorio.

## Ejecución local

Docker con Compose incluye las dependencias necesarias. La configuración mediante `.env` es
opcional; el arranque completo usa un solo comando:

```bash
docker compose up --build
```

La plantilla de configuración está en `.env.example`:

```bash
cp .env.example .env
```

- Dashboard: <http://localhost:8501>
- Documentación de la API: <http://localhost:8000/docs>
- Selenium/noVNC: <http://localhost:7900>, contraseña `secret`.
- PostgreSQL: `localhost:5432`.

Solo la imagen `pipeline` incluye Java y Spark. API y dashboard consultan PostgreSQL sin
esas dependencias. Los archivos persisten en `./data` y la base en el volumen `postgres_data`
entre reinicios ordinarios.

`COLLECTION_MAX_PAGES` limita las páginas por operación y región; el valor inicial es uno.
`COLLECTION_DELAY_SECONDS` establece la pausa entre páginas. El primer objetivo rota diariamente.
El estado y los logs están disponibles con:

```bash
docker compose logs -f pipeline
curl http://localhost:8000/estado-pipeline
```

## Datos y publicación

Bronze guarda JSONL por fecha y ejecución, acompañado por un manifiesto con resultados y errores.
Selenium espera los datos de la página en `window.__PRELOADED_STATE__`. Una verificación del
portal detiene la sesión y deja visible el fallo, incluso si hubo páginas válidas anteriores.

Silver produce Parquet tipado por ejecución. Solo elimina líneas originales exactamente duplicadas;
observaciones de fechas distintas forman parte del historial. Las monedas se conservan sin
conversión y las coordenadas ausentes no se inventan. Los avisos incompletos quedan identificados
con banderas de calidad.

La publicación Silver es atómica e idempotente: el manifiesto registra el digest de entrada y
la versión de transformación. Repetir una entrada comprometida la omite; cambiar su contenido o
versión genera un conflicto sin sobrescribirla. Los archivos archivados quedan fuera del dataset
activo.

El reprocesamiento de Bronze existente no requiere Selenium ni PostgreSQL:

```bash
docker compose run --rm --no-deps pipeline python -m zona_oportunidades.listings --all
```

Gold reemplaza `gold.opportunities` en una transacción. Si falla la publicación, el snapshot
anterior permanece disponible; un resultado vacío válido lo vacía para retirar avisos caducos.
La clave `(listing_id, operation)` admite venta y alquiler del mismo aviso.

Las migraciones son explícitas: [migrate_gold.sql](infra/postgres/migrate_gold.sql) actualiza
columnas y la clave de volúmenes anteriores, ejecutado por el propietario de la base.
La publicación no migra el esquema automáticamente. El esquema inicial está en
[init.sql](infra/postgres/init.sql).

La reconstrucción de Gold desde Silver requiere PostgreSQL disponible:

```bash
docker compose run --rm --no-deps pipeline python -m zona_oportunidades.opportunities
curl 'http://localhost:8000/oportunidades?only_opportunities=false&limit=10'
```

## Reglas de comparación

1. Se elige la última observación por aviso y operación antes de filtrar calidad. Dentro de
   ella se prioriza USD. Un precio nuevo inválido o solo en ARS no recupera un USD antiguo.
2. Son elegibles los departamentos de venta o alquiler con hasta 35 días de antigüedad, sin
   fechas futuras, precio USD positivo, ambientes y superficie cubierta positivos, ubicación,
   título y URL. Se excluyen errores de esquema o linaje y superficies inconsistentes.
3. Los comparables son otros avisos de igual operación, tipo, ubicación de fuente y ambientes,
   con superficie cubierta dentro de ±15%, incluidos los límites. La tolerancia se configura
   con `COMPARABLE_AREA_TOLERANCE`.
4. Promedio y mediana usan precios pedidos totales en USD. El propio aviso queda excluido;
   repetir observaciones históricas no aumenta su peso. El precio por m² es informativo.
5. El descuento es `(promedio - precio) / promedio * 100`. Un candidato necesita al menos
   diez comparables (`COMPARABLE_MIN_COUNT`) y precio estrictamente menor que el promedio.
   No se amplía la zona ni se reduce el mínimo cuando faltan pares.

Las coordenadas ausentes no impiden la comparación por ubicación, pero excluyen el aviso del mapa.
Los cálculos monetarios usan Decimal; PostgreSQL almacena doubles para estas comparaciones aproximadas.
El cruce de comparables puede crecer cuadráticamente dentro de una zona.

## Mapa y consultas

`GET /mapa` entrega los avisos Gold con coordenadas, foto y comparación. El dashboard consulta
mediante `/api/mapa`, sin iniciar recolecciones. Verde indica precio inferior al promedio; rojo,
superior; neutro, igualdad o falta de referencia. Una muestra pequeña puede aparecer verde sin
alcanzar el mínimo de una oportunidad.

Las tarjetas incluyen precio, promedio, diferencia, cantidad de comparables y enlace al aviso.
Fotos y mapas requieren red. `/oportunidades` permite filtrar por operación, zona y ambientes;
`total` cuenta resultados filtrados y `evaluados`, todo Gold.

## Despliegue y trabajo pendiente

La web está desplegada en <https://zonaprop-scrap.vercel.app/> con Vercel y Gold en Neon.
La verificación inicial registró 96 avisos y 95 ubicaciones. La configuración y los permisos de
lectura están documentados en [infra/vercel/README.md](infra/vercel/README.md).

El [pipeline manual y la limpieza](infra/pipeline/README.md) incluyen límites de capacidad,
cuarentena privada y publicación coordinada. No están habilitados ni verificados en el proveedor.
El [roadmap](infra/pipeline-roadmap.md) registra las verificaciones pendientes.

La retención acordada conserva la última observación semanal por aviso, operación y moneda durante
cinco semanas. El mantenimiento cloud la aplica mediante generaciones inmutables; Bronze solo
se retira tras una promoción verificada. El filtro Gold de 35 días, por sí solo, no limpia archivos.

## Verificación

El desarrollo local usa uv y el lockfile del repositorio:

```bash
uv sync --dev --all-extras
uv run ruff check .
uv run pytest -q
uv run mypy
uv run mutmut run
```

Ruff comprueba estilo; pytest, comportamiento; mypy, tipos en Gold, API, dashboard y orquestación.
Mutmut evalúa las pruebas de reglas de precio. Ninguno sustituye a los demás.

Pytest local puede omitir Silver sin PySpark. La imagen del pipeline permite comprobarlo con Spark:

```bash
docker compose run --rm --no-deps -v "$PWD/tests:/app/tests:ro" pipeline \
  uv run --no-sync --with pytest python -m pytest tests/listings -q
```

Las pruebas PostgreSQL necesitan una base accesible y `RUN_POSTGRES_TESTS=1`; de lo contrario se
omiten. Las pruebas habituales no recolectan del sitio real.

## Uso responsable

La intención es puramente educativa.
