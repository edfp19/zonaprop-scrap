# Pipeline y mantenimiento

Implementación para ejecución manual. No hay un cron activo ni se habilitan escrituras con
las plantillas: el informe local no sustituye las métricas de Neon. El objetivo es mantener un
catálogo acotado y cinco semanas de observaciones, con margen para reescribirlas sin agotar espacio.

## Componentes

- `operations/capacity.py`: presupuestos, estimación conservadora y admisión por pico previsto.
- `operations/report.py`: medidas locales y puertas cloud pendientes, sin datos de credenciales.
- `listings/retention.py`: última observación por aviso, operación, moneda y semana UTC.
- `opportunities/catalogue.py`: renovación de claves existentes y admisión independiente del precio.
- `operations/storage.py`: inventarios/checksums, publicación inmutable y registro de generaciones.
- `operations/cloud.py`: seed, mantenimiento, GC y coordinación con Gold en una transacción.
- `operations/ingestion.py` y `quarantine.py`: colección acotada y recuperación privada de Bronze.
- `operations/budget.py`: coste reservado por trabajo y rechazo de una captura de uso ya consumida.
- `.github/workflows/pipeline.yml`: disparo manual; los cambios necesitan habilitación explícita.

La versión inicial reescribe una generación completa de cinco semanas, limitada a 25 MB. El coste
previsto incluye esa copia temporal y la anterior. Si no entra, la publicación se detiene; no se
acorta la historia. La fragmentación por semanas queda para cuando ese límite resulte insuficiente.

## Preparación de Neon

Las migraciones se aplican como propietario, en `neondb`, antes de usar el escritor:

1. `infra/postgres/init.sql`
2. `infra/postgres/migrate_gold.sql`
3. `infra/postgres/pipeline_writer.sql`
4. `infra/postgres/pipeline_storage.sql`
5. `infra/postgres/bronze_quarantine.sql`
6. `infra/postgres/pipeline_budget.sql`

La contraseña de `pipeline_writer` se establece de forma privada mediante `ALTER ROLE ... PASSWORD`.
Las operaciones de coordinación necesitan su conexión **directa**, con TLS. El sitio conserva su
conexión pooled de `web_reader`. El bootstrap local aplica esquemas/migraciones mediante el servicio
efímero `migrate`; las aplicaciones no necesitan ejecutar DDL para publicar.

El bucket de Silver/Bronze es privado. Las credenciales de almacenamiento se limitan al alcance
necesario de lectura/escritura. Un ensayo pequeño debe verificar subida, lectura, borrado y la
reducción real de uso en Neon. `DeleteObject` puede ser un borrado lógico y sus reglas lifecycle no
se ejecutan: solicitar un borrado no acredita bytes recuperados.

## Capacidad antes de publicar

```bash
docker compose run --rm --no-deps -T pipeline \
  python -m zona_oportunidades.operations capacity-report --data-root /data
```

El JSON indica `local_baseline_only`; nunca habilita cloud por sí solo. El informe inicial se guarda
localmente en `data/capacity-baseline.json`. Las plantillas `policy.example.json` y `usage.example.json`
describen el contrato, no un dimensionamiento aprobado. El target ilustrativo de 1.000 claves debe
sustituirse después del piloto de cobertura. Cero en uso significa dato de ejemplo, no medición;
`verified: false` y la fecha antigua impiden aplicar esa plantilla.

El detalle de consumo v2 está documentado para planes de pago. La disponibilidad de métricas
suficientes en el proyecto Free debe verificarse. Mientras tanto, cada ejecución aplicada exige
una captura manual reciente y verificada del proyecto: almacenamiento total, cuotas y consumo
mensual. No se transforma un cero de un campo API deprecado en una medición válida.

Al 60% se informa una advertencia para priorizar mantenimiento. Por encima del 80% de pico previsto se rechazan nuevas escrituras.
También se reservan presupuesto mensual de cómputo/transferencia, espacio del runner y dos extracciones
fallidas. Cada captura se consume una sola vez; los costes reservados no se reembolsan automáticamente
tras un fallo. Bajar una fila de una tabla no devuelve CU-horas ni egress ya consumidos.

## Ensayo local y seed

Las credenciales se guardan en `.env.pipeline`, ignorado por Git:

```text
DATABASE_URL=CONEXION_DIRECTA_DE_PIPELINE_WRITER_CON_TLS
AWS_ACCESS_KEY_ID=CLAVE_DE_STORAGE
AWS_SECRET_ACCESS_KEY=SECRETO_DE_STORAGE
```

La política y la captura real se colocan en `data/cloud/policy.json` y `data/cloud/usage.json`.
El primer seed usa los Silver ya existentes; no vuelve a recolectar:

```bash
docker compose run --rm --no-deps --env-from-file .env.pipeline pipeline \
  python -m zona_oportunidades.operations.cloud seed \
  --policy /data/cloud/policy.json --usage /data/cloud/usage.json \
  --project-id PROYECTO_VERIFICADO --reclamation-verified \
  --silver-root /data/silver/listing_observations --data-root /data \
  --bucket BUCKET_PRIVADO --endpoint-url https://ENDPOINT_DE_STORAGE --region REGION
```

Sin `--apply` solo se evalúan las puertas de capacidad; no se inspecciona ni cambia el bucket.
`--reclamation-verified` acredita un ensayo hecho, no lo sustituye. Una vez verificadas las puertas,
`--apply` realiza la operación. Una captura nueva es necesaria para cada operación aplicada.

## Actions

Variables del repositorio: `NEON_PROJECT_ID`, `S3_REGION`, `CLOUD_POLICY_JSON` y, tras los ensayos,
`OBJECT_RECLAMATION_VERIFIED=true`, `PIPELINE_APPLY_ENABLED=true`.

Secrets: `PIPELINE_DATABASE_URL`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_ENDPOINT_URL`,
`S3_BUCKET`. Los campos de workflow no reciben contraseñas. El modo `capacity-report` funciona sin
estos secretos y, en un runner nuevo, mide un directorio vacío, no el lakehouse local.

Los modos `ingest`, `maintain` y `gc` arrancan en simulación y reciben la captura en `usage_snapshot`.
El workflow no publica Bronze/Silver como artifacts. Recopilación y preparación pueden solaparse con
mantenimiento; un advisory lock de PostgreSQL protege lecturas de generaciones, cambios de puntero y
borrado. Si está ocupado, el trabajo se detiene para reintento manual en lugar de competir.

## Recuperación y límites

Un fallo de fuente, un límite o una colección parcial no reemplaza Gold. Bronze se conserva en
cuarentena privada; solo la promoción verificada habilita su borrado. Dos reservas fallidas llenan
la capacidad y detienen nueva colección para investigación. No hay caducidad automática del Bronze
no promovido. Una caída total de almacenamiento o un hard kill del runner puede impedir persistir
los últimos bytes: no existe una garantía de durabilidad absoluta en ese caso.

GC retira inventarios conocidos y obsoletos tras al menos 24 horas, sin borrar generaciones activas.
También reintenta el borrado de Bronze ya promovido y retira metadatos resueltos tras 90 días;
conserva reservas fallidas, generaciones activas y comprobantes pendientes.
GC puede ejecutarse con almacenamiento o cuarentena llenos porque no crea otro dataset; sigue exigiendo métricas verificadas y presupuesto de cómputo/transferencia. Las métricas posteriores deben confirmar la recuperación de cuota. La caché pública del mapa admite
hasta una hora de demora y reduce lecturas a Neon; no garantiza disponibilidad ilimitada gratuita.
El cron semanal se añade solo después de un ensayo real y de resolver la observación de cuotas Free.

El piloto local del 27 de octubre obtuvo 30 registros en el primer objetivo y detectó un bloqueo
de acceso en el segundo: 302.707 bytes de Bronze en 54,45 segundos. Se detuvo sin ejecutar Spark
ni reemplazar Gold. Es una medición parcial, insuficiente para aprobar costes de un trabajo completo.

Referencias: [consumo Neon](https://neon.com/docs/guides/consumption-metrics),
[S3 Neon](https://neon.com/docs/storage/s3-compatibility),
[roadmap](../pipeline-roadmap.md).

## Verificación reproducible

```bash
uv run ruff check .
uv run mypy
docker compose run --rm --no-deps -T -e RUN_POSTGRES_TESTS=1 \
  -v "$PWD/src:/app/src:ro" -v "$PWD/tests:/app/tests:ro" \
  -v "$PWD/infra:/app/infra:ro" -v "$PWD/app.py:/app/app.py:ro" pipeline \
  uv run --no-sync --with pytest --with fastapi --with httpx --with flask --with requests \
  python -m pytest tests -q
uv run mutmut run 'zona_oportunidades.operations.capacity.x_assess_admission__mutmut_*' --max-children 2
```

Las pruebas de PostgreSQL crean y eliminan bases temporales en el servidor local de Compose.
No usan Neon ni la web real. La mutación está acotada a admisión de capacidad; no equivale a
haber ejecutado todas las mutaciones de todos los módulos.

Resultado local del 27 de octubre: suite completa con Spark/PostgreSQL, 254 aprobadas y una
omitida por falta de `QUARANTINE_TEST_DSN`. Esa integración se ejecutó después en una base
temporal y pasó; también pasó el ciclo completo seed → compactación → publicación. Ruff,
mypy (25 archivos), Compose, sintaxis JavaScript y actionlint pasaron. En admisión se ejecutaron
179 mutaciones: 141 detectadas y 38 supervivientes, principalmente cambios en diagnósticos y
metadatos; no se declara una puntuación perfecta ni validación cloud.
