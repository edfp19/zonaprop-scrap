# Website deployment

The cloud website uses the same Leaflet files and FastAPI queries as Compose.
Vercel runs `app.py` as a Python function, serving the existing dashboard HTML and a
FastAPI StaticFiles mount for CSS/JavaScript. Vercel can promote the static mount to its CDN.
The function reads Gold from Neon with `web_reader` over the pooled TLS connection.
Spark, Java, Selenium and Flask are excluded from the installed API dependencies.

## Neon setup

1. Select the intended database in Neon's SQL editor.
2. Run `infra/postgres/init.sql` as the database owner.
3. Run `infra/postgres/web_reader.sql` as the same owner.
4. Set a password privately in the SQL editor with
   `ALTER ROLE web_reader PASSWORD 'REPLACE_WITH_YOUR_PASSWORD';`.
   Do not save the real statement in the repository or chat.
5. Obtain the **pooled** connection URL for `web_reader`, including `sslmode=require`.
   If the Connect role menu does not list the SQL-created reader, construct its URL using
   `postgresql://web_reader:ENCODED_PASSWORD@POOLED_HOST/neondb?sslmode=require`, inserting
   `-pooler` after the endpoint ID in the known host. URL-encode special characters in the
   password. Add the URL directly to Vercel as `DATABASE_URL` for Production and Preview.
   Do not use the owner URL for the website. Surrounding whitespace is trimmed by Settings.

The reader has SELECT on the two serving tables and no write grants. Creation of roles requires
the owner connection. This setup does not upload property data; the database still needs a seed.

## Import the initial Gold snapshot with psql

The ignored seed file, prepared from the local PostgreSQL snapshot, can be imported without
pasting it into the SQL Editor. It refuses a nonempty target and commits only after its checks.
The existing PostgreSQL container includes psql; no host client installation is required.

```bash
docker compose cp data/cloud-seed/neon-gold-seed.sql postgres:/tmp/neon-gold-seed.sql
docker compose exec \
  -e PGHOST=YOUR_DIRECT_NEON_HOST \
  -e PGUSER=YOUR_NEON_OWNER_ROLE \
  -e PGDATABASE=neondb \
  -e PGSSLMODE=require \
  postgres psql -W -v ON_ERROR_STOP=1 -f /tmp/neon-gold-seed.sql
```

Get the host and owner role from Neon's Connect dialog with pooling disabled. Enter the
owner password at psql's hidden prompt, not as part of the command. The reader cannot import
rows. `ON_ERROR_STOP` stops execution on the first SQL error; an uncommitted transaction rolls
back when the client disconnects. Expected results: total=96 and on_map=95. The seed file is a
local generated artifact, so it is not available from a fresh clone until exported again.

After importing, verify the website login separately. Repeat the connection command using
`PGUSER=web_reader` and its own password, replacing `-f /tmp/neon-gold-seed.sql` with
`-c 'SELECT count(*) AS total FROM gold.opportunities;'`. Expect 96. Table read grants do not
automatically allow the owner login to `SET ROLE web_reader`; role membership is a separate
permission, so the seed does not switch roles.

For Neon password resets, use its Console role reset or `ALTER ROLE ... PASSWORD` in the
SQL Editor. Do not use psql's `\password`: it sends a pre-hashed verifier, which Neon rejects
with "Neon only supports being given plaintext passwords". Plaintext here describes the
password supplied to Neon, not an unencrypted network connection; keep TLS enabled.

## Vercel setup

After code is pushed and Gold is seeded:

1. Import `edfp19/zonaprop-scrap` into a personal Hobby project.
2. Keep the repository root as Root Directory and select the FastAPI framework preset.
3. Set `DATABASE_URL` as described above; deploy using the checked-in `vercel.json` commands.
4. Check `/salud`, `/api/mapa` and `/` on the deployment URL. The seeded dataset currently
   has 96 Gold rows, of which 95 have coordinates. Confirm this against the actual seed.
5. Inspect the first build log: only the `api` extra should be installed, with no pyspark or
   selenium. Check the function bundle size in the deployment output.

The install command uses
`uv sync --active --frozen --no-dev --extra api --no-editable --reinstall-package zona-oportunidades`:
`--active` installs into Vercel's build environment, and the committed lockfile pins dependency
versions. `--no-editable` installs the actual package files instead of a link to the build
checkout's `src/` directory, which is not a usable import path in the deployed function.
`--reinstall-package` refreshes our package from source even if uv has cached an older wheel.
The cloud entrypoint serves the packaged dashboard files directly; no asset-copy build command
or second source copy is needed.
`/api/mapa` is an alias for the existing `/mapa` query, so the browser URL works in both hosts.

## Local checks

```bash
uv run ruff check .
uv run mypy
uv run --extra api --extra dashboard --with httpx pytest tests/test_config.py tests/test_cloud_entrypoint.py tests/dashboard -q
```

Check the installed API package independently of checkout paths and editable `.pth` links:

```bash
uv venv --python 3.12 /tmp/zonaprop-vercel-env
VIRTUAL_ENV=/tmp/zonaprop-vercel-env uv sync --active --frozen --no-dev --extra api --no-editable --reinstall-package zona-oportunidades
/tmp/zonaprop-vercel-env/bin/python -I -S -c '
import sys
sys.path.insert(0, "/tmp/zonaprop-vercel-env/lib/python3.12/site-packages")
from zona_oportunidades.presentation.api.main import app, health
assert health() == {"estado": "ok"}
assert any(route.path == "/api/mapa" for route in app.routes)
print("Installed API imports without editable links")
'
```

This simulates loading vendored packages by adding site-packages explicitly without processing
editable links. It checks imports and the health function, not a live Neon connection or the
complete Vercel bundle.

Compose's existing API and Flask dashboard keep their separate images. Local asset checks
do not start collection or contact Neon. Vercel uses Python 3.12, which is
within the project's supported range. A successful local check does not validate a Vercel build
or Neon grants; those must be checked on the first deployment.

## Still pending

The initial Gold seed is committed in Neon: 96 rows, with 95 map locations. The reader login
was verified with a direct connection returning current_user=web_reader and total=96.
The website is verified at <https://zonaprop-scrap.vercel.app/>: HTML and map API return 200,
the API returns 96 rows, and the browser displays 95 locations. A loaded photo, its Zonaprop
link, green/red pins, the above-average filter, and a card inside a mobile viewport were checked.
Silver bucket setup and the GitHub Actions batch job are pending.
No workflow or schedule has been enabled. Silver retention and Bronze cleanup remain
planned policies. The current Gold freshness window is 35 days; a retained snapshot does not
automatically become fresh as time passes.

Provider references: [FastAPI on Vercel](https://vercel.com/docs/frameworks/backend/fastapi),
[Python dependencies and bundling](https://vercel.com/docs/functions/runtimes/python).
