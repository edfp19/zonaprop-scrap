# Website deployment

The cloud website uses the same Leaflet files and FastAPI queries as Compose.
Vercel serves the generated `public/` files and runs `app.py` as a Python function.
The function reads Gold from Neon with `web_reader` over the pooled TLS connection.
Spark, Java, Selenium and Flask are excluded from the installed API dependencies.

## Neon setup

1. Select the intended database in Neon's SQL editor.
2. Run `infra/postgres/init.sql` as the database owner.
3. Run `infra/postgres/web_reader.sql` as the same owner.
4. Set a password privately in the SQL editor with
   `ALTER ROLE web_reader PASSWORD 'REPLACE_WITH_YOUR_PASSWORD';`.
   Do not save the real statement in the repository or chat.
5. Use Neon's connection dialog to obtain the **pooled** connection URL for `web_reader`,
   including `sslmode=require`. Add it directly to Vercel as `DATABASE_URL` for Production
   and Preview. Do not use the owner URL for the website.

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

The install command uses `uv sync --active --frozen --no-dev --extra api`: `--active` installs
into Vercel's build environment, and the committed lockfile pins dependency versions. The build
command copies the dashboard assets into `public/`; no second copy is maintained in Git.
`/api/mapa` is an alias for the existing `/mapa` query, so the browser URL works in both hosts.

## Local checks

```bash
uv run python infra/vercel/build_static.py
uv run ruff check .
uv run mypy
uv run pytest tests/test_config.py tests/dashboard -q
```

Compose's existing API and Flask dashboard keep their separate images. Running the build script
does not start collection or contact Neon. The cloud function requires Python 3.12, which is
within the project's supported range. A successful local check does not validate a Vercel build
or Neon grants; those must be checked on the first deployment.

## Still pending

The initial Gold seed is committed in Neon: 96 rows, with 95 map locations. The reader login
was verified with a direct connection returning current_user=web_reader and total=96.
The first Vercel deployment, Silver bucket setup, and the GitHub Actions batch job are pending.
No workflow or schedule has been enabled. Silver retention and Bronze cleanup remain
planned policies. The current Gold freshness window is 35 days; a retained snapshot does not
automatically become fresh as time passes.

Provider references: [FastAPI on Vercel](https://vercel.com/docs/frameworks/backend/fastapi),
[Python dependencies and bundling](https://vercel.com/docs/functions/runtimes/python).
