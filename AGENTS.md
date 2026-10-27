# Project preferences

## Purpose and communication

- This is a teaching project. Prefer the smallest clear implementation that teaches the concept;
  avoid production infrastructure and abstractions without a demonstrated need.
- Explain the core concepts, responsibilities, and important files before implementation details.
  Do not default to line-by-line walkthroughs or repeated quizzes unless requested.
- Guided manual infrastructure setup is part of the learning experience. The user is comfortable
  running SQL and using provider consoles; do not assume these steps should be automated away.
  Before each step, explain what it changes, why it is needed, and the expected result. Automate
  repetition when requested or when it no longer helps teach the concept.
- Keep lessons under `lessons/`, following the existing standalone HTML format. Put an architecture
  summary and a diagram of the main flow near the top. Separate implemented behavior from plans.
- Explain what changed, why, how it was checked, and any meaningful limitations in plain language.

## Python and data contracts

- Use typed Python for backend code, data processing, and operational scripts. Annotate function
  parameters and returns, and keep shared data contracts explicit.
- Prefer dataclasses for structured internal values and results rather than loosely shaped
  dictionaries or positional tuples. Use frozen dataclasses when the values should be immutable.
- Type annotations and dataclasses do not enforce field types at runtime. Validate untrusted input
  at system boundaries; use explicit Spark schemas for DataFrames and existing validation tools
  where appropriate. Avoid adding duplicate validation to already validated internal values.
- Prefer interfaces and modules with clear responsibilities over additional layers or frameworks.

## Tooling and correctness

- Use `uv` and the checked-in lockfile for reproducible Python environments.
- Keep Ruff as the linter. Preserve the configured Python target, import rules, and line length.
- Provide repeatable correctness checks appropriate to a change. Distinguish linting, static type
  checking, runtime validation, and behavioral tests; none substitutes for all the others.
- Ruff and pytest are configured. Mypy checks Gold, the API, dashboard server, orchestration,
  operations and weekly retention; mutmut targets Gold price assessment and capacity admission.
  Extend these focused scopes when important logic warrants it.
  Document reproducible commands and do not claim checks ran when they did not.

## Testing preferences

- Tests must earn their keep: exercise observable behavior and realistic failure modes. Avoid tests
  that merely mirror implementation, assert incidental structure, or exist only to raise coverage.
- Prioritize important logic first: eligibility and comparison rules, price calculations, history
  selection, idempotency, and publication integrity.
- Use focused mutation testing on those important areas to assess whether tests detect incorrect
  behavior. Favor meaningful mutants such as changed thresholds, inverted conditions, omitted
  filters, and altered calculations. Strengthen tests for meaningful surviving mutants before
  expanding broad test coverage.
- Keep the testing approach proportional to this teaching project. Small deterministic fixtures
  and focused integration tests are preferred; routine tests should not depend on the live website.
- Report passed, failed, and skipped checks accurately. A skipped Spark test is not validation of
  Spark behavior. Do not add tests for prose-only documentation changes.

## Bootstrap and operations

- Keep `docker compose up --build` as the single-command application bootstrap after optional
  `.env` configuration. Required services and runtime dependencies belong in Compose/images.
- Do not require host-installed Python, Java, Spark, Chrome, or PostgreSQL to run the application.
  Local `uv` workflows remain useful optional development paths.
- Keep service dependencies scoped: Spark and Java belong in the pipeline image, not API or
  dashboard images. Preserve persistent data across ordinary container restarts.
- Document copyable commands for inspection, replay, and relevant checks. Do not introduce extra
  services or orchestration systems without a concrete need.

## Agreed data preferences

- Rough historical comparisons are sufficient for this teaching project. The intended retention
  policy is the latest weekly observation per listing, operation, and currency for five weeks.
- Bronze extracts may be deleted after their Silver publication has been successfully verified.
  The loss of intermediate history and full historical rebuild capability is an accepted tradeoff.
- These are intended policies, not a claim that cleanup exists. Implement retention explicitly,
  including its interaction with replay and immutable run manifests; do not silently delete data
  during unrelated work.

## Current validation commands

```bash
uv run ruff check .
uv run pytest -q
uv run mypy
uv run mutmut run
```

Run the Silver tests with Spark available in the pipeline image:

```bash
docker compose run --rm --no-deps -v "$PWD/tests:/app/tests:ro" pipeline \
  uv run --no-sync --with pytest python -m pytest tests/listings -q
```

The local pytest command can skip Silver tests when PySpark is absent. The container command may
download the temporary pytest dependencies.
