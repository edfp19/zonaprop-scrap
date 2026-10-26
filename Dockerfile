FROM ghcr.io/astral-sh/uv:0.9.26-python3.11-bookworm-slim AS runtime-base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH=/app/src \
    PATH="/app/.venv/bin:$PATH"

FROM runtime-base AS pipeline
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --extra collection --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --extra collection

FROM runtime-base AS api
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --extra api --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --extra api

FROM runtime-base AS dashboard
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --extra dashboard --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --extra dashboard
