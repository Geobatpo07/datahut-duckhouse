# syntax=docker/dockerfile:1.7
# Arrow Flight server (flight_server.app.app_xorq) — built with uv.
# No secrets here: runtime config comes from the environment (see docker-compose.yml
# + .devcontainer/.env.example / project .env).

ARG PYTHON_VERSION=3.11

FROM ghcr.io/astral-sh/uv:0.9-python${PYTHON_VERSION}-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential git \
    && rm -rf /var/lib/apt/lists/*

# Resolve + install dependencies only (cached until pyproject.toml / uv.lock change).
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --frozen --no-install-project --no-dev


FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

RUN groupadd --system app \
    && useradd --system --gid app --create-home --shell /bin/bash app_user

WORKDIR /app

# Bring in the resolved virtualenv, then the source.
COPY --from=builder --chown=app_user:app /app/.venv /app/.venv
COPY --chown=app_user:app . /app

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8

RUN mkdir -p /app/ingestion/data /app/snapshots /app/logs \
    && chown -R app_user:app /app/ingestion /app/snapshots /app/logs

USER app_user

EXPOSE 8815

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import socket; socket.create_connection(('localhost', 8815), timeout=5).close()" || exit 1

CMD ["python", "-m", "flight_server.app.app_xorq"]
