# syntax=docker/dockerfile:1.7
# XORQ orchestration API (FastAPI, xorq/main.py) — standalone, built with uv.
# No secrets here: configuration comes from the environment (docker-compose.yml).

ARG PYTHON_VERSION=3.11

FROM ghcr.io/astral-sh/uv:0.9-python${PYTHON_VERSION}-bookworm-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8

WORKDIR /svc

RUN uv pip install --system --no-cache \
    "fastapi>=0.116.0" \
    "uvicorn[standard]>=0.24.0" \
    "pydantic>=2.9.0"

RUN groupadd --system app \
    && useradd --system --gid app --create-home --shell /bin/bash app_user

COPY --chown=app_user:app xorq/ ./xorq/

USER app_user

EXPOSE 9980

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:9980/tenants/list', timeout=5).status==200 else 1)" || exit 1

CMD ["python", "-m", "xorq.main"]
