# One image, three start commands. The node stage builds the SPA; the python
# stage installs the backend and copies the bundle in, so a single container
# serves the whole product.

# --------------------------------------------------------------------------
# Stage 1 -- build the frontend bundle
# --------------------------------------------------------------------------
FROM node:20-slim AS frontend

WORKDIR /build

# Copy the manifests alone first, so a source-only change does not re-run
# npm install.
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci --no-audit --no-fund

COPY frontend/ ./
RUN npm run build


# --------------------------------------------------------------------------
# Stage 2 -- the application image
# --------------------------------------------------------------------------
FROM python:3.12-slim AS app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

WORKDIR /srv

# Dependencies before source, so an application edit reuses the layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY alembic.ini ./
COPY alembic/ ./alembic/
COPY app/ ./app/
COPY scripts/ ./scripts/
# hatchling reads README.md to build the project's own metadata.
COPY README.md ./
RUN uv sync --frozen --no-dev

COPY --from=frontend /build/dist ./frontend/dist

# Non-root: nothing here needs to write to the filesystem.
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /srv
USER appuser

EXPOSE 8000

# Overridden per service in compose and on Railway. The api is the default
# because it is the one that has to answer a health check.
CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
