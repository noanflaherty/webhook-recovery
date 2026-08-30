# Local development entry points.
#
# The whole system is `make up`. Everything else here is a shortcut for a
# command that already exists -- nothing in this file is load-bearing, so a
# reviewer who ignores it entirely still gets the same results from the README.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# uv ignores an active VIRTUAL_ENV that is not the project's, but warns loudly
# about it on every invocation. Clearing it keeps the output readable.
UV := VIRTUAL_ENV= uv
COMPOSE := docker compose
NPM := npm --prefix frontend

API ?= http://localhost:8000
WORKERS ?= 3

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------------

.PHONY: install
install: ## Install python and node dependencies
	$(UV) sync
	$(NPM) install

# --------------------------------------------------------------------------
# The whole system
# --------------------------------------------------------------------------

.PHONY: up
up: ## Build and run everything: postgres, migrate, api, conductor, 3 workers
	$(COMPOSE) up --build --scale worker=$(WORKERS)

.PHONY: up-d
up-d: ## Same, detached
	$(COMPOSE) up --build -d --scale worker=$(WORKERS)

.PHONY: down
down: ## Stop everything (keeps the database volume)
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop everything and drop the database volume
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail logs from every service
	$(COMPOSE) logs -f

.PHONY: ps
ps: ## What is running
	$(COMPOSE) ps

# --------------------------------------------------------------------------
# Running pieces on the host, against the compose postgres
# --------------------------------------------------------------------------

.PHONY: db
db: ## Start postgres alone and apply migrations
	$(COMPOSE) up -d postgres
	@until $(COMPOSE) exec -T postgres pg_isready -U postgres -d webhook_recovery >/dev/null 2>&1; \
		do sleep 0.3; done
	$(UV) run alembic upgrade head

.PHONY: api
api: ## Run the api on :8000 with reload
	$(UV) run uvicorn app.api.main:app --reload --port 8000

.PHONY: conductor
conductor: ## Run one conductor
	$(UV) run python -m app.conductor

.PHONY: worker
worker: ## Run one worker
	$(UV) run python -m app.worker

.PHONY: web
web: ## Run the Vite dev server on :5173, proxying /api to :8000
	$(NPM) run dev

.PHONY: psql
psql: ## Open a psql shell on the compose database
	$(COMPOSE) exec postgres psql -U postgres -d webhook_recovery

# --------------------------------------------------------------------------
# Migrations
# --------------------------------------------------------------------------

.PHONY: migrate
migrate: ## Apply migrations
	$(UV) run alembic upgrade head

.PHONY: migration
migration: ## Autogenerate a migration: make migration m="add the thing"
	@test -n "$(m)" || (echo 'usage: make migration m="what changed"' && exit 1)
	$(UV) run alembic revision --autogenerate -m "$(m)"
	@echo
	@echo "Read the generated file before committing it -- autogenerate gets"
	@echo "partial index predicates subtly wrong more often than anything else."

.PHONY: migration-check
migration-check: ## Fail if the models have drifted from the migrations
	$(UV) run alembic check

# --------------------------------------------------------------------------
# Quality
# --------------------------------------------------------------------------

.PHONY: check
check: lint typecheck test ## lint + typecheck + test

.PHONY: lint
lint: ## Lint and check formatting
	$(UV) run ruff check .
	$(UV) run ruff format --check .

.PHONY: fmt
fmt: ## Auto-fix lint and format
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

.PHONY: typecheck
typecheck: ## mypy, strict, over app/
	$(UV) run mypy app

.PHONY: test
test: ## Run the test suite
	$(UV) run pytest

.PHONY: build-web
build-web: ## Type-check and build the frontend bundle
	$(NPM) run build

# --------------------------------------------------------------------------
# Contract
# --------------------------------------------------------------------------

.PHONY: fixtures
fixtures: ## Regenerate the frontend fixtures and openapi.json from the models
	$(UV) run python scripts/gen_fixtures.py

.PHONY: fixtures-check
fixtures-check: ## Fail if the committed fixtures are stale
	@$(MAKE) --no-print-directory fixtures >/dev/null
	@git diff --quiet -- frontend/src/fixtures openapi.json \
		|| (echo "fixtures are stale -- run 'make fixtures' and commit the result" && exit 1)
	@echo "fixtures up to date"

# --------------------------------------------------------------------------
# Verification (the Phase 0 exit criteria)
# --------------------------------------------------------------------------

.PHONY: verify
verify: ## Check a running stack: health, processes, schema, clock, bundle
	@./scripts/verify.sh $(API)
