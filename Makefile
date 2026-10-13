.PHONY: up down logs test lint fmt fmt-check scan check

up: ## build & boot everything (API + worker + mongo)
	docker compose up --build -d control-plane worker
	@echo "API: http://localhost:8000  (docs: http://localhost:8000/docs)"

down:
	docker compose down -v

logs:
	docker compose logs -f control-plane worker

test: ## full automated test suite
	docker compose run --rm --build tests

lint:
	docker compose run --rm --no-deps tests ruff check .

fmt:
	docker compose run --rm --no-deps tests ruff format .

fmt-check:
	docker compose run --rm --no-deps tests ruff format --check .

scan:
	docker compose run --rm --no-deps tests bandit -c pyproject.toml -r app worker scripts -q

check: ## single command: lint + format check + security scan + tests
	$(MAKE) lint fmt-check scan test
