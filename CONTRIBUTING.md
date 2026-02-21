# Contributing to dp

Thanks for your interest in contributing to dp! This document covers everything you need to get started.

## Development Setup

```bash
git clone https://github.com/chraltro/db.git
cd db
pip install -e ".[dev]"
cd frontend && npm install && npm run build && cd ..
```

Verify everything works:

```bash
pytest tests/
```

## Project Structure

```
src/dp/           Python package (CLI + engine + server)
frontend/         React + Vite web UI
tests/            pytest test suite
```

See [CLAUDE.md](CLAUDE.md) for a detailed architecture reference.

## Making Changes

### Backend (Python)

1. Source is in `src/dp/`
2. CLI commands live in `cli.py`
3. Engine logic is in `engine/` — `transform.py` is the core SQL DAG engine
4. API endpoints are in `server/app.py`
5. Run `pytest tests/` after changes

### Frontend (React)

1. Source is in `frontend/src/`
2. React 19 + Vite, no TypeScript
3. Dev server: `cd frontend && npm run dev` (port 5173, proxies to 3000)
4. Build: `cd frontend && npm run build`

## Testing

```bash
pytest tests/              # all tests
pytest tests/ -x           # stop on first failure
pytest tests/ -v           # verbose output
pytest tests/test_api.py   # specific file
```

Tests use temporary in-memory DuckDB databases. No external services needed.

## Code Style

- Python 3.10+, type hints throughout
- `from __future__ import annotations` in all modules
- Imports: stdlib, then third-party, then local
- Rich library for terminal output
- Lazy imports in CLI commands (faster startup)

## SQL Style

SQL files are linted with SQLFluff (DuckDB dialect):
- Keywords: UPPER (`SELECT`, `FROM`, `WHERE`)
- Identifiers: lower (`customer_id`, `order_count`)

```bash
dp lint          # check
dp lint --fix    # auto-fix
```

## Pull Requests

1. Fork the repo and create a branch from `main`
2. Make your changes
3. Add or update tests as needed
4. Run `pytest tests/` and make sure everything passes
5. Open a PR with a clear description of what changed and why

Keep PRs focused — one feature or fix per PR.

## Reporting Issues

Open an issue on GitHub with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Python version and OS

## Adding a New CLI Command

1. Add `@app.command()` function in `src/dp/cli.py`
2. Import engine modules lazily (inside the function body)
3. Use `_resolve_project()` for project dir resolution
4. Add corresponding API endpoint in `server/app.py` if needed
5. Add tests

## Adding a New Connector

1. Create a new file in `src/dp/connectors/`
2. Define a connector class following the existing pattern
3. Register it in `src/dp/connectors/__init__.py`
4. Add tests

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
