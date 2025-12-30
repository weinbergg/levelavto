## Encar (Carapis) integration

Minimal notes to fetch Encar data through the `encar` PyPI client (Carapis).

- Env: set `ENCAR_CARAPIS_API_KEY` (or `CARAPIS_API_KEY`) in `.env`. Optional: `ENCAR_CARAPIS_BASE_URL` to point to a custom gateway.
- Config lives in `backend/app/parsing/sites_config.yaml` under `encar` (brands, limits, pages).
- Quick smoke test (no DB): `docker-compose run --rm web python -m backend.app.tools.encar_carapis_smoketest`
- Full parser run: `docker-compose run --rm web python -m backend.app.parsing.runner --source encar --trigger manual`
- Sample JSON for debugging lands at `/app/tmp/encar_sample.json`.

The adapter retries transient HTTP errors, limits pages/limit per query from config, and always pulls full photo sets for each vehicle.
