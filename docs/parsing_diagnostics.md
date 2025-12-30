# Parsing diagnostics

Run quick field-completeness and sanity checks per source on a small sample (pagination.max_pages=2 in `sites_config.yaml`).

Examples:
```
docker-compose run --rm web python -m backend.app.tools.parsing_diagnostics --source all
docker-compose run --rm web python -m backend.app.tools.parsing_diagnostics --source mobile_de
docker-compose run --rm web python -m backend.app.tools.parsing_diagnostics --source encar
docker-compose run --rm web python -m backend.app.tools.parsing_diagnostics --source emavto_klg
```

Output includes:
- Last run summary (total_seen/inserted/updated/deactivated).
- Total rows for the source in DB.
- Field completeness (filled/total, %) and min/max for numeric fields (year, mileage, price).
- Distinct values for brand/currency/country.

Notes:
- The tool invokes `ParserRunner` to fetch the latest sample according to YAML config, then inspects the `cars` table.
- Keep `pagination.max_pages=2` for non-intrusive sampling; increase later for production runs.

