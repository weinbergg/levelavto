#!/bin/sh
set -e

MINMAX=$(docker compose exec -T db psql -U autodealer -d autodealer -t -A -c \
"select min(id), max(id)
 from cars
 where is_available=true
   and country='DE'
   and total_price_rub_cached is null;")

MIN_ID=$(echo "$MINMAX" | cut -d'|' -f1)
MAX_ID=$(echo "$MINMAX" | cut -d'|' -f2)

echo "min=$MIN_ID max=$MAX_ID"

docker compose exec -T web sh -lc "
R1=$MIN_ID
R2=\$((MIN_ID + (MAX_ID-MIN_ID)/4))
R3=\$((MIN_ID + (MAX_ID-MIN_ID)/2))
R4=\$((MIN_ID + 3*(MAX_ID-MIN_ID)/4))
R5=$MAX_ID

for RANGE in \"\$R1 \$R2\" \"\$R2 \$R3\" \"\$R3 \$R4\" \"\$R4 \$R5\"; do
  set -- \$RANGE
  START=\$1
  END=\$2
  START_ID=\$START END_ID=\$END python -u - <<'PY' &
import os
from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.cars_service import CarsService

start_id = int(os.environ['START_ID'])
end_id = int(os.environ['END_ID'])
batch = 200
processed = 0

with SessionLocal() as db:
    svc = CarsService(db)
    while True:
        rows = (
            db.query(Car)
            .filter(
                Car.id >= start_id,
                Car.id < end_id,
                Car.is_available.is_(True),
                Car.country == 'DE',
                Car.total_price_rub_cached.is_(None),
            )
            .order_by(Car.id)
            .limit(batch)
            .all()
        )
        if not rows:
            print(f\"[done] range {start_id}-{end_id} processed={processed}\", flush=True)
            break
        for car in rows:
            try:
                svc.ensure_calc_cache(car)
            except Exception:
                pass
            processed += 1
        db.commit()
        print(f\"[progress] range {start_id}-{end_id} processed={processed}\", flush=True)
PY

done
wait
" | tee /opt/levelavto/logs/recalc_de.log
