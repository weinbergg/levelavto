"""Pytest bootstrap so ``from backend.app...`` imports work everywhere.

Inside the production container ``WORKDIR`` is ``/app`` and the source
tree lives at ``/app/backend/app/...``. When pytest is launched with
``backend/tests/...`` from the container's ``/app`` cwd it auto-adds
``/app/backend`` to ``sys.path`` (the rootdir), which is enough for
``from app...`` imports but breaks the ``from backend.app...`` style
the rest of the codebase uses.

This conftest sits at ``backend/conftest.py`` so pytest discovers it
during collection and we can prepend the *parent* of ``backend/`` to
``sys.path``. That parent is guaranteed to be the directory containing
``backend`` regardless of cwd, so the same conftest works on:

* the developer machine (``cd code && pytest backend/tests/...``)
* the VPS container (``docker compose exec -T web pytest backend/tests/...``)
* any future CI runner.
"""

import sys
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _BACKEND_DIR.parent

for candidate in (_PROJECT_ROOT, _BACKEND_DIR):
    candidate_str = str(candidate)
    if candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)
