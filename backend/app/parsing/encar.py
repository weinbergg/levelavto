from __future__ import annotations

# Deprecated HTML parser replaced by Carapis-backed implementation.
from .encar_carapis import EncarCarapisParser

EncarParser = EncarCarapisParser

__all__ = ["EncarParser", "EncarCarapisParser"]
