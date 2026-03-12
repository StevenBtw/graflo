"""I/O utilities for migration workflows."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from suthing import FileHandle

from graflo.architecture.schema import Schema


def load_schema(path: str | Path) -> Schema:
    """Load and initialize schema from YAML path."""
    schema_raw = FileHandle.load(path)
    schema = Schema.from_dict(schema_raw)
    schema.finish_init()
    return schema


def schema_hash(schema: Schema) -> str:
    """Stable hash of schema for migration revision tracking."""
    payload = json.dumps(schema.to_dict(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def plan_to_json_serializable(plan: Any) -> dict[str, Any]:
    """Convert pydantic plan-like object to JSON payload."""
    if hasattr(plan, "model_dump"):
        return plan.model_dump()
    if hasattr(plan, "to_dict"):
        return plan.to_dict()
    raise TypeError(f"Unsupported plan object type: {type(plan)}")
