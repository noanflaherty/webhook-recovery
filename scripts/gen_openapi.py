"""Regenerate ``openapi.json`` from the FastAPI app.

The committed schema is the *generated witness* of the frozen contract in
``app/api/schemas.py``: `frontend/src/api/types.ts` is a hand-written mirror of
those models, and this file is what a reviewer diffs it against. Checking the
generated schema into the repo is what turns "the frontend types match the
backend" from a claim into something CI fails on.

This used to be four lines at the bottom of ``scripts/gen_fixtures.py``, which
also synthesized the replay fixtures. The fixtures are gone; the witness is not,
so it moved somewhere its name says what it does.

    uv run python scripts/gen_openapi.py
"""

from __future__ import annotations

import json
from pathlib import Path

from app.api.main import create_app

OPENAPI_PATH = Path("openapi.json")


def main() -> None:
    OPENAPI_PATH.write_text(json.dumps(create_app().openapi(), indent=2) + "\n")
    print(f"wrote {OPENAPI_PATH}")


if __name__ == "__main__":
    main()
