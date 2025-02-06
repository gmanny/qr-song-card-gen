import json
from pathlib import Path
from typing import Any

import anyio


async def load_track_db(track_db_file: str | Path) -> dict[str, Any]:
    "Loads track database from file."
    async with await anyio.open_file(track_db_file, "r") as file:
        track_db = json.loads(await file.read())
    return track_db


async def save_track_db(track_db_file: str | Path, track_db: dict[str, Any]) -> None:
    "Saves track database to file."
    async with await anyio.open_file(track_db_file, "w") as file:
        await file.write(json.dumps(track_db, indent=4))
