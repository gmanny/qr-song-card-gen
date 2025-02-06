"""
Fetches track metadata from Spotify and saves it to a JSON DB file.

Usage:
    fetch_track_metadata.py tracklist.txt tracks.json [-f]

Run fetch_track_metadata.py --help for more information.
"""

import argparse
import asyncio
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import anyio
from httpx import AsyncClient, ConnectError, ConnectTimeout, ReadTimeout, WriteTimeout
from lxml import etree

from common import load_track_db, save_track_db
from make_qr_cards import Track


async def fetch_track_metadata(
    client: AsyncClient, track_id: str, set: str, set_index: int
) -> Track | None:
    "Fetches track metadata given the track ID."

    backoff = 0
    backoff_times = [20, 60, 120, 300, 600]

    async def backoff_sleep():
        nonlocal backoff

        if backoff >= len(backoff_times):
            sleep_time = backoff_times[-1] + 60 * backoff
        else:
            sleep_time = backoff_times[backoff]

        print(f"Backing off for {sleep_time} seconds...")
        await asyncio.sleep(sleep_time)

        backoff += 1

    while True:
        try:
            response = await client.get(f"/track/{track_id}")
            if response.status_code >= 400:
                print(
                    f"Error fetching track {track_id}: {response.status_code} {response.reason_phrase}:"
                )
                print(response.text)

                await backoff_sleep()

                continue
            break
        except (ReadTimeout, ConnectTimeout, WriteTimeout, ConnectError) as e:
            print(f"Error requesting Track {track_id}: {e!s}")
            await backoff_sleep()

    html = etree.HTML(response.text)
    data_attrs = {
        "release_date": {
            "tag": "meta",
            "search_attr_name": "name",
            "search_attr_value": "music:release_date",
            "result_attr_name": "content",
        },
        "title": {
            "tag": "meta",
            "search_attr_name": "property",
            "search_attr_value": "og:title",
            "result_attr_name": "content",
        },
        "artist": {
            "tag": "meta",
            "search_attr_name": "name",
            "search_attr_value": "music:musician_description",
            "result_attr_name": "content",
        },
        "album": {
            "tag": "meta",
            "search_attr_name": "property",
            "search_attr_value": "og:description",
            "result_attr_name": "content",
            "result_value_regex": r"· (.+?) ·",
        },
        "album_track": {
            "tag": "meta",
            "search_attr_name": "name",
            "search_attr_value": "music:album:track",
            "result_attr_name": "content",
        },
        "track_url": {
            "tag": "meta",
            "search_attr_name": "property",
            "search_attr_value": "og:url",
            "result_attr_name": "content",
        },
        "album_url": {
            "tag": "meta",
            "search_attr_name": "name",
            "search_attr_value": "music:album",
            "result_attr_name": "content",
        },
        "artist_url": {
            "tag": "meta",
            "search_attr_name": "name",
            "search_attr_value": "music:musician",
            "result_attr_name": "content",
        },
    }

    track_dict = {}
    for attr_name, attr_data in data_attrs.items():
        xpath_expr = f"//{attr_data['tag']}[@{attr_data['search_attr_name']}='{attr_data['search_attr_value']}']"
        result = html.xpath(xpath_expr)
        if len(result) == 0:
            print(f"Error: no {attr_name} found in track {track_id}")
            return None

        result = result[0].attrib[attr_data["result_attr_name"]]

        if "result_value_regex" in attr_data:
            result = re.search(attr_data["result_value_regex"], result).group(1)

        track_dict[attr_name] = result

    track_dict["set"] = set
    track_dict["set_index"] = set_index

    return Track(**track_dict)


album_suffixes = [
    re.compile(r"\s*-?\s*\(?Remastered (\d{4})\)?$"),
    re.compile(r"\s*-?\s*\(?(\d{4}) R?e?c?o?r?d?i?n?g?\s?Re-?[Mm]astere?d?\)?$"),
    "(Radio Mix)",
    "(Expanded)",
    "(Expanded Edition)",
    "(Expanded Version)",
    "(Extended)",
    "(Extended Edition)",
    "(Extended Version)",
    "(Platinum)",
    "(Platinum Edition)",
    "(Platinum Version)",
    "(Legacy)",
    "(Legacy Edition)",
    "(Legacy Version)",
    "(Deluxe)",
    "(Deluxe Edition)",
    "(Deluxe Remastered Edition)",
    "(Deluxe Version)",
    "(Super Deluxe)",
    "(eDeluxe)",
    "(Special Edition)",
    "(Remastered)",
    "(Remixes)",
    "(Bonus Track Version)",
    "(Original Motion Picture Soundtrack)",
    "(International Version)",
    "(International)",
    "(25th Anniversary Edition)",
    "(25th Anniversary Deluxe Edition)",
    "- 20th Anniversary Edition",
    "(The Sound of the Prohibition Era, 1919-1933)",
    "(Golden Gate Edition)",
    "(Mono & Stereo)",
    "(30th Anniversary / Deluxe Edition)",
    "(The Complete Sessions 1994-1995)",
    " [The Official 2010 FIFA World Cup (TM) Song]",
    "(Collector's Edition)",
    ' - From "Four Weddings And A Funeral"',
    ' - Love Theme from "Titanic"',
    " - From Deadpool and Wolverine Soundtrack",
    "[30 Years]",
    "[Deluxe Edition]",
    "(Original Single Mix)",
    "[Single Version]",
    "(Gold Edition)",
    "(Deluxe Tour Edition)",
    "(U.S. Version)",
    "(BonusTrack Version)",
    "(Deluxe Remastered Anniversary Edition)",
    "(Original Mono & Stereo Mix)",
    "(From Barbie The Album)",
]
track_suffixes = [
    " - Remastered Version",
    re.compile(r"\s*/?-?\s*\[?\(?Remastered\s?(\d{4})\s?V?e?r?s?i?o?n?\)?]?$"),
    re.compile(
        r"\s*/?-?\s*\(?(\d{4})\s?-?\s?R?e?c?o?r?d?i?n?g?\s?Re-?[Mm]astere?d?\)?$"
    ),
    re.compile(r"\s*/?-?\s*\(?\[?feat\.[^)]*]?\)?$"),
    ' - Erick "More" Album Mix',
    " - Mono Version",
    " - Original Version",
    " - Single Version",
    " - Radio Edit",
    "(Radio Edit)",
    " - Radio Mix",
    ' - From "Dirty Dancing" Soundtrack',
    " - Original Mix",
    ' - 7" Mix',
    " - Mono",
    "(Mono)",
    " - Remastered",
    " [The Official 2010 FIFA World Cup (TM) Song]",
    "(Rerecorded)",
    "(Remastered)",
    ' - From "Four Weddings And A Funeral"',
    ' - Love Theme from "Titanic"',
    " - From Deadpool and Wolverine Soundtrack",
    "[Short Radio Edit]",
    ' - Studio Recording From "The Voice" Performance',
    " - 30 Years Remaster",
    "(Original Single Mix)",
    " - Remastered Version",
    " - Radio Sample",
    "(Club Mix)",
    " - Full Version; Single",
    " - Pop On-Tour Version",
    " - Justice Vs Simian",  # TODO: Make a regex + support substringing artist names
    ' - From the Film "Pretty Woman"',
    " - Pendulum Mix",
    " - From Barbie The Album",
]


def clean_string(s: str, patterns_to_remove: list[str | re.Pattern]) -> str:
    "Removes patterns from the string."

    for pattern in patterns_to_remove:
        if isinstance(pattern, str):
            s = s.replace(pattern, "").strip()
        else:
            s = re.sub(pattern, "", s).strip()

    return s


track_data_field_sequence = [
    "release_date",
    "title",
    "title_clean",
    "title_override",
    "artist",
    "artist_override",
    "album",
    "album_clean",
    "album_override",
    "album_track",
    "track_url",
    "album_url",
    "artist_url",
    "sets",
]


def reorder_track_data(track_data: dict[str, Any]) -> dict[str, Any]:
    "Reorders track data."

    copy = {}
    for field in track_data_field_sequence:
        field_value = track_data.get(field)
        if field_value is not None:
            copy[field] = field_value
        if field in track_data:
            del track_data[field]

    for k, v in track_data.items():
        copy[k] = v
        del track_data[k]

    for k, v in copy.items():
        track_data[k] = v

    return track_data


def clean_track_data(track_data: dict[str, Any]) -> dict[str, Any]:
    "Cleans track data."

    if "set" in track_data:
        del track_data["set"]
    if "set_index" in track_data:
        del track_data["set_index"]

    track_data["title_clean"] = clean_string(track_data["title"], track_suffixes)
    track_data["album_clean"] = clean_string(track_data["album"], album_suffixes)

    track_data = reorder_track_data(track_data)

    return track_data


async def main():
    "Main method"
    arg_parser = argparse.ArgumentParser(
        prog="python -m fetch_track_metadata",
        description="Fetches track metadata given the list of Spotify track IDs",
    )
    arg_parser.add_argument(
        "list_file", help="Name of file with track ID on each line."
    )
    arg_parser.add_argument(
        "track_db_file",
        help="Name of file with existing track metadata database. Created if it does not exist.",
    )
    arg_parser.add_argument(
        "-s",
        "--set-id",
        dest="set_id",
        help="Sets the set ID if it's not specified in the track list file. "
        "The set ID from the file will be used even if this option is specified.",
    )
    arg_parser.add_argument(
        "--set-id-override",
        dest="set_id_override",
        help="Overrides the set ID of all tracks in the file to the specified value.",
    )
    arg_parser.add_argument(
        "-f",
        "--force-reload",
        dest="force_reload",
        action="store_true",
        help="Forces metadata reload even if the track exists in the database.",
    )
    args = arg_parser.parse_args()

    set_id = args.set_id
    set_id_override = args.set_id_override
    force_reload = args.force_reload

    track_db_file = Path(args.track_db_file)
    if track_db_file.exists() and not track_db_file.is_file():
        print(f"File {track_db_file} exists but is not a file.")
        exit(1)

    if not track_db_file.exists():
        track_db = {"tracks": {}}
    else:
        track_db = await load_track_db(track_db_file)

    db_reprocess_mode = args.list_file == "="
    if db_reprocess_mode:
        track_ids = list(track_db["tracks"].keys())
    else:
        list_file = Path(args.list_file)
        if not list_file.exists() or not list_file.is_file():
            print(f"File {list_file} does not exist or is not a file.")
            exit(1)
        async with await anyio.open_file(list_file, "r") as file:
            track_ids = [line.strip() for line in await file.readlines()]

    spotify_client = AsyncClient(
        base_url="https://open.spotify.com/",
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        },
    )

    already_reloaded: set[str] = set()

    start = datetime.now()
    for idx, track_id_def in enumerate(track_ids):
        if len(track_id_def.strip()) == 0 or track_id_def.startswith("#"):
            continue

        tokens = track_id_def.split(";")
        track_id = tokens[0]
        track_set = tokens[1] if len(tokens) > 1 else None
        if set_id_override is not None:
            track_set = set_id_override
        elif track_set is None:
            track_set = set_id
        track_index = int(tokens[2]) if len(tokens) > 2 else idx + 1

        if track_set is None and not db_reprocess_mode:
            print(f"Error: Track {track_id} has no set ID, use `--set-id` to set it.")
            exit(1)

        existing_track_data: dict[str, Any] | None = track_db["tracks"].get(track_id)
        if existing_track_data and (not force_reload or track_id in already_reloaded):
            if not db_reprocess_mode:
                sets = existing_track_data.setdefault("sets", {})
                sets[track_set] = track_index

            clean_track_data(existing_track_data)

            print(
                f"Skipping track {track_id} because it already exists in the database: "
                f"{existing_track_data['artist']} - {existing_track_data['title']}"
            )
            continue

        if db_reprocess_mode:
            raise RuntimeError(
                f"Came across track {track_id} that is not in DB during reprocessing mode."
            )

        print(f"Fetching metadata for track {track_id}...")

        track = await fetch_track_metadata(
            spotify_client, track_id, track_set, track_index
        )
        if track is None:
            print(
                f"Skipping track {track_id} at position {track_index} because it could not be fetched."
            )
            continue
        print(f"Saving track {track_id} to database: {track}")

        new_track_data = track._asdict() | {"sets": {track_set: track_index}}
        new_track_data = clean_track_data(new_track_data)

        # Preserve overrides from the database.
        if existing_track_data is not None:
            new_track_data["title_override"] = existing_track_data["title_override"]
            new_track_data["artist_override"] = existing_track_data["artist_override"]
            new_track_data["album_override"] = existing_track_data["album_override"]

        track_db["tracks"][track_id] = new_track_data
        await save_track_db(track_db_file, track_db)

        # Take 5 seconds between requests
        await asyncio.sleep(5 - (datetime.now() - start).total_seconds())
        start = datetime.now()

    await save_track_db(track_db_file, track_db)


if __name__ == "__main__":
    asyncio.run(main())
