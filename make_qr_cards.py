#!/usr/bin/env python3

# Copyright 2023 Ruud van Asseldonk
# Copyright 2025 Slava Kolobaev

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.

from __future__ import annotations

import argparse
import asyncio
import html
import os
import random
import subprocess
from datetime import date
from pathlib import Path

import anyio
import qrcode

from typing import Any, Iterable, Literal, NamedTuple
from collections import Counter, defaultdict

from qrcode.image.svg import SvgPathImage

from common import load_track_db


class Track(NamedTuple):
    release_date: date
    title: str
    artist: str
    album: str
    album_track: int

    track_url: str
    album_url: str
    artist_url: str

    set: str
    set_index: int

    @property
    def release_year(self) -> int:
        return self.release_date.year

    def qr_svg(self) -> tuple[str, int]:
        """
        Render a QR code for the URL as SVG path, return also the side length
        (in SVG units, which by convention we map to mm).
        """
        import lxml.etree as ET

        # A box size of 10 means that every "pixel" in the code is 1mm, but we
        # don't know how many pixels wide and tall the code is, so return that
        # too, the "pixel size". Note, it is independent of the specified box
        # size, we always have to divide by 10.
        qr = qrcode.make(self.track_url, image_factory=SvgPathImage, box_size=8)
        return ET.tostring(qr.path).decode("ascii"), qr.pixel_size / 10


def line_break_text(s: str) -> list[str]:
    """
    Line break the artist and title so they (hopefully) fit on a card. This is a
    hack based on string lengths, but it's good enough for most cases.
    """
    if len(s) < 24:
        return [s]

    words = s.split(" ")

    if len(s) > 48:
        # Achieve three lines by splitting the first 48 characters evenly and adding the rest as a third line.
        first_two_lines = ""
        first_third_line_word_index = None
        for i, word in enumerate(words):
            if len(first_two_lines + word) >= 48:
                first_third_line_word_index = i
                break
            first_two_lines += word + " "

        return line_break_text(first_two_lines.strip()) + [
            " ".join(words[first_third_line_word_index:])
        ]

    char_count = sum(len(word) for word in words)

    # The starting situation is everything on the first line. We'll try out
    # every possible line break and pick the one with the most even distribution
    # (by characters in the string, not true text width).
    top, bot = " ".join(words), ""
    diff = char_count

    # Try line-breaking between every word.
    for i in range(1, len(words) - 1):
        w1, w2 = words[:i], words[i:]
        t, b = " ".join(w1), " ".join(w2)
        d = abs(len(t) - len(b))
        if d < diff:
            top, bot, diff = t, b, d

    return [top, bot]


def render_text_svg(x_mm: float, y_mm: float, s: str, class_: str) -> Iterable[str]:
    """
    Render the artist or title, broken across lines if needed.
    """
    lines = line_break_text(s)
    line_height_mm = 6
    h_mm = line_height_mm * len(lines)

    for i, line in enumerate(lines):
        dy_mm = line_height_mm * (1 + i) - h_mm / 2
        yield (
            f'<text x="{x_mm}" y="{y_mm + dy_mm}" text-anchor="middle" '
            f'class="{class_}">{html.escape(line)}</text>'
        )


class Table(NamedTuple):
    """
    A table of cards laid out on two-sided paper.
    """

    cells: list[Track]

    # Original cards are 65mm wide, so on a 210mm wide A4 paper, we can fit
    # 3 columns and still have 7mm margin on both sides. That may be a bit
    # tight but either way, let's do 3 columns.
    width: int = 3

    # In the 297mm A4 paper, if we put 4 rows of 65mm that leaves 37mm of
    # margin, about 20mm top and bottom.
    height: int = 4

    @staticmethod
    def new() -> Table:
        return Table(cells=[])

    def append(self, track: Track) -> None:
        self.cells.append(track)

    def is_empty(self) -> bool:
        return len(self.cells) == 0

    def is_full(self) -> bool:
        return len(self.cells) >= self.width * self.height

    def render_svg(
        self,
        font: str,
        grid: bool,
        crop_marks: bool,
        mode: Literal["qr"] | Literal["title"],
        page_footer: str,
    ) -> str:
        """
        Render the front of the page as svg. The units are in millimeters.
        """
        # Size of the page.
        w_mm = 210
        h_mm = 297
        # Using 65mm cards leaves only 7.5 mm per side as margin, but we're still going
        #  with it.
        side_mm = 65

        tw_mm = side_mm * self.width
        th_mm = side_mm * self.height
        hmargin_mm = (w_mm - tw_mm) / 2
        vmargin_mm = (h_mm - th_mm) / 2
        # Align the table top-left with a fixed margin and leave more space at
        # the bottom, so we can put a page number there.
        vmargin_mm = hmargin_mm

        parts: list[str] = []
        parts.append(
            '<svg version="1.1" width="210mm" height="297mm" '
            'viewBox="0 0 210 297" '
            'xmlns="http://www.w3.org/2000/svg">'
        )
        parts.append(
            f"""
            <style>
            text {{ font-family: {font!r}; }}
            .year {{ font-size: 18px; font-weight: 900; }}
            .title, .artist, .footer {{ font-size: 5.2px; }}
            .artist {{ font-weight: bold; }}
            .title {{ font-style: italic; }}
            .set, .set_index {{ font-size: 2.5px; font-weight: 200; }}
            rect, line {{ stroke: black; stroke-width: 0.2; }}
            </style>
            """
        )
        if grid:
            parts.append(
                f'<rect x="{hmargin_mm}" y="{vmargin_mm}" '
                f'width="{tw_mm}" height="{th_mm}" '
                'fill="transparent" stroke-linejoin="miter"/>'
            )
        for ix in range(0, self.width + 1):
            x_mm = hmargin_mm + ix * side_mm
            if grid and ix > 0 and ix <= self.width:
                parts.append(
                    f'<line x1="{x_mm}" y1="{vmargin_mm}" '
                    f'x2="{x_mm}" y2="{vmargin_mm + th_mm}" />'
                )
            if crop_marks:
                parts.append(
                    f'<line x1="{x_mm}" y1="{vmargin_mm - 5}" x2="{x_mm}" y2="{vmargin_mm - 1}" />'
                    f'<line x1="{x_mm}" y1="{vmargin_mm + th_mm + 1}" x2="{x_mm}" y2="{vmargin_mm + th_mm + 5}" />'
                )

        for iy in range(0, self.height + 1):
            y_mm = vmargin_mm + iy * side_mm
            if grid and iy > 0 and iy <= self.height:
                parts.append(
                    f'<line x1="{hmargin_mm}" y1="{y_mm}" '
                    f'x2="{hmargin_mm + tw_mm}" y2="{y_mm}" />'
                )
            if crop_marks:
                parts.append(
                    f'<line x1="{hmargin_mm - 5}" y1="{y_mm}" x2="{hmargin_mm - 1}" y2="{y_mm}" />'
                    f'<line x1="{hmargin_mm + tw_mm + 1}" y1="{y_mm}" x2="{hmargin_mm + tw_mm + 5}" y2="{y_mm}" />'
                )

        for i, track in enumerate(self.cells):
            if mode == "qr":
                # Note, we mirror over the x-axis, to match the titles codes
                # when printed double-sided.
                ix = self.width - 1 - (i % self.width)
                iy = i // self.width
                qr_path, qr_mm = track.qr_svg()
                # I'm lazy so we center the QR codes, we don't resize them. If the
                # urls get longer, then the QR codes will cover a larger area of the
                # cards.
                x_mm = hmargin_mm + ix * side_mm + (side_mm - qr_mm) / 2
                y_mm = vmargin_mm + iy * side_mm + (side_mm - qr_mm) / 2
                parts.append(f'<g transform="translate({x_mm}, {y_mm})">')
                parts.append(qr_path)
                parts.append("</g>")

            if mode == "title":
                ix = i % self.width
                iy = i // self.width
                x_mm = hmargin_mm + (ix + 0.5) * side_mm
                y_mm = vmargin_mm + (iy + 0.5) * side_mm
                half_side_mm = side_mm / 2
                parts.append(
                    f'<text x="{x_mm}" y="{y_mm + 6.5}" text-anchor="middle" '
                    f'class="year">{track.release_year}</text>'
                )
                for part in render_text_svg(x_mm, y_mm - 19, track.artist, "artist"):
                    parts.append(part)
                for part in render_text_svg(x_mm, y_mm + 18, track.title, "title"):
                    parts.append(part)
                parts.append(
                    f'<text x="{x_mm - half_side_mm + 3}" y="{y_mm + half_side_mm - 3}" '
                    f'text-anchor="start" class="set">{track.set}</text>'
                )
                parts.append(
                    f'<text x="{x_mm + half_side_mm - 3}" y="{y_mm + half_side_mm - 3}" '
                    f'text-anchor="end" class="set_index">{track.set_index}</text>'
                )

        parts.append(
            f'<text x="{w_mm - hmargin_mm}" y="{h_mm - hmargin_mm}" text-anchor="end" '
            f'class="footer">{html.escape(page_footer)}</text>'
        )

        parts.append("</svg>")

        return "\n".join(parts)


async def main() -> None:
    "Main method"

    arg_parser = argparse.ArgumentParser(
        prog="python -m make_qr_cards",
        description="Generate a PDF with QR codes for a list of music tracks.",
    )
    arg_parser.add_argument(
        "list_file", help="Name of file with track ID on each line."
    )
    arg_parser.add_argument(
        "track_db_file",
        help="Name of file with existing track metadata database. Created if it does not exist.",
    )
    arg_parser.add_argument(
        "-o", "--offset", dest="offset_count", help="Skip the first N tracks.", type=int
    )
    arg_parser.add_argument(
        "-l",
        "--limit",
        dest="limit_count",
        help="Limit the number of tracks.",
        type=int,
    )
    arg_parser.add_argument(
        "-s",
        "--set",
        dest="set_name",
        help="Only consider tracks from the given set.",
    )
    arg_parser.add_argument(
        "--set-alias",
        dest="set_alias",
        help="Name of the set alias to display on the cards.",
    )
    arg_parser.add_argument(
        "-f",
        "--font",
        dest="font_name",
        help="Font to use on the cards. Default is `Cantarell`.",
    )
    arg_parser.add_argument(
        "-g",
        "--grid",
        dest="grid",
        action="store_true",
        help="Draw a grid around the cards.",
    )
    arg_parser.add_argument(
        "-cm",
        "--crop-marks",
        dest="crop_marks",
        action="store_true",
        help="Draw crop marks at the sides of the page.",
    )
    arg_parser.add_argument(
        "--shuffle",
        dest="shuffle_cards",
        action="store_true",
        help="Outputs the cards onto pages in a random order so that they are already shuffled after cutting.",
    )
    arg_parser.add_argument(
        "--skip-pdf",
        dest="skip_pdf",
        action="store_true",
        help="Don't execute the `rsvg-convert` command to combine the SVG files into a PDF. "
        f"The command can be executed manually: {os.linesep}"
        f"`rsvg-convert --format=pdf --output=build/cards.pdf build/*.svg`{os.linesep}"
        "Useful when you want to generate SVGs on a Windows machine and only run `rsvg-convert` on Linux.",
    )
    arg_parser.add_argument(
        "--skip-if-set",
        dest="skip_if_set",
        help="Will skip the track if it is also present in the specified set. "
        "Comma-separated list of set IDs is supported. Useful for making boosters to your existing sets.",
    )
    arg_parser.add_argument(
        "--fuzzy-track-dupes",
        dest="fuzzy_track_dupes",
        action="store_true",
        help="Use fuzzy matching when detecting track presence in another set. "
        "Instead of matching just by the track ID, it will also try matching clean versions of title and artist.",
    )
    args = arg_parser.parse_args()

    list_file = Path(args.list_file)
    if not list_file.exists() or not list_file.is_file():
        print(f"File {list_file} does not exist or is not a file.")
        exit(1)

    track_db_file = Path(args.track_db_file)
    if not track_db_file.exists() or not track_db_file.is_file():
        print(f"File {track_db_file} does not exist or is not a file.")
        exit(1)

    offset_count = args.offset_count or 0
    limit_count = args.limit_count
    set_name = args.set_name
    set_alias = args.set_alias
    skip_pdf = args.skip_pdf
    skip_if_set = args.skip_if_set
    skip_if_set = set(skip_if_set.split(",")) if skip_if_set else set()
    fuzzy_track_dupes = args.fuzzy_track_dupes
    shuffle_cards = args.shuffle_cards

    font = args.font_name or "Cantarell"
    grid = args.grid
    crop_marks = args.crop_marks

    table = Table.new()
    tables: list[Table] = []
    tracks: list[Track] = []

    year_counts: Counter[int] = Counter()
    decade_counts: Counter[int] = Counter()

    track_metadata = (await load_track_db(track_db_file))["tracks"]

    async with await anyio.open_file(list_file, "r") as file:
        track_ids = [line.strip() for line in await file.readlines()]

    # Prepare for fuzzy matching
    exclude_track_data: dict[tuple[str, str], set[str]] = defaultdict(set)
    if fuzzy_track_dupes and len(skip_if_set) > 0:
        for track in track_metadata.values():
            if track["set"] not in skip_if_set:
                continue

            title = track.get("title_override", track["title_clean"])
            artist = track.get("artist_override", track["artist"])

            exclude_track_data[(title, artist)] |= skip_if_set & set(
                track["sets"].keys()
            )

    count = 0
    processed_count = 0
    for idx, track_id_def in enumerate(track_ids):
        if len(track_id_def.strip()) == 0 or track_id_def.startswith("#"):
            continue

        tokens = track_id_def.split(";")
        track_id = tokens[0]
        track_set = tokens[1] if len(tokens) > 1 else None
        track_index = int(tokens[2]) if len(tokens) > 2 else idx

        if set_name is not None and track_set != set_name:
            continue

        existing_track_data: dict[str, Any] | None = track_metadata.get(track_id)
        if existing_track_data is None:
            print(f"Track {track_id} not found in database.")
            exit(2)

        if len(skip_if_set) > 0:
            other_sets = [
                set_id
                for set_id in existing_track_data["sets"].keys()
                if set_id in skip_if_set
            ]
            if other_sets:
                print(
                    f"Skipping track {track_id} because it is also present in the set {other_sets}"
                )
                continue

        track = Track(
            release_date=date.fromisoformat(existing_track_data["release_date"]),
            title=existing_track_data.get(
                "title_override", existing_track_data["title_clean"]
            ),
            artist=existing_track_data.get(
                "artist_override", existing_track_data["artist"]
            ),
            album=existing_track_data.get(
                "album_override", existing_track_data["album_clean"]
            ),
            album_track=int(existing_track_data["album_track"]),
            track_url=existing_track_data["track_url"],
            album_url=existing_track_data["album_url"],
            artist_url=existing_track_data["artist_url"],
            set=set_alias or track_set or "",
            set_index=track_index,
        )

        conflicting_sets = exclude_track_data.get((track.title, track.artist))
        if conflicting_sets is not None:
            print(
                f"Skipping track {track_id} because it is also present in the set(s) {conflicting_sets}"
            )
            continue

        count += 1
        if offset_count >= count:
            continue

        tracks.append(track)
        processed_count += 1

        if limit_count is not None and processed_count >= limit_count:
            break

    if shuffle_cards:
        random.shuffle(tracks)
    else:
        tracks.sort()
    for track in tracks:
        table.append(track)
        year_counts[track.release_year] += 1
        decade_counts[10 * (track.release_year // 10)] += 1

        if table.is_full():
            tables.append(table)
            table = Table.new()

    # Append the final table, which may not be full.
    if not table.is_empty():
        tables.append(table)

    # Print statistics about how many tracks we have per year and per decade, so
    # you can tweak the track selection to make the distribution somewhat more
    # even.
    print("YEAR STATISTICS")
    for year, count in sorted(year_counts.items()):
        print(f"{year}: {count:2} {'#' * count}")

    print("\nDECADE STATISTICS")
    for decade, count in sorted(decade_counts.items()):
        print(f"{decade}s: {count:2} {'#' * count}")

    print("\nTOTAL")
    print(f"{sum(decade_counts.values())} tracks")

    os.makedirs("build", exist_ok=True)

    # For every table, write the two pages as svg.
    pdf_inputs: list[str] = []
    for i, table in enumerate(tables):
        p = i + 1
        pdf_inputs.append(f"build/{p:05d}a.svg")
        pdf_inputs.append(f"build/{p:05d}b.svg")
        with open(pdf_inputs[-2], "w", encoding="utf-8") as f:
            f.write(table.render_svg(font, grid, crop_marks, "title", f"{p}a"))
        with open(pdf_inputs[-1], "w", encoding="utf-8") as f:
            f.write(table.render_svg(font, grid, crop_marks, "qr", f"{p}b"))

    # Combine the svgs into a single pdf for easy printing.
    if not skip_pdf:
        cmd = ["rsvg-convert", "--format=pdf", "--output=build/cards.pdf", *pdf_inputs]
        subprocess.check_call(cmd)


if __name__ == "__main__":
    asyncio.run(main())
