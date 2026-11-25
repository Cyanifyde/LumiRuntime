from __future__ import annotations

import asyncio
import hashlib
from collections import OrderedDict
from io import BytesIO

from PIL import Image, ImageDraw

from .models import COLOURS, GAMES, LABELS, date_label
from .renderer import BACKGROUND, BORDER, FOREGROUND, MUTED, SUPERSAMPLE, WIDTH, _draw_text, _font
from .statistics import METRICS

HEIGHT = 579
PAGE_SIZE = 10


def number(value, *, percent=False):
    if value is None:
        return "—"
    if percent:
        return f"{float(value) * 100:.1f}%"
    return f"{int(value):,}" if int(value) == value else f"{float(value):,.2f}"


def fit(text, width, size=12):
    text = " ".join(str(text).split())
    while text and _font(size).getlength(text) / SUPERSAMPLE > width:
        text = text[:-2].rstrip("…") + "…"
    return text


def edition_lines(editions, width):
    lines, line = [], ""
    for edition, count in editions:
        entry = f"E{edition} {count:,}"
        candidate = line + " · " + entry if line else entry
        if line and _font(11).getlength(candidate) / SUPERSAMPLE > width:
            lines.append(line)
            line = entry
        else:
            line = candidate
    return [*lines, line] if line else ["No editions"]


def most_common(counts):
    winner = min(
        counts.items(),
        key=lambda item: (-item[1], str(item[0]).casefold(), str(item[0])),
        default=None,
    )
    return (winner,) if winner is not None else ()


def drawing_data(report, view, game, target, metric, page, label):
    selected = GAMES if game == "all" else (game,)
    summaries = []
    for key in selected:
        s = report.games[key]
        summaries.append(
            dict(
                game=key,
                drops=s.drops,
                cards=s.cards,
                samples=s.samples,
                average=number(s.average),
                median=number(s.median),
                best=number(s.best),
                ones=s.numbers[1],
                low=number(s.low_rate, percent=True),
                buckets=s.buckets(),
                editions=tuple(sorted(s.editions.items())),
                average_edition=number(s.average_edition),
                edition_samples=sum(s.editions.values()),
                users=len(s.users),
                manual=s.manual,
                automatic=s.automatic,
                unknown=s.unknown,
                day=max(s.days, key=lambda d: (s.days[d], -d)) if s.days else None,
                hour=max(s.hours, key=lambda h: (s.hours[h], -h)) if s.hours else None,
                characters=most_common(s.characters),
                series=most_common(s.series),
                unique_characters=len(s.characters),
                unique_series=len(s.series),
            )
        )
    ranked = report.leaderboard(metric) if view == "lb" else []
    pages = max(1, (len(ranked) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, pages - 1)
    return dict(
        view=view,
        game=game,
        target=target,
        metric=metric,
        page=page,
        pages=pages,
        start=report.start_ms,
        end=report.end_ms,
        cutoff=report.cutoff_ms,
        days=report.observed_days,
        label=label or report.labels.get(target, str(target)),
        summaries=summaries,
        ranked=[
            (
                rank,
                report.labels.get(user, str(user)),
                number(value, percent=metric == "low_rate"),
                samples,
            )
            for rank, user, value, samples in ranked[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]
        ],
    )


def render_statistics(data):
    scale = SUPERSAMPLE
    width = (WIDTH - 40) / len(data["summaries"])
    edition_rows = next(
        (
            edition_lines(row["editions"], width - 20)
            for row in data["summaries"]
            if row["game"] == "karuta"
        ),
        [],
    )
    extra = max(0, len(edition_rows) - 1) * 16 if data["view"] != "lb" else 0
    height = HEIGHT + extra
    canvas = Image.new("RGB", (WIDTH * scale, height * scale), BACKGROUND)
    draw = ImageDraw.Draw(canvas)

    def text(x, y, value, size=12, fill=MUTED, anchor="ls", width=None):
        _draw_text(
            draw,
            (x, y),
            fit(value, width, size) if width else str(value),
            size=size,
            fill=fill,
            anchor=anchor,
        )

    title = (
        "Drop leaderboard"
        if data["view"] == "lb"
        else "Player drops"
        if data["view"] == "user"
        else "Server drops"
    )
    text(20, 38, title, 20, FOREGROUND)
    text(
        WIDTH - 20,
        37,
        f"{date_label(data['start'])}\u2013{date_label(data['end'] - 1)} · UTC",
        anchor="rs",
    )
    if data["view"] == "lb":
        text(
            20,
            66,
            ("All games" if data["game"] == "all" else LABELS[data["game"]])
            + " · "
            + METRICS[data["metric"]],
            14,
            FOREGROUND,
        )
        text(WIDTH - 20, 66, f"{data['page'] + 1}/{data['pages']}", anchor="rs")
        text(20, 101, "Rank")
        text(78, 101, "Player")
        text(WIDTH - 20, 101, "Result", anchor="rs")
        for index, (rank, label, value, _samples) in enumerate(data["ranked"]):
            y = 137 + index * 37
            text(20, y, str(rank), 14, FOREGROUND)
            text(78, y, label, 14, FOREGROUND, width=490)
            text(WIDTH - 20, y, value, 14, FOREGROUND, "rs")
        if not data["ranked"]:
            text(20, 140, "No recorded drops.", 14)
    else:
        if data["view"] == "user":
            text(20, 65, data["label"], 14, FOREGROUND, width=500)
        selected = data["summaries"]
        width = (WIDTH - 40) / len(selected)
        for column, row in enumerate(selected):
            x, available = 20 + column * width, width - 20

            def line(y, label, value, x=x, available=available):
                label_width = max(12, available - _font(12).getlength(str(value)) / scale - 10)
                text(x, y, label, width=label_width)
                text(x + available, y, value, fill=FOREGROUND, anchor="rs")

            text(x, 97, LABELS[row["game"]], 16, COLOURS[row["game"]])
            unit = "gen" if row["game"] == "sofi" else "print"
            line(124, "Drops / cards", f"{row['drops']:,} / {row['cards']:,}")
            line(148, f"Average {unit}", row["average"])
            line(170, f"Median {unit}", row["median"])
            line(192, f"Lowest {unit}", row["best"])
            line(214, "Number ones", number(row["ones"]))
            line(236, "Below 100", row["low"])
            largest = max(row["buckets"], default=0) or 1
            for index, (label, count) in enumerate(
                zip(
                    ("1\u20139", "10\u201399", "100\u2013999", "1,000+"),
                    row["buckets"],
                    strict=True,
                )
            ):
                y = 278 + index * 19
                text(x, y, label, size=11)
                bx = x + available * 0.38
                draw.rectangle(
                    (bx * scale, (y - 8) * scale, (bx + available * 0.35) * scale, (y - 2) * scale),
                    fill=BORDER,
                )
                if count:
                    draw.rectangle(
                        (
                            bx * scale,
                            (y - 8) * scale,
                            (bx + available * 0.35 * count / largest) * scale,
                            (y - 2) * scale,
                        ),
                        fill=COLOURS[row["game"]],
                    )
                text(x + available, y, f"{count:,}", size=11, fill=FOREGROUND, anchor="rs")
            if row["game"] == "karuta":
                line(363, "Average edition", row["average_edition"])
                for index, value in enumerate(edition_rows):
                    text(x, 383 + index * 16, value, size=11)
            peak = (
                (date_label(row["day"]) if row["day"] is not None else "—")
                + " / "
                + (f"{row['hour']:02d}:00 UTC" if row["hour"] is not None else "—")
            )
            line(423 + extra, "Peak", peak)
            text(x, 446 + extra, "Top character", size=11)
            names = " · ".join(f"{name} ({count})" for (name, _series), count in row["characters"])
            text(x, 468 + extra, names or "No identified characters", size=11, width=available)
            text(x, 489 + extra, "Top series", size=11)
            series = " · ".join(f"{name} ({count})" for name, count in row["series"])
            text(x, 510 + extra, series or "No identified series", size=11, width=available)
            line(531 + extra, "Drops / day", number(row["drops"] / data["days"]))
    canvas = canvas.resize((WIDTH, height), Image.Resampling.LANCZOS)
    output = BytesIO()
    canvas.save(output, "PNG", compress_level=3)
    return output.getvalue()


class StatsRenderer:
    def __init__(self, lock=None):
        self.lock = lock or asyncio.Lock()
        self.cache = OrderedDict()

    async def render(self, report, **selection):
        data = drawing_data(report, **selection)
        key = hashlib.sha256(repr(data).encode()).digest()
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            task = asyncio.create_task(asyncio.to_thread(render_statistics, data))
            try:
                png = await asyncio.shield(task)
            except asyncio.CancelledError:
                await asyncio.gather(task, return_exceptions=True)
                raise
            with Image.open(BytesIO(png)) as image:
                if (
                    image.format != "PNG"
                    or image.width != WIDTH
                    or not HEIGHT <= image.height <= 2600
                    or len(png) > 2 * 1024 * 1024
                ):
                    raise OSError("Invalid statistics image.")
                image.verify()
            self.cache[key] = png
            while len(self.cache) > 32:
                self.cache.popitem(last=False)
            return png

    async def close(self):
        async with self.lock:
            self.cache.clear()
