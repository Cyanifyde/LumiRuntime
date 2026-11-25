from __future__ import annotations

from .checks import identity_key


def merge(previous, incoming):
    owner, source, created, stamp = (
        incoming[key] for key in ("owner", "source", "created", "stamp")
    )
    start, end, total = (incoming[key] for key in ("start", "end", "total"))
    entries = incoming["entries"]
    if min(owner, source, created, stamp) <= 0 or stamp < created * 1000 or not 0 <= total <= 1000:
        raise ValueError("invalid wishlist page")
    if (total and len(entries) != end - start + 1) or (not total and (entries or start or end)):
        raise ValueError("invalid wishlist page")
    if total and not 1 <= start <= end <= total:
        raise ValueError("invalid wishlist range")
    if any(identity_key(item) is None for item in entries) or len(
        {identity_key(item) for item in entries}
    ) != len(entries):
        raise ValueError("invalid wishlist names")
    epoch = [created, source]
    if previous and (
        epoch < previous["epoch"] or (epoch == previous["epoch"] and stamp <= previous["stamp"])
    ):
        return previous
    same = previous and previous["epoch"] == epoch and previous["total"] == total
    observed = dict(previous.get("pages", {})) if same else {}

    value = dict(end=end, entries=entries)
    if str(start) in observed and observed[str(start)] != value:
        observed.clear()
    observed[str(start)] = value
    active = {identity_key(item): item for item in (previous or {}).get("entries", [])}
    active.update((identity_key(item), item) for item in entries)
    positions, duplicate = {}, False
    for first, page in observed.items():
        for position, item in enumerate(page["entries"], int(first)):
            key = identity_key(item)
            duplicate |= position in positions and identity_key(positions[position]) != key
            positions[position] = item
    duplicate |= len({identity_key(item) for item in positions.values()}) != len(positions)
    complete = (
        not duplicate
        and stamp // 1000 - created <= 900_000
        and len(positions) == total
        and (not positions or (min(positions) == 1 and max(positions) == total))
    )
    if complete and incoming["verified"] and incoming["unfiltered"]:
        active = {identity_key(item): item for item in positions.values()}
    return dict(
        owner=owner,
        epoch=epoch,
        stamp=stamp,
        total=total,
        pages=observed,
        entries=list(active.values()),
        complete=bool(complete and incoming["verified"] and incoming["unfiltered"]),
    )
