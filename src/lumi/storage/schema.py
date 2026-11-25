from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import weakref
from pathlib import Path

_CONTEXTS = weakref.WeakKeyDictionary()
_NAME = re.compile(r"[a-z][a-z0-9_]*\Z")


class SchemaPolicyError(RuntimeError):
    pass


def fingerprint(create_statement):

    lines = create_statement.splitlines()
    if lines and lines[-1].startswith(") ENGINE="):
        lines[-1] = re.sub(r" AUTO_INCREMENT=\d+(?= |$)", "", lines[-1])
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def load_policy(path, service):
    try:
        info = path.lstat()
        if path.is_symlink() or not path.is_file() or info.st_size > 1024 * 1024:
            raise ValueError
        document = json.loads(path.read_bytes())
        if (
            set(document) != {"schema", "service", "revision", "tables"}
            or document["schema"] != 1
            or document["service"] != service
            or not isinstance(document["revision"], str)
            or not document["revision"]
            or not isinstance(document["tables"], dict)
            or not document["tables"]
        ):
            raise ValueError
        for table, digest in document["tables"].items():
            if (
                not _NAME.fullmatch(table)
                or not isinstance(digest, str)
                or not re.fullmatch(r"[a-f0-9]{64}", digest)
            ):
                raise ValueError
        return document
    except (OSError, ValueError, TypeError, KeyError):
        raise SchemaPolicyError("Database schema policy unavailable.") from None


async def validate(pool, policy):
    async with pool.acquire() as connection, connection.cursor() as query:
        for table, expected in policy["tables"].items():
            await query.execute("SHOW CREATE TABLE `" + table + "`")
            row = await query.fetchone()
            if not row or fingerprint(row[1]) != expected:
                raise SchemaPolicyError("Database schema is not compatible with this release.")


async def startup_migrate(store):

    path = os.getenv("LUMI_DB_SCHEMA_POLICY_FILE")
    if not path:
        if os.getenv("LUMI_REQUIRE_SCHEMA_POLICY") == "1":
            raise SchemaPolicyError("Database schema policy required.")
        await store.migrate()
        return False
    service = os.getenv("LUMI_SERVICE", os.getenv("LUMI_GROUP", "edge"))
    policy = load_policy(Path(path), service)
    settings = getattr(store, "_settings", None) or getattr(store, "settings", None)
    if not isinstance(settings, dict):

        owner = next(
            (
                getattr(store, name)
                for name in ("owner", "catalogue", "_store", "counts", "store")
                if getattr(store, name, None) is not None
            ),
            None,
        )
        if owner is not None:
            return await startup_migrate(owner)
        raise SchemaPolicyError("Database connection ownership is not classified.")
    key = (
        tuple(str(settings.get(name, "")) for name in ("host", "port", "user", "db")),
        hashlib.sha256(json.dumps(policy, sort_keys=True).encode()).digest(),
    )
    loop = asyncio.get_running_loop()
    lock, validated = _CONTEXTS.setdefault(loop, (asyncio.Lock(), set()))
    async with lock:
        if key not in validated:
            pool = getattr(store, "_pool", None) or getattr(store, "pool", None)
            if pool is None:
                raise SchemaPolicyError("Database is not connected.")
            await validate(pool, policy)
            validated.add(key)
    return True
