from __future__ import annotations

import asyncio
import contextlib
import json
import re
import secrets
import time
import unicodedata
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from pathlib import Path
from types import MappingProxyType

import aiohttp
from redis.asyncio import Redis
from redis.exceptions import RedisError

from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    PresentationPage,
    ResultEnvelope,
    TypedRequest,
)

API_URL = "https://api.frankfurter.dev/v2/rate"
CACHE_PREFIX = "lumi:currency:rate:v1:"
FRESH_SECONDS = 60 * 60
MAX_STALE_SECONDS = 72 * 60 * 60
REQUEST_TIMEOUT_SECONDS = 5
MAX_RESPONSE_BYTES = 16 * 1024
MAX_QUERY_CHARS = 240
MAX_PENDING_PAIRS = 32
MAX_MEMORY_PAIRS = 512
_UNLOCK = "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end"
_AMOUNT = re.compile(r"[+-]?(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(?:\.[0-9]+)?")
_FAMILIES = (
    "dollar",
    "pound",
    "peso",
    "franc",
    "rupee",
    "dinar",
    "dirham",
    "rial",
    "riyal",
    "shilling",
    "kwacha",
    "won",
    "manat",
    "guilder",
    "krona",
    "krone",
    "leu",
    "bolívar",
)


class CurrencyInputError(ValueError):
    pass


class RatesUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Currency:
    code: str
    name: str
    minor_units: int
    supported: bool


def _name(value: str) -> str:
    value = unicodedata.normalize("NFKD", unicodedata.normalize("NFKC", value)).casefold()
    value = "".join(c for c in value if not unicodedata.combining(c))
    return " ".join(value.replace(".", "").split())


def _plural(value: str) -> str:
    if value.endswith(("yen", "yuan", "won", "baht", "rand")):
        return value
    if value.endswith("y"):
        return value[:-1] + "ies"
    return value + "s"


def _catalogue() -> tuple[dict[str, Currency], dict[str, frozenset[str]]]:
    data = json.loads(Path(__file__).with_name("currency_data.json").read_text("utf-8"))
    currencies: dict[str, Currency] = {}
    aliases: dict[str, set[str]] = {}
    for row in data["currencies"]:
        code = row["code"]
        currencies[code] = Currency(code, row["name"], row["minor_units"], row["supported"])
        names = [row["name"], *row["names"]]
        names.extend(
            family
            for family in _FAMILIES
            if any(re.search(r"\b" + re.escape(family) + r"\b", name, re.I) for name in names)
        )
        for name in names:
            for variant in (name, _plural(name)):
                aliases.setdefault(_name(variant), set()).add(code)
        for symbol in row["symbols"]:
            aliases.setdefault(_name(symbol), set()).add(code)
    return currencies, {name: frozenset(codes) for name, codes in aliases.items()}


_currencies, _aliases = _catalogue()
CURRENCIES = MappingProxyType(_currencies)
ALIASES = MappingProxyType(_aliases)


def resolve_currency(value: str) -> Currency:
    code = value.strip().upper()
    if code in CURRENCIES:
        result = CURRENCIES[code]
    else:
        candidates = ALIASES.get(_name(value), frozenset())
        if len(candidates) > 1:
            raise CurrencyInputError("Use a currency code.")
        if not candidates:
            raise CurrencyInputError("Unknown currency.")
        result = CURRENCIES[next(iter(candidates))]
    if not result.supported:
        raise CurrencyInputError("Unsupported currency.")
    return result


@dataclass(frozen=True, slots=True)
class Conversion:
    amount: Decimal
    base: Currency
    quote: Currency


def parse_conversion(value: str) -> Conversion:
    if len(value) > MAX_QUERY_CHARS:
        raise CurrencyInputError()
    value = unicodedata.normalize("NFKC", value).strip()

    prefix_symbol = None
    match = _AMOUNT.match(value)
    if match is None:
        prefixed = re.match(r"([^\d\s+\-]{1,8})(?=[+\-]?[0-9])", value)
        if prefixed is None:
            raise CurrencyInputError()
        prefix_symbol = prefixed[1]
        value = value[prefixed.end() :]
        match = _AMOUNT.match(value)
        if match is None:
            raise CurrencyInputError()
    raw_amount = match[0]

    integer, _, fraction = raw_amount.lstrip("+-").replace(",", "").partition(".")
    if len(integer) > 18 or len(fraction) > 12:
        raise CurrencyInputError("Amount too large or precise.")
    remainder = value[match.end() :]
    if not remainder or (not remainder[0].isspace() and remainder[0].isalnum()):
        raise CurrencyInputError()
    remainder = remainder.strip()
    if prefix_symbol is not None:
        remainder = prefix_symbol + " to " + re.sub(r"^to\s+", "", remainder, flags=re.I)
    parts = re.split(r"\s+to\s+", remainder, flags=re.I)
    if len(parts) == 1:
        parts = remainder.split()
        if len(parts) != 2:
            raise CurrencyInputError()
    if len(parts) != 2 or not all(part.strip() for part in parts):
        raise CurrencyInputError()

    if any(character in remainder for character in ("\n", "\r", "\x00")):
        raise CurrencyInputError()
    return Conversion(
        Decimal(raw_amount.replace(",", "")),
        resolve_currency(parts[0]),
        resolve_currency(parts[1]),
    )


@dataclass(frozen=True, slots=True)
class Rate:
    base: str
    quote: str
    value: Decimal
    date: str
    fetched_at: float
    cached: bool = False


def _decimal_rate(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise RatesUnavailableError()
    value = Decimal(value)
    if not value.is_finite() or not Decimal("1e-18") <= value <= Decimal("1e18"):
        raise RatesUnavailableError()
    if len(value.as_tuple().digits) > 40:
        raise RatesUnavailableError()
    return value


def _rate_date(value: object, now: float) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise RatesUnavailableError()
    try:
        day = date.fromisoformat(value)
    except ValueError:
        raise RatesUnavailableError() from None

    if day > datetime.fromtimestamp(now, UTC).date() + timedelta(days=1):
        raise RatesUnavailableError()
    return value


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise RatesUnavailableError()
        result[key] = value
    return result


def _json(raw: bytes | str) -> object:
    try:
        return json.loads(raw, parse_float=Decimal, object_pairs_hook=_object)
    except (ValueError, UnicodeError, RecursionError):
        raise RatesUnavailableError() from None


def parse_rate(payload: object, base: str, quote: str, now: float) -> Rate:
    if (
        not isinstance(payload, dict)
        or payload.get("base") != base
        or payload.get("quote") != quote
    ):
        raise RatesUnavailableError()
    return Rate(
        base, quote, _decimal_rate(payload.get("rate")), _rate_date(payload.get("date"), now), now
    )


def _cache_rate(raw: bytes | str, base: str, quote: str, now: float) -> Rate:
    if len(raw) > MAX_RESPONSE_BYTES:
        raise RatesUnavailableError()
    payload = _json(raw)
    if not isinstance(payload, dict) or type(payload.get("schema")) is not int:
        raise RatesUnavailableError()
    if payload["schema"] != 1 or type(payload.get("fetched_at_ms")) is not int:
        raise RatesUnavailableError()
    if not 0 <= payload["fetched_at_ms"] <= int(now * 1_000):
        raise RatesUnavailableError()
    fetched_at = payload["fetched_at_ms"] / 1_000
    if not 0 <= now - fetched_at <= MAX_STALE_SECONDS:
        raise RatesUnavailableError()
    try:
        if not isinstance(payload.get("rate"), str) or len(payload["rate"]) > 64:
            raise RatesUnavailableError()
        payload["rate"] = Decimal(payload["rate"])
    except InvalidOperation:
        raise RatesUnavailableError() from None
    return replace(parse_rate(payload, base, quote, now), fetched_at=fetched_at)


class CurrencyRatesService:
    def __init__(
        self,
        cache_redis: Redis,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._redis = cache_redis
        self._clock = clock
        self._session: aiohttp.ClientSession | None = None
        self._memory: OrderedDict[tuple[str, str], Rate] = OrderedDict()
        self._inflight: dict[tuple[str, str], asyncio.Task[Rate]] = {}
        self._gate = asyncio.Semaphore(4)

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS, connect=2),
                connector=aiohttp.TCPConnector(limit=4, ttl_dns_cache=300),
                headers={"Accept": "application/json"},
            )

    async def close(self) -> None:
        tasks = tuple(self._inflight.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._inflight.clear()
        self._memory.clear()
        session, self._session = self._session, None
        if session is not None:
            await session.close()

    def _remember(self, rate: Rate) -> None:
        pair = (rate.base, rate.quote)
        self._memory[pair] = rate
        self._memory.move_to_end(pair)
        while len(self._memory) > MAX_MEMORY_PAIRS:
            self._memory.popitem(last=False)

    async def _read(self, base: str, quote: str) -> Rate | None:
        rate = self._memory.get((base, quote))
        if rate is not None and not 0 <= self._clock() - rate.fetched_at <= MAX_STALE_SECONDS:
            self._memory.pop((base, quote), None)
            rate = None
        if rate is not None and self._clock() - rate.fetched_at < FRESH_SECONDS:
            self._memory.move_to_end((base, quote))
            return rate
        try:
            async with asyncio.timeout(0.5):
                raw = await self._redis.get(CACHE_PREFIX + base + ":" + quote)
            if raw is not None:
                stored = _cache_rate(raw, base, quote, self._clock())
                if rate is None or stored.fetched_at >= rate.fetched_at:
                    rate = stored
                    self._remember(stored)
        except (RedisError, OSError, TimeoutError, RatesUnavailableError):
            pass
        return rate

    async def quote(self, base: str, quote: str) -> Rate:
        if any(code not in CURRENCIES or not CURRENCIES[code].supported for code in (base, quote)):
            raise RatesUnavailableError()
        old = await self._read(base, quote)
        if old is not None and self._clock() - old.fetched_at < FRESH_SECONDS:
            return old
        pair = (base, quote)
        task = self._inflight.get(pair)
        if task is None:
            if len(self._inflight) >= MAX_PENDING_PAIRS:
                return self._fallback(old)
            task = asyncio.create_task(self._refresh(base, quote, old))
            self._inflight[pair] = task

            def finished(done: asyncio.Task[Rate]) -> None:
                self._inflight.pop(pair, None)
                if not done.cancelled():
                    done.exception()

            task.add_done_callback(finished)
        return await asyncio.shield(task)

    def _fallback(self, rate: Rate | None) -> Rate:
        if rate is not None and 0 <= self._clock() - rate.fetched_at <= MAX_STALE_SECONDS:
            return replace(rate, cached=True)
        raise RatesUnavailableError()

    async def _claim(self, key: str, token: str) -> bool | None:
        try:
            async with asyncio.timeout(0.5):
                return bool(await self._redis.set(key, token, nx=True, ex=8))
        except (RedisError, OSError, TimeoutError):
            return None

    async def _refresh(self, base: str, quote: str, old: Rate | None) -> Rate:
        key = CACHE_PREFIX + base + ":" + quote
        lock_key, token = key + ":lock", secrets.token_hex(16)
        claimed = await self._claim(lock_key, token)
        try:
            if claimed is False:
                async with asyncio.timeout(REQUEST_TIMEOUT_SECONDS):
                    while claimed is False:
                        newer = await self._read(base, quote)
                        if newer is not None and self._clock() - newer.fetched_at < FRESH_SECONDS:
                            return newer
                        await asyncio.sleep(0.1)
                        claimed = await self._claim(lock_key, token)

            if claimed:
                newer = await self._read(base, quote)
                if newer is not None and self._clock() - newer.fetched_at < FRESH_SECONDS:
                    return newer
            rate = await self._fetch(base, quote)
            self._remember(rate)
            payload = json.dumps(
                {
                    "schema": 1,
                    "base": base,
                    "quote": quote,
                    "rate": str(rate.value),
                    "date": rate.date,
                    "fetched_at_ms": int(rate.fetched_at * 1_000),
                }
            )
            with contextlib.suppress(RedisError, OSError, TimeoutError):
                async with asyncio.timeout(0.5):
                    await self._redis.set(key, payload, ex=MAX_STALE_SECONDS + 1)
            return rate
        except (RatesUnavailableError, TimeoutError):
            return self._fallback(old)
        finally:
            if claimed:
                with contextlib.suppress(RedisError, OSError, TimeoutError):
                    async with asyncio.timeout(0.5):
                        await self._redis.eval(_UNLOCK, 1, lock_key, token)

    async def _fetch(self, base: str, quote: str) -> Rate:
        if self._session is None or self._session.closed:
            raise RatesUnavailableError()
        try:
            async with asyncio.timeout(REQUEST_TIMEOUT_SECONDS), self._gate:
                async with self._session.get(
                    API_URL + "/" + base.lower() + "/" + quote.lower(),
                    allow_redirects=False,
                ) as response:
                    if response.status != 200 or response.content_type != "application/json":
                        raise RatesUnavailableError()
                    if (
                        response.content_length is not None
                        and response.content_length > MAX_RESPONSE_BYTES
                    ):
                        raise RatesUnavailableError()
                    chunks = bytearray()
                    async for chunk in response.content.iter_chunked(4_096):
                        chunks.extend(chunk)
                        if len(chunks) > MAX_RESPONSE_BYTES:
                            raise RatesUnavailableError()
                    return parse_rate(_json(bytes(chunks)), base, quote, self._clock())
        except (aiohttp.ClientError, OSError, TimeoutError):
            raise RatesUnavailableError() from None


def _amount_text(amount: Decimal) -> str:
    text = format(amount, ",f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if amount.is_zero() else text


def conversion_page(conversion: Conversion, rate: Rate | None) -> PresentationPage:
    source = f"{_amount_text(conversion.amount)} {conversion.base.code}"
    if conversion.base.code == conversion.quote.code:
        return PresentationPage(description=f"{source} = {source}")
    if rate is None:
        raise RatesUnavailableError()
    with localcontext() as context:
        context.prec = 80
        value = conversion.amount * rate.value
        rounded = value.quantize(
            Decimal(1).scaleb(-conversion.quote.minor_units),
            rounding=ROUND_HALF_UP,
        )
    if rounded.is_zero():
        rounded = abs(rounded)
    result = f"{rounded:,.{conversion.quote.minor_units}f} {conversion.quote.code}"
    return PresentationPage(description=f"{source} ≈ {result}", footer="Frankfurter")


class CurrencyModule:
    def __init__(
        self,
        *,
        generation: int,
        cache_redis: Redis,
        group: str = "tools",
        service: CurrencyRatesService | None = None,
    ) -> None:
        self._generation = generation
        self._group = group
        self._service = service or CurrencyRatesService(cache_redis)

    @property
    def manifest(self) -> Manifest:
        return Manifest(
            schema=2,
            module="currency",
            group=self._group,
            generation=self._generation,
            job_stream=f"lumi:jobs:{self._group}",
            display_name="Currency",
            summary="Currency conversion.",
            presentation_mode="v2",
            version="1.0.1",
            commands=(
                CommandSpec(
                    root="currency",
                    aliases=("convert",),
                    handler="currency.convert",
                    grammar=r"^(?:\s+(?P<query>[\s\S]+))?$",
                    captures=(CaptureSpec("query", "text", required=False, maximum=1_900),),
                    help=HelpSpec(
                        ".currency <amount> <from> [to] <to>",
                        "Convert money using daily reference rates.",
                    ),
                    acl_key="currency",
                    deadline_ms=15_000,
                    public=True,
                    context_fields=("prefix",),
                ),
            ),
        )

    async def start(self) -> None:
        await self._service.start()

    async def close(self) -> None:
        await self._service.close()

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        del generation
        query = str(request.arguments.get("query") or "").strip()
        prefix = str(request.event.get("prefix") or ".")
        usage = f"{prefix}currency <amount> <from> [to] <to>"
        error_code = None
        try:
            if not query or query.casefold() == "help":
                page = PresentationPage(description=usage)
            else:
                conversion = parse_conversion(query)
                rate = None
                if conversion.base != conversion.quote:
                    remaining = 12.0
                    if deadline := request.event.get("_expires_at_ms"):
                        remaining = min(remaining, (int(deadline) / 1_000) - time.time())
                    if remaining <= 0:
                        raise RatesUnavailableError()
                    async with asyncio.timeout(remaining):
                        rate = await self._service.quote(
                            conversion.base.code,
                            conversion.quote.code,
                        )
                page = conversion_page(conversion, rate)
        except CurrencyInputError as exc:
            page = PresentationPage(description=str(exc) or usage)
        except Exception:
            page = PresentationPage(description="Rates unavailable.")
            error_code = "rates_unavailable"
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="currency",
            group=self._group,
            generation=self._generation,
            status="error" if error_code else "ok",
            intents=(DiscordIntent(kind="send", pages=(page,)),),
            error_code=error_code,
            completed_at_ms=int(time.time() * 1_000),
        )
