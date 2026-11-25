from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
from decimal import Decimal, Inexact, Rounded, localcontext
from warnings import catch_warnings, simplefilter

from .models import CalculatorOfferSpec
from .syntax import InvalidCalculation, display, source_hash, translate

MAX_PENDING = 32
EXECUTION_SECONDS = 0.5
POOL_SIZE = 2
MAX_USES = 256
_GRAMMAR = None


def grammar():

    global _GRAMMAR
    if _GRAMMAR is None:
        import expr

        template = expr.create_state(max_safe_number=9e99)
        with catch_warnings():
            simplefilter("ignore")
            _GRAMMAR = template._build(), template._build_lexer()
    return _GRAMMAR


def evaluate(raw: str) -> CalculatorOfferSpec:

    import expr

    expression = translate(raw)
    with localcontext() as context:
        context.prec = 28
        context.Emax = 1_000
        context.Emin = -1_000
        context.clear_flags()
        state = expr.create_state(max_safe_number=9e99)
        state.__parser__, state.__lexer__ = grammar()
        value = state.evaluate(expression)
        if not isinstance(value, Decimal) or not value.is_finite() or abs(value) > Decimal("9e99"):
            raise InvalidCalculation()
        approximate = bool(context.flags[Inexact] or context.flags[Rounded])
        if value.is_zero():
            value = Decimal(0)
        text = (
            format(value, "f") if value.is_zero() or -12 <= value.adjusted() <= 30 else str(value)
        )
        if "E" not in text and "." in text:
            text = text.rstrip("0").rstrip(".")
        if len(text) > 160:
            raise InvalidCalculation()
        return CalculatorOfferSpec(source_hash(raw), display(raw), text, approximate)


class CalculatorEvaluator:


    def __init__(self) -> None:
        self._queue = asyncio.Queue(maxsize=MAX_PENDING)
        self._pending: set[asyncio.Future] = set()
        self._children: set[asyncio.subprocess.Process] = set()
        self._workers: list[asyncio.Task] = []
        self._startup = asyncio.Lock()
        self._closed = False

    async def start(self) -> None:
        async with self._startup:
            if self._workers:
                return
            self._closed = False
            ready = [asyncio.Event() for _ in range(POOL_SIZE)]
            self._workers = [
                asyncio.create_task(self._worker(event), name=f"calculator-evaluator-{index}")
                for index, event in enumerate(ready)
            ]
            await asyncio.gather(*(event.wait() for event in ready))

    async def close(self) -> None:
        async with self._startup:
            self._closed = True
            for worker in self._workers:
                worker.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers.clear()
            for child in tuple(self._children):
                await self._retire(child)
            for future in self._pending:
                if not future.done():
                    future.set_result(None)
            self._pending.clear()
            while not self._queue.empty():
                self._queue.get_nowait()
                self._queue.task_done()

    async def calculate(self, raw: str) -> CalculatorOfferSpec | None:
        if self._closed or len(self._pending) >= MAX_PENDING:
            return None
        try:
            translate(raw)
        except (InvalidCalculation, RecursionError):
            return None
        if not self._workers:
            await self.start()
        if self._closed or len(self._pending) >= MAX_PENDING:
            return None
        future = asyncio.get_running_loop().create_future()
        self._pending.add(future)
        try:
            self._queue.put_nowait((raw, future))
            return await asyncio.wait_for(future, timeout=4)
        except TimeoutError:
            return None
        finally:
            self._pending.discard(future)

    async def _spawn(self):
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "lumi.calculator.engine",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            limit=4_096,
            env={
                k: v
                for k, v in os.environ.items()
                if k
                in {
                    "PATH",
                    "PYTHONPATH",
                    "SYSTEMROOT",
                    "WINDIR",
                    "TEMP",
                    "TMP",
                    "PYTHONUTF8",
                    "PYTHONDONTWRITEBYTECODE",
                }
            },
        )
        self._children.add(child)
        try:
            async with asyncio.timeout(3):
                if await child.stdout.readline() != b"READY\n":
                    raise ValueError("evaluator not ready")
            return child
        except BaseException:
            await self._retire(child)
            raise

    async def _retire(self, child) -> None:
        if child.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                child.kill()
        await child.wait()
        self._children.discard(child)

    async def _worker(self, ready: asyncio.Event) -> None:
        import msgspec

        child, uses = None, 0
        try:
            while not self._closed:
                if child is None or child.returncode is not None or uses >= MAX_USES:
                    if child is not None:
                        await self._retire(child)
                        child = None
                    try:
                        child, uses = await self._spawn(), 0
                    except (OSError, ValueError, TimeoutError):
                        ready.set()
                        await asyncio.sleep(1)
                        continue
                    ready.set()
                try:
                    raw, future = await asyncio.wait_for(self._queue.get(), timeout=1)
                except TimeoutError:
                    continue
                try:
                    if future.done():
                        continue
                    result = None
                    try:
                        if child.returncode is not None:
                            await self._retire(child)
                            child, uses = await self._spawn(), 0
                        async with asyncio.timeout(EXECUTION_SECONDS):
                            child.stdin.write(msgspec.json.encode(raw) + b"\n")
                            await child.stdin.drain()
                            output = await child.stdout.readline()
                        if not output or len(output) > 4_096:
                            raise ValueError("invalid evaluator response")
                        result = msgspec.json.decode(output, type=CalculatorOfferSpec | None)
                        uses += 1
                    except (OSError, ValueError, TimeoutError):
                        await self._retire(child)
                        child = None
                    if not future.done():
                        future.set_result(result)
                finally:
                    self._queue.task_done()
        finally:
            ready.set()
            if child is not None:
                await self._retire(child)


def main() -> None:
    import expr
    import msgspec

    if sys.platform != "win32":
        import resource

        resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))
        signal.signal(signal.SIGALRM, lambda *_: sys.exit(1))
    grammar()
    sys.stdout.buffer.write(b"READY\n")
    sys.stdout.buffer.flush()
    for _ in range(MAX_USES):
        packet = sys.stdin.buffer.readline(4_097)
        if not packet or len(packet) > 4_096 or not packet.endswith(b"\n"):
            return
        if sys.platform != "win32":
            signal.setitimer(signal.ITIMER_REAL, EXECUTION_SECONDS)
        result = None
        try:
            raw = msgspec.json.decode(packet, type=str)
            result = evaluate(raw)
        except Exception:
            pass
        finally:
            if sys.platform != "win32":
                signal.setitimer(signal.ITIMER_REAL, 0)
        sys.stdout.buffer.write(msgspec.json.encode(result) + b"\n")
        sys.stdout.buffer.flush()
        del packet, result
        if "raw" in locals():
            del raw


if __name__ == "__main__":
    main()
