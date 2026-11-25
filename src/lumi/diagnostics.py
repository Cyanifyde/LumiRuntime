from __future__ import annotations

import json
import logging
import math
import re
import sys
import threading
from importlib.resources import files

EVENTS = frozenset(json.loads(files("lumi").joinpath("diagnostic_events.json").read_text()))
LABEL = re.compile(r"([a-z][a-z_]{1,79})(?:\s|$)")
ERRORS = frozenset("""
ActionUnavailable AssertionError AttributeError CancelledError ClientError ConnectionError DataError
DatabaseError FileNotFoundError Forbidden HTTPException IndexError IntegrityError KeyError LockError
MySQLError NoPermissionError NotFound OSError OperationalError OverflowError PermissionError
RedisError ResponseError RuntimeError TimeoutError TypeError UnicodeError
UnicodeDecodeError UnicodeEncodeError ValueError
""".split())
METRICS = frozenset({"duration_ms", "elapsed_ms", "attempt", "retry", "count"})
CODE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.]{0,127}")
PRIVATE_LOGGERS = ("lumi.privacy", "lumi.edge.privacy")
SQL_WARNING_PREFIXES = (
    ("Duplicate entry ", "duplicate_row"),
    ("Data truncated ", "truncated_value"),
    ("Out of range value ", "out_of_range"),
    ("Truncated incorrect ", "invalid_cast"),
)
_SANITIZED = object()


def _operation():

    operation = None
    frame = sys._getframe(1)
    while frame is not None:
        module = frame.f_globals.get("__name__", "")
        if module.startswith(PRIVATE_LOGGERS):
            return False
        if (operation is None and module.startswith("lumi.")
                and module not in {"lumi.diagnostics", "lumi.storage.checkpoint"}):
            name = frame.f_code.co_qualname
            if len(name) > 128 or not CODE_NAME.fullmatch(name):
                name = frame.f_code.co_name
            if CODE_NAME.fullmatch(module) and CODE_NAME.fullmatch(name):
                operation = module + "." + name
        frame = frame.f_back
    return operation


def safe_record(record):

    label = LABEL.match(record.msg) if isinstance(record.msg, str) else None
    event = label[1] if label and label[1] in EVENTS else "runtime_diagnostic"
    if event == "runtime_diagnostic" and record.levelno < logging.WARNING:
        return None
    result = {"schema": 1, "event": event, "level": record.levelname.lower()}
    if record.levelno >= logging.WARNING:
        operation = _operation()
        if operation is False:
            return None
        if operation:
            result["operation"] = operation
        if record.name == "asyncmy":
            result["event"] = "database_driver_warning"
            msg = record.msg if isinstance(record.msg, str) else ""
            result["category"] = next(
                (name for prefix, name in SQL_WARNING_PREFIXES if msg.startswith(prefix)), "other"
            )
    if record.exc_info and getattr(record.exc_info[0], "__name__", "") in ERRORS:
        result["error"] = record.exc_info[0].__name__
    for key in METRICS:
        value = record.__dict__.get(key)

        if type(value) in {int, float} and math.isfinite(value) and 0 <= value < 2**31:
            result[key] = value
    return result


class DiagnosticFilter(logging.Filter):
    def filter(self, record):


        if record.name.startswith(PRIVATE_LOGGERS):
            return False
        if getattr(record, "_lumi_safe_diagnostic", None) is _SANITIZED:
            return True
        safe = safe_record(record)
        if safe is None:
            return False
        clean = logging.LogRecord("runtime", record.levelno, "runtime", 0,
                                  json.dumps(safe, separators=(",", ":")), (), None)

        record.__dict__.clear()
        record.__dict__.update(clean.__dict__)
        record._lumi_safe_diagnostic = _SANITIZED
        return True


class DiagnosticFormatter(logging.Formatter):
    def format(self, record):
        if getattr(record, "_lumi_safe_diagnostic", None) is not _SANITIZED:
            if not DiagnosticFilter().filter(record):
                return ""
        return record.msg


def configure_diagnostics(*, level=logging.INFO):
    logging.basicConfig(level=level)
    handlers = set(logging.getLogger().handlers)
    for logger in logging.Logger.manager.loggerDict.values():
        if isinstance(logger, logging.Logger):
            handlers.update(logger.handlers)
    if logging.lastResort:
        handlers.add(logging.lastResort)
    for handler in handlers:
        if not any(isinstance(f, DiagnosticFilter) for f in handler.filters):
            handler.addFilter(DiagnosticFilter())
        handler.setFormatter(DiagnosticFormatter())
    logging.captureWarnings(True)
    sys.excepthook = lambda kind, _value, _trace: logging.getLogger("runtime").critical(
        "runtime_unhandled_failure", exc_info=(kind, None, None))
    sys.unraisablehook = lambda _args: logging.getLogger("runtime").error(
        "runtime_unraisable_failure")
    threading.excepthook = lambda args: logging.getLogger("runtime").critical(
        "runtime_thread_failure", exc_info=(args.exc_type, None, None))
