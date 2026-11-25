from __future__ import annotations

from typing import Annotated

import msgspec

MODULE = "calculator"
EMOJI = "🧮"
TTL_MS = 900_000
MAX_CHARS = 512
MAX_TOKENS = 128
MAX_DEPTH = 16
CANDIDATE_PATTERN = "^[ \\t\u00a0`0-9.,+\\-\u2212*/\u00d7÷^()%kKmMbBtTeE\uff01-\uff5e]{1,512}$"
DATE_PATTERN = (
    r"(?:[0-9]{4}[/.-][0-9]{1,2}[/.-][0-9]{1,2}|[0-9]{1,2}[/.-][0-9]{1,2}[/.-][0-9]{2,4})"
)


class CalculatorOfferSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    source_hash: Annotated[str, msgspec.Meta(pattern="^[a-f0-9]{64}$")]
    expression: Annotated[str, msgspec.Meta(min_length=1, max_length=MAX_CHARS * 2)]
    value: Annotated[str, msgspec.Meta(min_length=1, max_length=160)]
    approximate: bool = False
