import re

from .models import PresentationBlock

WRITE_LAYOUT_MESSAGE = "write_layout_message"
ATTEMPT = re.compile(r"layout-write:[a-f0-9]{32}")
MAX_LAYOUT_COMPONENTS = 40


def layout_receipt(group: str, module: str, attempt: str) -> str:
    return f"lumi:edge-action-receipt:{group}:{module}:{attempt}"


def ordered_component_count(blocks: tuple[PresentationBlock, ...]) -> int:

    return 1 + sum(
        3 if block.kind == "section"
        else 1 + len(block.controls) if block.kind == "action_row"
        else 1
        for block in blocks
    )
