from __future__ import annotations

from functools import wraps


def tracked_read(method):


    @wraps(method)
    async def guarded(self, *args, **kwargs):
        gate = getattr(self, "ingress_gate", None)
        if gate is None:
            return await method(self, *args, **kwargs)
        while True:
            async with gate.track(0, internal=True) as admitted:
                if admitted:
                    return await method(self, *args, **kwargs)
            await gate.wait_open()

    return guarded
