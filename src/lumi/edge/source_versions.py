from collections import OrderedDict
import secrets


class SourceVersions:
    def __init__(self, clock, *, limit=1024):
        self.clock, self.limit = clock, limit
        self.entries = OrderedDict()

    def watch(self, key, deadline):
        value = secrets.token_hex(16)
        self.entries[key] = (value, deadline)
        self.entries.move_to_end(key)
        while len(self.entries) > self.limit:
            self.entries.popitem(last=False)
        return value

    def version(self, key):
        value = self.entries.get(key)
        return value[0] if value and self.clock() < value[1] else None

    def clear(self):
        self.entries.clear()

    def gateway(self, event, payload):
        if event in {"READY", "RESUMED"}:
            self.clear()
        elif event in {"MESSAGE_UPDATE", "MESSAGE_DELETE", "MESSAGE_DELETE_BULK"}:
            channel = int(payload["channel_id"])
            ids = payload["ids"] if event == "MESSAGE_DELETE_BULK" else (payload["id"],)
            for identifier in ids:
                self.entries.pop((channel, int(identifier)), None)
