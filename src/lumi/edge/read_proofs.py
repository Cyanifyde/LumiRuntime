import hmac
import re
import secrets

from lumi.contracts import encode


def message_author_scope(channel_id, message_id):


    return "message_author", channel_id, message_id


class ReadProofs:
    def __init__(self, clock):
        self.clock = clock
        self.key = secrets.token_bytes(32)

    def _mac(self, action, scope, expires):
        return hmac.digest(self.key, encode((
            action.job_id, action.module, action.group, action.generation, action.guild_id, scope, expires,
        )), "sha256").hex()

    def issue(self, action, scope, *, deadline=None):
        return self.issue_window(action, scope, deadline=deadline)[0]

    def issue_window(self, action, scope, *, deadline=None):

        expires = min(action.expires_at_ms, self.clock() + 30_000)
        if deadline is not None:
            expires = min(expires, deadline)
        return f"{expires}:{self._mac(action, scope, expires)}", expires

    def valid_until(self, action, scope):
        return self.with_proof(action, action.read_proof, scope)

    def with_proof(self, action, proof, scope):
        match = re.fullmatch(r"([0-9]{1,16}):([a-f0-9]{64})", proof or "")
        if match and self.clock() < int(match[1]) and hmac.compare_digest(
            match[2], self._mac(action, scope, int(match[1])),
        ):
            return int(match[1])
        return 0
