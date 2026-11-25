from lumi.contracts.native_mutations import (
    DeleteMessages,
    decode_attempt,
    mutation_kind,
    nickname_change,
)

from .models import TTL


def scope(attempt):
    return (
        mutation_kind(attempt), attempt.guild, attempt.operation, attempt.actor,
        attempt.channel if isinstance(attempt, DeleteMessages) else 0,
    )


def observed_attempt(obs):
    record = decode_attempt(obs.data)
    attempt = record.attempt
    target, channel = (
        (attempt.messages[0], attempt.channel) if isinstance(attempt, DeleteMessages)
        else (attempt.member, 0)
    )
    if (obs.guild, obs.actor, obs.resource, obs.channel) != (
        attempt.guild, attempt.actor, target, channel,
    ) or obs.order <= 0:
        raise ValueError("Mismatched native attempt.")
    return record


def suppressions(obs, attempt, client_id):
    if isinstance(attempt, DeleteMessages):
        common = dict(
            guild=attempt.guild, channel=attempt.channel, created=obs.occurred,
            expires=obs.occurred + TTL, capture_order=obs.order,
        )
        return (
            dict(common, id="batch:" + attempt.operation, actor=client_id),
            *(dict(common, id=str(mid), batch=attempt.operation) for mid in attempt.messages),
        )
    return (dict(
        id="nick:" + str(attempt.member), guild=attempt.guild, actor=client_id,
        subject=attempt.member,
        change=attempt.change, operation=attempt.operation,
        created=obs.occurred, match_until=obs.occurred + 60000,
        expires=obs.occurred + TTL, capture_order=obs.order,
    ),)


def matches(suppression, obs):
    if not suppression or not suppression["created"] <= obs.occurred < suppression.get(
        "match_until", suppression["expires"],
    ):
        return False
    return not (obs.order and obs.order <= suppression.get("capture_order", 0))


def matches_nickname(suppression, obs, before, after):
    return matches(suppression, obs) and (
        suppression["change"] == nickname_change(before, after) if "change" in suppression
        else before == suppression["before"] and after == suppression["after"]
    )
