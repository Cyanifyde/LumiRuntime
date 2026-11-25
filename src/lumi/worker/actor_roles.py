from lumi.contracts.role_access import (
    READ_ACTOR_ROLES,
    WRITE_ACTOR_ROLES,
    ActorRoleChanges,
    actor_role_attempt,
)


class ActorRoles:
    def __init__(self, actions, *, job_id, guild_id, channel_id, message_id, actor_id):
        self.actions = actions
        self.job_id = job_id
        self.scope = dict(
            job_id=job_id, guild_id=guild_id, channel_id=channel_id,
            message_id=message_id, member_id=actor_id,
        )

    async def settled(self, step=0):
        return await self.actions.read_actor_role_receipt(actor_role_attempt(self.job_id, step))

    async def observe(self):
        facts = await self.actions.request(
            **self.scope, kind=READ_ACTOR_ROLES, idempotency_key=f"actor-roles:{self.job_id}",
        )
        if not facts.ok or facts.status != "observed":
            return None
        return facts.data["joined"], frozenset(facts.data["assigned"])

    async def apply(self, joined, required, changes, *, step=0):
        return await self.actions.request(
            **self.scope, kind=WRITE_ACTOR_ROLES,
            idempotency_key=actor_role_attempt(self.job_id, step),
            actor_role_changes=ActorRoleChanges(
                joined, tuple(dict.fromkeys(required)), tuple(changes),
            ),
        )
