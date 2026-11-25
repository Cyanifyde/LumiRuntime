from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import wraps
from types import MappingProxyType

from lumi.shared.request_scope import memo


def policy_decision(method):
    @wraps(method)
    def wrapped(self, **kwargs):
        key = (self, self._epoch, tuple(sorted(kwargs.items())))
        return memo("policy", key, lambda: method(self, **kwargs))
    return wrapped


@dataclass(frozen=True, slots=True)
class GuildPolicy:
    prefix: str
    enabled_modules: frozenset[str]
    enabled_commands: frozenset[str]
    module_roles: Mapping[str, frozenset[int]]
    command_roles: Mapping[str, frozenset[int]]
    module_channels: Mapping[str, tuple[frozenset[int], frozenset[int]]]
    command_channels: Mapping[str, tuple[frozenset[int], frozenset[int]]]


class PolicyCache:


    def __init__(self, default_prefix: str) -> None:
        self.default_prefix = default_prefix
        self._guilds: Mapping[int, GuildPolicy] = MappingProxyType({})
        self._revisions: Mapping[int, int] = MappingProxyType({})
        self._epoch = 0

    def replace(self, policies: Mapping[int, GuildPolicy]) -> None:
        self._guilds = MappingProxyType(dict(policies))
        self._epoch += 1

    def upsert(self, guild_id: int, revision: int, policy: GuildPolicy) -> bool:
        if revision <= self._revisions.get(guild_id, -1):
            return False
        policies = dict(self._guilds)
        revisions = dict(self._revisions)
        policies[guild_id] = policy
        revisions[guild_id] = revision
        self._guilds = MappingProxyType(policies)
        self._revisions = MappingProxyType(revisions)
        self._epoch += 1
        return True

    def prefix(self, guild_id: int | None) -> str:
        if guild_id is None:
            return self.default_prefix
        policy = self._guilds.get(guild_id)
        return policy.prefix if policy else self.default_prefix

    def enabled_modules(self, guild_id: int | None) -> frozenset[str]:
        if guild_id is None:
            return frozenset()
        policy = self._guilds.get(guild_id)
        return policy.enabled_modules if policy else frozenset()

    def command_enabled(self, guild_id: int | None, module: str, acl_key: str) -> bool:
        if guild_id is None:
            return False
        policy = self._guilds.get(guild_id)
        return bool(
            policy
            and module in policy.enabled_modules
            and self.command_key(module, acl_key) in policy.enabled_commands
        )

    def module_enabled(self, guild_id: int | None, module: str) -> bool:
        return module in self.enabled_modules(guild_id)

    @policy_decision
    def allowed(
        self,
        *,
        guild_id: int | None,
        module: str,
        acl_key: str,
        public: bool,
        role_ids: frozenset[int],
        administrator: bool,
        channel_ids: frozenset[int] = frozenset(),
        category_ids: frozenset[int] = frozenset(),
    ) -> bool:
        if guild_id is None:
            return False
        policy = self._guilds.get(guild_id)
        command_key = self.command_key(module, acl_key)
        if policy is None or module not in policy.enabled_modules:
            return False
        if acl_key and command_key not in policy.enabled_commands:
            return False
        if administrator:
            return True
        module_roles = policy.module_roles.get(module, frozenset())
        command_roles = (
            policy.command_roles.get(command_key, frozenset()) if acl_key else frozenset()
        )
        role_grants = module_roles | command_roles
        if role_grants:
            if not role_ids & role_grants:
                return False
        elif not public:
            return False
        if not self._channel_allowed(policy.module_channels.get(module), channel_ids, category_ids):
            return False
        command_rule = policy.command_channels.get(command_key) if acl_key else None
        if not self._channel_allowed(command_rule, channel_ids, category_ids):
            return False
        return True

    @staticmethod
    def command_key(module: str, acl_key: str) -> str:
        return f"{module}:{acl_key}"

    @staticmethod
    def _channel_allowed(
        rule: tuple[frozenset[int], frozenset[int]] | None,
        channel_ids: frozenset[int],
        category_ids: frozenset[int],
    ) -> bool:
        if rule is None:
            return True
        allowed_channels, allowed_categories = rule
        if not allowed_channels and not allowed_categories:
            return True
        return bool(channel_ids & allowed_channels or category_ids & allowed_categories)
