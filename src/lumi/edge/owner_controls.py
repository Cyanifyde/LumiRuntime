from __future__ import annotations

from lumi.contracts import CommandSpec, ComponentSpec, JobEnvelope


def owner_command_prefix(snapshot, content: str, guild_prefix: str) -> str | None:

    for prefix in dict.fromkeys((".", guild_prefix)):
        if prefix and content.startswith(prefix):
            root = content[len(prefix) :].partition(" ")[0].casefold()
            command = snapshot.commands.get(root)
            if command and command.spec.use_default_prefix and command.spec.owner_scope == "global":
                return prefix
    return None


def owner_job_spec(snapshot, job: JobEnvelope) -> CommandSpec | ComponentSpec | None:

    module = snapshot.modules.get(job.module)
    if module is None or module.manifest.group != job.group:
        return None
    request = job.request
    dispatch = request.event.get("dispatch")
    if dispatch == "command" and request.route == request.handler:
        for spec in module.manifest.commands:
            if spec.handler == request.handler and spec.owner_scope:
                return spec
    if dispatch == "component":
        namespace, _, action = request.route.partition(".")
        registered = snapshot.components.get((namespace, action))
        if (
            registered and registered.module == job.module and registered.group == job.group
            and registered.spec.handler == request.handler and registered.spec.owner_scope
        ):
            return registered.spec
    return None
