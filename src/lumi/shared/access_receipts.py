def admitted_request(request, *, module, group, generation, actor, guild, channel, acl=None, passive=False):
    event = request.event
    if event.get("dispatch") not in {"command", "component"} and not (
        passive and event.get("dispatch") == "passive"
    ):
        return False
    expected = dict(
        schema=1, module=module, group=group, generation=generation,
        route=request.route, handler=request.handler, acl=event.get("_acl_key", ""),
        public=event.get("_public", False), actor=actor, guild=guild, channel=channel,
    )
    return event.get("_edge_access") == expected and (acl is None or acl == expected["acl"])
