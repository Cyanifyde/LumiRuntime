import re
from contextlib import AsyncExitStack

import discord

from lumi.contracts.channel_messages import MESSAGE_PAGE
from lumi.contracts.channel_resources import POST_READS, POST_WRITES, ROLLBACK_POST_LIMIT
from lumi.shared.attachment_payloads import transition_scope
from lumi.shared.discord_effects import panel_binding

from .attachment_delivery import uploads
from .effect_grants import snowflake
from .intents import InvalidIntent
from .message_fetches import action_message_binding, fetch_message, remember_owned_output
from .registry import module_colour

NONCE = re.compile(r"[A-Za-z0-9:_-]{1,25}")
IDENTIFIER = re.compile(r"[a-z][a-z0-9_-]{0,63}")


class PostDenied(ValueError):
    pass


def valid(query, intent):
    operation, post = query.operation, query.post
    if operation not in POST_READS | POST_WRITES:
        return post is None and intent is None
    if operation == "list_posts":
        return post is None and intent is None
    if post is None:
        return False
    identified = operation in {"locate_post", "publish_post", "edit_post", "react", "pin"}
    presenting = operation in {"publish_post", "edit_post"}
    return bool(
        (post.message is not None) == (operation in {"inspect_post", "edit_post", "react", "pin"})
        and (post.message is None or snowflake(post.message))
        and (post.nonce is not None) == (operation in {"locate_post", "publish_post"})
        and (post.nonce is None or NONCE.fullmatch(post.nonce))
        and (post.module is not None) == identified and (post.group is not None) == identified
        and (not identified or (IDENTIFIER.fullmatch(post.module) and IDENTIFIER.fullmatch(post.group)))
        and (post.owner is None or ((presenting or operation == "locate_post") and snowflake(post.owner)))
        and (post.session is None or (operation in {"locate_post", "edit_post"} and re.fullmatch(r"[a-f0-9]{16,32}", post.session)))
        and (post.emoji is not None) == (operation == "react")
        and (post.emoji is None or (0 < len(post.emoji) <= 100 and not any(c.isspace() for c in post.emoji)))
        and (intent is not None) == presenting
        and (not presenting or (
            intent.kind == ("send" if operation == "publish_post" else "edit")
            and intent.presentation_mode in {"legacy", "v2"} and not intent.mention_actor
            and not intent.source_guild_ids and not intent.attachment_ref
            and (operation != "edit_post" or (not intent.attachments and not intent.session_id))
        ))
    )


def presentation_owner(edge, job, post):
    entry = edge.registry.snapshot.modules.get(post.module)
    if not entry or entry.manifest.group != post.group or entry.runtime_state != "online":
        raise PostDenied("presentation_unavailable")
    declared = entry.manifest.channel_transition
    if (post.group, post.module) != (job.group, job.module) and not (declared and declared.rebuild_posts):
        raise PostDenied("presentation_unavailable")
    return entry


def binding_code(job, query):
    return f"transition:{job.group}:{job.module}:{query.transition}"


def own(message, guild):
    return message.author.id == guild.me.id and not getattr(message, "webhook_id", None)


async def bind(edge, job, guild, target, query, message, *, family, intent=None):
    await remember_owned_output(
        edge.queue_redis, guild_id=guild.id, channel_id=target.id, message_id=message.id,
        module=query.post.module, family=family, panel=binding_code(job, query),
    )
    session = intent.session_id if intent is not None else query.post.session
    if session:
        await edge._bind_ui_session(
            session_id=session, owner_id=query.post.owner or job.actor_id, group=query.post.group,
            message_id=message.id, guild_id=guild.id, channel_id=target.id,
        )


async def read(edge, job, guild, target, query):
    operation, post = query.operation, query.post
    if operation == "inspect_post":
        try:
            message = await fetch_message(edge, target, post.message)
        except discord.NotFound as exc:
            if exc.code != 10008:
                raise
            return {"present": False}
        return {"present": True, "owned": own(message, guild), "pinned": bool(message.pinned)}
    if operation == "locate_post":
        presentation_owner(edge, job, post)
        matches = [message async for message in target.history(limit=MESSAGE_PAGE)
                   if own(message, guild) and str(message.nonce) == post.nonce]
        if len(matches) == 1:
            message = matches[0]
            family = "v2" if getattr(getattr(message, "flags", None), "components_v2", False) else "legacy"
            await bind(edge, job, guild, target, query, message, family=family)
        return {"messages": [message.id for message in matches]}
    return await list_facts(guild, target)


async def list_facts(guild, target):
    messages = [dict(id=item.id, owned=own(item, guild)) async for item in target.history(limit=ROLLBACK_POST_LIMIT + 1)]
    return {"messages": messages[:ROLLBACK_POST_LIMIT], "complete": len(messages) <= ROLLBACK_POST_LIMIT}


async def prepare(edge, job, guild, target, query, intent, stack: AsyncExitStack, *, deadline):

    operation, post = query.operation, query.post
    entry = presentation_owner(edge, job, post)
    message = None
    if post.message is not None:
        binding = await action_message_binding(edge.queue_redis, guild.id, post.message)
        expected_panel = binding_code(job, query)
        if operation == "edit_post" and post.session:
            if (post.module, post.group, target.id) != (job.module, job.group, job.channel_id):
                raise PostDenied("message_not_owned")
            acl = edge.registry.snapshot.scheduled[(job.module, job.request.route)].spec.acl_key
            expected_panel = panel_binding(job.actor_id, acl, post.session)
        if not binding or (binding["module"], binding.get("output_channel"), binding.get("panel")) != (
            post.module, target.id, expected_panel,
        ):
            raise PostDenied("message_not_owned")
        if intent is not None and binding.get("family") != intent.presentation_mode:
            raise PostDenied("invalid_action")
        message = target.get_partial_message(post.message)
    if intent is not None:
        if intent.content is not None and (post.module, post.group) != (job.module, job.group):
            raise PostDenied("invalid_action")
        if not getattr(target.permissions_for(guild.me), "send_messages", False):
            raise PostDenied("permission_denied")
        if intent.attachments and not getattr(target.permissions_for(guild.me), "attach_files", False):
            raise PostDenied("permission_denied")
        mode = intent.presentation_mode
        try:
            edge.validator.validate(intent, presentation_mode=mode)
            common = dict(
                module=post.module, registry=edge.registry.snapshot, owner_id=post.owner,
                blocklist=edge.blocklist, timeout=900 if post.owner else None,
            )
            colour = module_colour(common["registry"], post.module)
            kwargs = {"allowed_mentions": discord.AllowedMentions.none()}
            if mode == "v2":
                kwargs["view"] = edge.layouts.build_many(intent.pages, accent_color=colour, **common)
            else:
                kwargs["embeds"] = edge.layouts.legacy_embeds(intent.pages, accent_color=colour)
                if intent.controls:
                    kwargs["view"] = edge.layouts.legacy_controls(intent.controls, **common)
                if intent.content is not None:
                    kwargs["content"] = intent.content
            if intent.attachments:
                files = await stack.enter_async_context(
                    uploads(
                        edge, intent, job, transfer=(post.group, query.transition),
                        deadline=deadline,
                    ),
                )
                kwargs["files"] = list(files)
        except (ValueError, InvalidIntent, OSError):
            raise PostDenied("invalid_action") from None

    async def apply():
        if presentation_owner(edge, job, post) is not entry:
            raise PostDenied("access_changed")
        if operation == "publish_post":
            sent = await target.send(**kwargs, nonce=post.nonce)
            edge._bind_view(kwargs, sent)
            await bind(edge, job, guild, target, query, sent, family=mode, intent=intent)
            if intent.attachments:
                await edge.attachment_payloads.discard(
                    post.group, transition_scope(guild.id, query.transition), intent,
                )
            return "sent", {"message_id": sent.id}
        if operation == "edit_post":
            await message.edit(**kwargs)
        elif operation == "react":
            await message.add_reaction(post.emoji)
        else:
            await message.pin(reason=query.reason)
        return "edited", {"message_id": post.message}

    return apply
