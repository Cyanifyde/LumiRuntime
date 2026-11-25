import io
from contextlib import ExitStack, asynccontextmanager

import discord

from lumi.shared.attachment_payloads import action_scope, references, transition_scope


def binding(job, action=None):
    if action is None:
        return job.group, job.job_id, job.expires_at_ms
    return (
        job.group,
        action_scope(job.job_id, action.action_id),
        min(job.expires_at_ms, action.expires_at_ms),
    )


@asynccontextmanager
async def uploads(edge, intent, job, *, action=None, transfer=None, deadline=None):

    if not references(intent):
        yield ()
        return
    group, scope, expires = binding(job, action)
    if transfer is not None:
        group, transition = transfer
        scope = transition_scope(job.guild_id, transition)
    if deadline is not None:
        expires = min(expires, deadline)
    contents = await edge.attachment_payloads.read(group, scope, intent, deadline=expires)
    names = (
        tuple(item.file_name for item in intent.attachments)
        if intent.attachments
        else tuple(name for page in intent.pages for name in edge.validator._page_file_names(page))
    )
    with ExitStack() as stack:
        files = []
        for name, data in zip(names, contents, strict=True):
            source = stack.enter_context(io.BytesIO(data))
            file = discord.File(source, filename=name)
            stack.callback(file.close)
            files.append(file)
        yield tuple(files)


@asynccontextmanager
async def kwargs_for(edge, intent, job, *, for_edit=False, action=None):
    async with uploads(edge, intent, job, action=action) as files:

        extra = {"files": files} if files else {}
        yield edge._intent_kwargs(intent, job, for_edit=for_edit, **extra)
