GROUPS = ("platform", "security", "community", "cards")
STAGE = "handover_stage_security"
CLEANUP = "handover_cleanup_security"
RETENTION_MS = 30 * 86400000


def stage_insert(code, revision, started):


    return f"""INSERT INTO moderation_channel_handovers
        (id,guild_id,old_channel,new_channel,phase,document)
        SELECT job.id,job.guild_id,job.channel_id,0,'waiting',
          JSON_OBJECT(
            'id',job.id,'guild',job.guild_id,'old',job.channel_id,'new',0,
            'phase','waiting','started',CAST({started} AS UNSIGNED),
            'actor',CAST(JSON_VALUE(job.document,'$.actor') AS UNSIGNED),
            'settings',JSON_EXTRACT(job.document,'$.settings'),
            'epoch',0,'acks',JSON_OBJECT(),'plans',JSON_OBJECT()
          )
        FROM moderation_purge_jobs AS job
        WHERE job.id={code} AND job.revision={revision}
          AND job.state IN ('queued','cancelling')
          AND JSON_VALUE(job.document,'$.mode')='reset'
          AND JSON_TYPE(JSON_EXTRACT(job.document,'$.settings'))='OBJECT'
        ON DUPLICATE KEY UPDATE id=moderation_channel_handovers.id"""


def retention_statements(cutoff):
    return (
        "DELETE FROM moderation_channel_handovers "
        "WHERE phase IN ('complete','rolled_back') "
        f"AND JSON_VALUE(document,'$.completed')<{cutoff} LIMIT 1000",
        "DELETE FROM moderation_channel_writes WHERE state='settled' "
        f"AND updated_ms<{cutoff} LIMIT 1000",
    )


def names(group):
    if group not in GROUPS:
        raise ValueError("Unknown handover participant.")
    return "handover_" + group, "handover_heartbeat_" + group, "handover_ack_" + group
