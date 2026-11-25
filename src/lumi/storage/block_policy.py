VIEW = "lumi_block_subjects"
QUERY = (
    "SELECT user_id FROM global_blocks UNION ALL "
    "SELECT user_id FROM global_temporary_blocks "
    "WHERE expires_at_ms>CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS UNSIGNED)"
)


def relation(*, scoped):

    return f"`{VIEW}`" if scoped else f"({QUERY})"
