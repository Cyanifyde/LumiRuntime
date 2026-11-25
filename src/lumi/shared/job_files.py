import re

HEX_ID = re.compile(r"[a-f0-9]{32}\Z")
JOB_FILE_NAME = re.compile(r"job-[a-f0-9]{32}-[a-f0-9]{32}\Z")


def job_file_name(reference, job_id):
    prefix, separator, token = reference.partition(":")
    if prefix != "job-file" or not separator or not HEX_ID.fullmatch(token) or (
        not isinstance(job_id, str) or not HEX_ID.fullmatch(job_id)
    ):
        raise ValueError("Temporary attachment lacks an exact job binding")
    return f"job-{job_id}-{token}"
