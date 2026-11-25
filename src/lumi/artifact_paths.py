import os
from pathlib import Path


def karuta_csv_root(temp_root):
    if configured := os.getenv("LUMI_KARUTA_CSV_ROOT"):
        return Path(configured)
    path = Path(temp_root)
    return path.parent / (path.name + "-karuta-csv")
