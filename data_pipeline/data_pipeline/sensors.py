import os
from dagster import (
    sensor,
    RunRequest,
    DefaultSensorStatus,
    define_asset_job
)

from . import assets

db_dataset_job = define_asset_job(
    "db_dataset_job", selection=[
        assets.df,
        assets.typical_transactions,
        assets.all_transactions
    ]
)


@sensor(
    job=db_dataset_job,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=86400 # раз в сутки
)
def check_db_changes(context):
    file_path = "../shared/data/database.db"
    last_mtime = float(context.cursor) if context.cursor else 0

    if os.path.isfile(file_path):
        file_mtime = os.stat(file_path).st_mtime
        if file_mtime > last_mtime:
            yield RunRequest()
            max_mtime = max(last_mtime, file_mtime)
            context.update_cursor(str(max_mtime))
