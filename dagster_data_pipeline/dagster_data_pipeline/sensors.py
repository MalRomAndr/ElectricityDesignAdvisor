import os
from dagster import (
    sensor,
    RunRequest,
    define_asset_job,
    DefaultSensorStatus
)
from . import assets

update_rules_by_db = define_asset_job(
    'update_rules_by_db',
    selection=[
        assets.df,
        assets.db_transactions,
        assets.all_transactions,
        assets.rules
    ]
)

@sensor(
    job=update_rules_by_db,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=86400 #раз в сутки
)
def check_db_changes(context):
    file_path = os.path.join('../', 'database.db')
    last_mtime = float(context.cursor) if context.cursor else 0

    if os.path.isfile(file_path):
        file_mtime = os.stat(file_path).st_mtime
        if file_mtime > last_mtime:
            yield RunRequest()
            max_mtime = max(last_mtime, file_mtime)
            context.update_cursor(str(max_mtime))
