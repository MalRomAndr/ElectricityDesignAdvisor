from dagster import (
    schedule,
    define_asset_job,
    ScheduleEvaluationContext,
    DefaultScheduleStatus,
    RunRequest
)

from . import assets
from . import jobs


update_rules_by_bom = define_asset_job(
    'update_rules_by_bom',
    selection=[
        assets.bom_transactions,
        assets.full_bom_transactions,
        assets.all_transactions_by_bom,
        assets.rules_by_bom
    ]
)

@schedule(
    job=update_rules_by_bom,
    cron_schedule="0 22 * * 6",
    default_status=DefaultScheduleStatus.RUNNING
)
def update_rules(context: ScheduleEvaluationContext):
    return RunRequest()

@schedule(
    job=jobs.overlap_checking_job,
    cron_schedule="0 23 * * 6",
    default_status=DefaultScheduleStatus.RUNNING
)
def overlap_checking(context: ScheduleEvaluationContext):
    return RunRequest()
