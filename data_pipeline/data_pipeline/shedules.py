from dagster import (
    schedule,
    ScheduleEvaluationContext,
    DefaultScheduleStatus,
    RunRequest,
    define_asset_job
)

from . import jobs
from . import assets

api_dataset_job = define_asset_job(
    'api_dataset_job', selection=[
        assets.new_users_transactions,
        assets.users_transactions_by_api,
        assets.all_transactions_by_api
    ]
)


@schedule(
    job=api_dataset_job,
    cron_schedule="0 22 * * 6",
    default_status=DefaultScheduleStatus.STOPPED
)
def update_dataset_by_api(context: ScheduleEvaluationContext):
    return RunRequest()


@schedule(
    job=jobs.overlap_checking_job,
    cron_schedule="0 23 * * 6",
    default_status=DefaultScheduleStatus.STOPPED
)
def overlap_checking(context: ScheduleEvaluationContext):
    return RunRequest()
