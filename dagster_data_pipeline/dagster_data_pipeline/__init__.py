from dagster import (
    Definitions,
    load_assets_from_modules,
    FilesystemIOManager,
)

from . import assets
from . import sensors
from . import shedules
from . import jobs

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    sensors=[sensors.check_db_changes],
    schedules=[shedules.update_rules, shedules.overlap_checking],
    jobs=[jobs.overlap_checking_job],
    resources={
        "pickle_data": FilesystemIOManager(base_dir="pickle_data/"),
        "new_pickle": FilesystemIOManager(base_dir="pickle_data/new")
    }
)

