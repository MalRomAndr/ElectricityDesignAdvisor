from dagster import Definitions, load_assets_from_modules, FilesystemIOManager
from dotenv import load_dotenv
from . import assets
from . import jobs
from . import sensors
from . import shedules

load_dotenv()   # Загрузить переменные окружения из файла .env
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[jobs.overlap_checking_job],
    sensors=[sensors.check_db_changes],
    schedules=[shedules.update_dataset_by_api, shedules.overlap_checking],
    resources={
        "data": FilesystemIOManager(base_dir="../shared/data"),
        "data_temp": FilesystemIOManager(base_dir="data_temp/")
        }
)
