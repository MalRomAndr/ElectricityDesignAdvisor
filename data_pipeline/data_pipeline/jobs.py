"""
Модуль с операциями Dagster
"""
import logging
import os
from dagster import job, op
from pygelf import GelfUdpHandler
from .assets import score_total, score_typical_only, score_user_only

@op
def send_score_to_graylog(score_typical, score_user, score):
    """
    Отправить метрику в грейлог
    """
    if os.getenv("DEBUG") == "True":
        print("Logging is disabled in debug mode")
        return

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger()
    record = logging.LogRecord(
        name="Dagster",
        func="Overlap Metric",
        msg="check_overlap",
        level=10,
        pathname=None,
        lineno=None,
        exc_info=None,
        args=None,
    )
    logger.addHandler(
        GelfUdpHandler(
            host="dev.lep10.ru",
            port=12204,
            debug=True,
            include_extra_fields=True,
            _all_transactions=score,
            _typical_transactions=score_typical,
            _users_transactions=score_user
        )
    )
    logger.handle(record)

@job
def overlap_checking_job():
    """
    Отправить метрику
    """
    send_score_to_graylog(
        score_typical_only.to_source_asset(),
        score_user_only.to_source_asset(),
        score_total.to_source_asset())
