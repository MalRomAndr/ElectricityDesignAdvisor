import sys
import pandas as pd
import numpy as np
import os
import shutil
import joblib
import logging
from dagster import op, job
from pygelf import GelfUdpHandler
from overlap import Overlap

# Чтобы работали импорты скриптов из папки shared:
sys.path.append("../shared/model/")
from fpgrowth import FPGrowthRecommender

overlap = Overlap()

def calc_overlap(dataset_pickle, transactions_pickle):
    """
    Расчет метрики
    """
    dataset = pd.read_pickle(dataset_pickle)
    transactions = pd.read_pickle(transactions_pickle)

    model = FPGrowthRecommender()
    model.fit(dataset)
    overlaps = []
    for p, t in zip(transactions['Id'], transactions['TypeId']):
        request = {'parts': p,
                   'structure_type': t}
        try: overlaps.append(overlap.check_overlap(request, model))
        except: pass
    return np.mean(overlaps)


@op
def all_overlap_old_rules():
    """
    Проверка новых транзакций на текущей версии модели
    """
    return calc_overlap('../shared/data/all_transactions',
                        'data_temp/all_transactions_by_api')


@op
def all_overlap_new_rules():
    """
    Проверка новых транзакций на новой версии модели
    """
    return calc_overlap('data_temp/all_transactions_by_api',
                        'data_temp/all_transactions_by_api')


@op
def compare_score(score_total_old, score_total_new):
    return score_total_new >= score_total_old


@op
def move_files(need_to_update):
    """
    Обновить файлы транзакций в общей папке
    """
    if need_to_update:
        shutil.move('data_temp/users_transactions_by_api', '../shared/data/users_transactions', copy_function=shutil.copy2)
        shutil.move('data_temp/all_transactions_by_api', '../shared/data/all_transactions', copy_function=shutil.copy2)


@op
def typical_overlap(move):
    """
    Считаем метрику на типовых корзинах
    """
    return calc_overlap('../shared/data/all_transactions',
                        '../shared/data/typical_transactions')


@op
def users_overlap(move):
    """
    Считаем метрику на пользовательских корзинах
    """
    return calc_overlap('../shared/data/all_transactions',
                        '../shared/data/users_transactions')


@op
def send_score_to_graylog(score_total: float, score_typical_only: float, score_users_only: float):
    """
    Отправить метрику в грейлог
    """
    if(os.getenv("DEBUG") == "True"):
        print("Logging is disabled in debug mode")
        return
    
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger()
    record = logging.LogRecord(
        name='Dagster',
        func='Overlap Metric',
        msg='check_overlap',
        level=10,
        pathname=None,
        lineno=None,
        exc_info=None,
        args=None,
    )
    logger.addHandler(
        GelfUdpHandler(
            host='dev.lep10.ru',
            port=12204,
            debug=True,
            include_extra_fields=True,
            _all_transactions=score_total,
            _typical_transactions=score_typical_only,
            _users_transactions=score_users_only
        )
    )
    logger.handle(record)


@op
def fit_model(move):
    dataset = pd.read_pickle('../shared/data/all_transactions')
    model = FPGrowthRecommender()
    model.fit(dataset)
    joblib.dump(model, '../shared/model/model')


@job
def overlap_checking_job():
    score_total_old = all_overlap_old_rules()
    score_total_new = all_overlap_new_rules()
    need_to_update = compare_score(score_total_old, score_total_new)
    move = move_files(need_to_update)
    score_typical_only = typical_overlap(move)
    score_users_only = users_overlap(move)
    send_score_to_graylog(score_total_new, score_typical_only, score_users_only)
    fit_model(move)
