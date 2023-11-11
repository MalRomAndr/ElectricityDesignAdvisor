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

def calc_overlap(dataset, transactions):
    '''
    Расчет метрики
    '''
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
    '''
    Проверка новых транзакций на текущей версии модели
    '''
    return calc_overlap('../shared/data/all_transactions',
                        'data_temp/all_transactions_by_api')


@op
def all_overlap_new_rules():
    '''
    Проверка новых транзакций на новой версии модели
    '''
    return calc_overlap('data_temp/all_transactions_by_api',
                        'data_temp/all_transactions_by_api')


@op
def compare_overlap(old_all_overlap, new_all_overlap):
    return new_all_overlap >= old_all_overlap


@op
def move_files(need_to_update):
    '''
    Обновить файлы транзакций в общей папке
    '''
    if need_to_update:
        shutil.move('data_temp/users_transactions_by_api', '../shared/data/users_transactions', copy_function=shutil.copy2)
        shutil.move('data_temp/all_transactions_by_api', '../shared/data/all_transactions', copy_function=shutil.copy2)


@op
def typical_overlap(move):
    '''
    Считаем метрику на типовых корзинах
    '''
    return calc_overlap('../shared/data/all_transactions',
                        '../shared/data/typical_transactions')


@op
def users_overlap(move):
    '''
    Считаем метрику на пользовательских корзинах
    '''
    return calc_overlap('../shared/data/all_transactions',
                        '../shared/data/users_transactions')


@op
def send2gray(new_all_overlap, typical, users):
    '''
    Отправить метрику в грейлог
    '''
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
            _all_transactions=new_all_overlap,
            _typical_transactions=typical,
            _users_transactions=users
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
    old_all_overlap = all_overlap_old_rules()
    new_all_overlap = all_overlap_new_rules()
    need_to_update = compare_overlap(old_all_overlap, new_all_overlap)
    move = move_files(need_to_update)
    typical = typical_overlap(move)
    users = users_overlap(move)
    send2gray(new_all_overlap, typical, users)
    fit_model(move)
