import sys
import os
import pandas as pd
import numpy as np
import shutil
import joblib
from dagster import op, job
import logging
from pygelf import GelfUdpHandler

model_path = os.path.dirname(os.path.abspath('../model/fpgrowth.py'))
metric_path = os.path.dirname(os.path.abspath('../model/overlap.py'))
sys.path.append(model_path)
sys.path.append(metric_path)
from fpgrowth import FPGrowthRecommender
from overlap import Overlap


model = FPGrowthRecommender()
overlap = Overlap()


@op
def all_overlap_old_rules():
    dataset = pd.read_pickle('data/all_transactions')
    transactions = pd.read_pickle('data/all_transactions_by_api')
    model.fit(dataset)
    overlaps = []
    for p, t in zip(transactions['Id'], transactions['TypeId']):
        request = {'parts': p,
                   'structure_type': t}
        try: overlaps.append(overlap.check_overlap(request, model))
        except: pass
    return np.mean(overlaps)


@op
def all_overlap_new_rules():
    dataset = pd.read_pickle('data/all_transactions_by_api')
    transactions = pd.read_pickle('data/all_transactions_by_api')
    model.fit(dataset)
    overlaps = []
    for p, t in zip(transactions['Id'], transactions['TypeId']):
        request = {'parts': p,
                   'structure_type': t}
        try: overlaps.append(overlap.check_overlap(request, model))
        except: pass
    return np.mean(overlaps)


@op
def compare_overlap(old_all_overlap, new_all_overlap):
    return new_all_overlap >= old_all_overlap


@op
def move_files(compare):
    shutil.copy2('data/df', '../model/data/df')
    if compare:
        shutil.move('data/users_transactions_by_api', 'data/users_transactions', copy_function=shutil.copy2)
        shutil.move('data/all_transactions_by_api', 'data/all_transactions', copy_function=shutil.copy2)


@op
def typical_overlap(move):
    dataset = pd.read_pickle('data/all_transactions')
    transactions = pd.read_pickle('data/typical_transactions')
    model.fit(dataset)
    overlaps = []
    for p, t in zip(transactions['Id'], transactions['TypeId']):
        request = {'parts': p,
                   'structure_type': t}
        try: overlaps.append(overlap.check_overlap(request, model))
        except: pass
    return np.mean(overlaps)


@op
def users_overlap(move):
    dataset = pd.read_pickle('data/all_transactions')
    transactions = pd.read_pickle('data/users_transactions')
    model.fit(dataset)
    overlaps = []
    for p, t in zip(transactions['Id'], transactions['TypeId']):
        request = {'parts': p,
                   'structure_type': t}
        try: overlaps.append(overlap.check_overlap(request, model))
        except: pass
    return np.mean(overlaps)


@op
def send2gray(new_all_overlap, typical, users):
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
    dataset = pd.read_pickle('data/all_transactions')
    model.fit(dataset)
    joblib.dump(model, '../model/data/model')


@job
def overlap_checking_job():
    old_all_overlap = all_overlap_old_rules()
    new_all_overlap = all_overlap_new_rules()
    compare = compare_overlap(old_all_overlap, new_all_overlap)
    move = move_files(compare)
    typical = typical_overlap(move)
    users = users_overlap(move)
    send2gray(new_all_overlap, typical, users)
    fit_model(move)
