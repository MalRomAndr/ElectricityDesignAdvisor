import numpy as np
import pandas as pd

from dagster import op, job, Failure

from . import functions



@op()
def all_transactions_by_bom_old_rules_overlap():
    transactions = pd.read_pickle('pickle_data/new/all_transactions_by_bom')
    overlaps = []
    for id, type in zip(transactions['Id'], transactions['TypeId']):
        id = np.array(id)
        try: overlaps.append(functions.overlap_checking(id, type, rules='rules'))
        except: pass
    return np.mean(overlaps)
@op()
def all_transactions_by_bom_new_rules_overlap(old_rules_metric):
    transactions = pd.read_pickle('pickle_data/new/all_transactions_by_bom')
    overlaps = []
    for id, type in zip(transactions['Id'], transactions['TypeId']):
        id = np.array(id)
        try: overlaps.append(functions.overlap_checking(id, type, rules='new/rules_by_bom'))
        except: pass
    if np.mean(overlaps) < old_rules_metric:
        raise Failure(description="Metric has decreased")
    return np.mean(overlaps)
@op()
def send_all_transactions2gray(new_rules_metric):
    functions.send_overlap_to_gray('all_transactions', new_rules_metric)
    result = 'success'
    return result


@op()
def move_bom_files_from_new(send):
    if send == 'success':
        pd.read_pickle('pickle_data/new/bom_transactions').to_pickle('pickle_data/bom_transactions')
        pd.read_pickle('pickle_data/new/full_bom_transactions').to_pickle('pickle_data/full_bom_transactions')
        pd.read_pickle('pickle_data/new/all_transactions_by_bom').to_pickle('pickle_data/all_transactions')
        pd.read_pickle('pickle_data/new/rules_by_bom').to_pickle('pickle_data/rules')
    result = 'success'
    return result


@op()
def typical_transactions_overlap(move):
    if move == 'success':
        transactions = pd.read_pickle('pickle_data/db_transactions')
        overlaps = []
        for id, type in zip(transactions['Id'], transactions['TypeId']):
            id = np.array(id)
            try: overlaps.append(functions.overlap_checking(id, type, rules='rules'))
            except: pass
        result = np.mean(overlaps)
        functions.send_overlap_to_gray('typical_transactions', result)
@op()
def bom_transactions_overlap(move):
    if move == 'success':
        transactions = pd.read_pickle('pickle_data/bom_transactions')
        overlaps = []
        for id, type in zip(transactions['Id'], transactions['TypeId']):
            id = np.array(id)
            try: overlaps.append(functions.overlap_checking(id, type, rules='rules'))
            except: pass
        result = np.mean(overlaps)
        functions.send_overlap_to_gray('bom_transactions', result)

@job
def overlap_checking_job():
    old_rules_metric = all_transactions_by_bom_old_rules_overlap()
    new_rules_metric = all_transactions_by_bom_new_rules_overlap(old_rules_metric)
    send = send_all_transactions2gray(new_rules_metric)
    move = move_bom_files_from_new(send)
    typical_transactions_overlap(move)
    bom_transactions_overlap(move)
