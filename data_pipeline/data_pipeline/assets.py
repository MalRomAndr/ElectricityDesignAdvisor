import sys
import os
from dagster import asset

data_repo_path = os.path.dirname(os.path.abspath('../model/data_repo.py'))
sys.path.append(data_repo_path)

from data_repo import DataRepo

worker = DataRepo()

db_path = os.path.abspath('../database.db')
url = 'http://dev.lep10.ru:9000/api/search/messages'


@asset(io_manager_key='data')
def df():
    return worker.create_df(db_path)


@asset(io_manager_key='data', deps=[df])
def new_typical_transactions():
    return worker.get_dataset(by='db', path=db_path)


@asset(io_manager_key='data', deps=[new_typical_transactions])
def typical_transactions():
    return worker.update_dataset(path_from='data/typical_transactions',
                                 path_by='data/new_typical_transactions')


@asset(io_manager_key='data', deps=[typical_transactions])
def all_transactions():
    return worker.update_dataset(path_from='data/all_transactions',
                                 path_by='data/typical_transactions')


@asset(io_manager_key='data')
def new_users_transactions():
    return worker.get_dataset(by='api', path=url)


@asset(io_manager_key='data', deps=[new_users_transactions])
def users_transactions_by_api():
    return worker.update_dataset(path_from='data/users_transactions',
                                 path_by='data/new_users_transactions')


@asset(io_manager_key='data', deps=[users_transactions_by_api])
def all_transactions_by_api():
    return worker.update_dataset(path_from='data/all_transactions',
                                 path_by='data/users_transactions_by_api')
