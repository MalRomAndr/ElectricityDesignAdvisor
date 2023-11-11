import sys
import os
from dagster import asset
from data_repo import DataRepo

worker = DataRepo()

db_path = os.path.abspath("../shared/data/database.db")
url = "http://dev.lep10.ru:9000/api/search/messages"


@asset(io_manager_key="data")
def df():
    return worker.create_df(db_path)


@asset(io_manager_key="data_temp", deps=[df])
def new_typical_transactions():
    return worker.get_dataset(by='db', path=db_path)


@asset(io_manager_key="data", deps=[new_typical_transactions])
def typical_transactions():
    return worker.concat_dataset(df1_path="../shared/data/typical_transactions",
                                 df2_path="data_temp/new_typical_transactions")


@asset(io_manager_key="data", deps=[typical_transactions])
def all_transactions():
    return worker.concat_dataset(df1_path="../shared/data/all_transactions",
                                 df2_path="data_temp/new_typical_transactions")


@asset(io_manager_key="data_temp")
def new_users_transactions():
    return worker.get_dataset(by='api', path=url)


@asset(io_manager_key="data_temp", deps=[new_users_transactions])
def users_transactions_by_api():
    return worker.concat_dataset(df1_path="../shared/data/users_transactions",
                                 df2_path="data_temp/new_users_transactions")


@asset(io_manager_key="data_temp", deps=[users_transactions_by_api])
def all_transactions_by_api():
    return worker.concat_dataset(df1_path="../shared/data/all_transactions",
                                 df2_path="data_temp/new_users_transactions")
