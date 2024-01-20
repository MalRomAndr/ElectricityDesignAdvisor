import os
from dagster import asset
from data_repo import DataRepo

repo = DataRepo()

db_path = os.path.abspath("../shared/data/database.db")
URL = "http://dev.lep10.ru:9000/api/search/messages"


@asset(io_manager_key="data")
def df():
    """
    Датафрейм вида | Id | TypeId | SupplierId |

    для фильтрации правил по типу опоры и удаления ненужных деталей
    """
    return repo.create_df(db_path)


@asset(io_manager_key="data", deps=[df])
def typical_transactions():
    """Таблица транзакций из БД"""
    return repo.create_dataset(source="db", path=db_path)


@asset(io_manager_key="data", deps=[typical_transactions])
def all_transactions():
    return repo.concat_dataset(df1_path="../shared/data/all_transactions",
                                 df2_path="../shared/data/typical_transactions")


@asset(io_manager_key="data_temp")
def new_users_transactions():
    """Транзакции из GrayLog за последний период"""
    return repo.create_dataset(source="api", path=URL)


@asset(io_manager_key="data_temp", deps=[new_users_transactions])
def users_transactions_by_api():
    return repo.concat_dataset(df1_path="../shared/data/users_transactions",
                                 df2_path="data_temp/new_users_transactions")


@asset(io_manager_key="data_temp", deps=[users_transactions_by_api])
def all_transactions_by_api():
    return repo.concat_dataset(df1_path="../shared/data/all_transactions",
                                 df2_path="data_temp/new_users_transactions")
