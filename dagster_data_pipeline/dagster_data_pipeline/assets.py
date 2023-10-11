from dagster import asset

from . import functions


@asset(io_manager_key='pickle_data')
def df():
    return functions.make_df()

@asset(deps=[df], io_manager_key='pickle_data')
def db_transactions():
    return functions.make_db_transactions()

@asset(deps=[db_transactions], io_manager_key='pickle_data')
def all_transactions():
    return functions.update_all_transactions(by='db_transactions')

@asset(deps=[all_transactions], io_manager_key='pickle_data')
def rules():
    return functions.make_rules(by='all_transactions')




@asset(io_manager_key='new_pickle')
def bom_transactions():
    return functions.get_bom_transactions()

@asset(deps=[bom_transactions], io_manager_key='new_pickle')
def full_bom_transactions():
    return functions.update_bom_transactions(by='new/bom_transactions')

@asset(deps=[bom_transactions], io_manager_key='new_pickle')
def all_transactions_by_bom():
    return functions.update_all_transactions(by='new/bom_transactions')

@asset(deps=[all_transactions_by_bom], io_manager_key='new_pickle')
def rules_by_bom():
    return functions.make_rules(by='new/all_transactions_by_bom')
