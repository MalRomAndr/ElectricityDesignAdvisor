"""
Модуль с ассетами Dagster
"""
import os
import sys
import numpy as np
import pandas as pd
from dagster import MetadataValue, asset
from data_repo import DataRepo
from overlap import Overlap

# Чтобы работали импорты скриптов из папки shared:
sys.path.append("../shared/model/")
from fpgrowth import FPGrowthRecommender

repo = DataRepo()

db_path = os.path.abspath("../shared/data/database.db")
URL = "http://dev.lep10.ru:9000/api/search/messages"


def calc_overlap(fitted_model, transactions):
    """
    Расчет метрики
    """
    overlaps = []

    for p, t in zip(transactions["Id"], transactions["TypeId"]):
        request = {"parts": p, "structure_type": t}
        score = Overlap().check_overlap(request, fitted_model)
        if score is not None:
            overlaps.append(score)

    return float(np.mean(overlaps))


@asset(io_manager_key="data")
def df(context):
    """
    Вспомогательный датафрейм с деталями

    | Id | Name | SupplierId | StructureId | TypeId |

    для фильтрации правил по типу опоры и удаления ненужных деталей
    """
    df1 = repo.create_df(db_path)

    # Визуализация в интерфейсе дагстера:
    context.add_output_metadata({
        "Shape": str(df1.shape),
        "Data": MetadataValue.md(df1.head(3).to_markdown())
        })
    return df1


@asset(io_manager_key="data")
def typical_transactions(context):
    """
    Таблица транзакций из БД
    """
    df1 = repo.create_dataset(source="db", path=db_path)

    # Визуализация в интерфейсе дагстера:
    context.add_output_metadata({
        "Shape": str(df1.shape),
        "Data": MetadataValue.md(df1.head(3).to_markdown())
        })
    return df1


@asset(io_manager_key="data")
def user_transactions(context):
    """
    Транзакции из GrayLog за последний период
    """
    df1 = repo.create_dataset(source="api", path=URL)

    # Визуализация в интерфейсе дагстера:
    context.add_output_metadata({
        "Shape": str(df1.shape),
        "Data": MetadataValue.md(df1.head(3).to_markdown())
        })
    return df1


@asset(io_manager_key="data")
def full_transactions(context, typical_transactions, user_transactions):
    """
    Транзакции typical + user
    """
    df1 = (pd.concat([typical_transactions, user_transactions], axis=0)
            .drop_duplicates()
            .reset_index(drop=True))

    # Визуализация в интерфейсе дагстера:
    context.add_output_metadata({
        "Shape": str(df1.shape),
        "Data": MetadataValue.md(df1.head(3).to_markdown())
        })
    return df1


@asset(io_manager_key="data")
def model(df, full_transactions):
    """
    Обученная модель
    """
    my_model = FPGrowthRecommender("../shared/data/df")
    my_model.fit(full_transactions)
    return my_model


@asset()
def score_typical_only(context, model, typical_transactions):
    """
    Скор только на типовых проектах из БД
    """
    score = calc_overlap(model, typical_transactions)
    context.add_output_metadata({"Score": float(score)})
    return score


@asset()
def score_user_only(context, model, user_transactions):
    """
    Скор только на пользовательских данных из GrayLog
    """
    score = calc_overlap(model, user_transactions)
    context.add_output_metadata({"Score": float(score)})
    return score


@asset()
def score_total(context, model, full_transactions):
    """
    Скор на всех данных (typical + user)
    """
    score = calc_overlap(model, full_transactions)
    context.add_output_metadata({"Score": float(score)})
    return score
