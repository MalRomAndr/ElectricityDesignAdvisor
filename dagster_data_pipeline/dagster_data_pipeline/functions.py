import pandas as pd
import numpy as np
import requests
import json
import math
import sqlite3
import os
import logging
from pygelf import GelfUdpHandler
from sklearn.model_selection import KFold
from mlxtend.frequent_patterns import fpgrowth, association_rules
from mlxtend.preprocessing import TransactionEncoder


def make_df():
    conn = sqlite3.connect('../database.db')

    # Подгружаем таблицы из sql
    data_Parts = pd.read_sql("select Id, Name from Parts;", con=conn)
    data_StructuresParts = pd.read_sql("select StructureId, PartId from StructuresParts;", con=conn)
    data_Structures = pd.read_sql("select Id, TypeId from Structures;", con=conn)
    data_Conductors = pd.read_sql("select PartId, TypeId from Conductors;", con=conn)

    # Объединяем в датасет
    df = data_Parts.merge(data_StructuresParts, left_on='Id', right_on='PartId', how='outer').drop('PartId', axis=1)
    data_Structures.rename(columns={'Id': 'Id_str'}, inplace=True)
    df = df.merge(data_Structures, left_on='StructureId', right_on='Id_str').drop(labels='Id_str', axis=1)
    data_Conductors.rename(columns={'PartId': 'Id'}, inplace=True)
    df = pd.concat([df, data_Conductors], axis=0)

    # Отсекаем лишнее в наименованиях опор
    df['StructureId'] = df['StructureId'].str.split('_').str[0]

    # Заполняем пропуски в данных
    df[['Name', 'StructureId']] = df[['Name', 'StructureId']].fillna('')

    # Удаляем строки дубликаты
    df = df.drop_duplicates().reset_index(drop=True)
    return df


def make_db_transactions():
    df = pd.read_pickle('pickle_data/df')
    transactions = df.pivot_table(index=['StructureId'], values=['Id', 'TypeId'], aggfunc=pd.Series.unique) \
        .reset_index(drop=False).drop(0, axis=0)
    transactions['TypeId'] = transactions['TypeId'].apply(lambda x: x[0])
    transactions['Id'] = transactions['Id'].apply(lambda x: tuple(x))
    transactions = transactions.drop_duplicates().reset_index(drop=True)
    return transactions


def update_all_transactions(by: str):
    file_path = os.path.join('pickle_data/', 'all_transactions')
    if os.path.isfile(file_path):
        transactions = pd.read_pickle(file_path)
    else:
        transactions = pd.DataFrame()
    transactions = pd.concat([transactions, pd.read_pickle('pickle_data/'+by)], axis=0)
    transactions = transactions.drop_duplicates().reset_index(drop=True)
    return transactions


def update_bom_transactions(by: str):
    file_path = os.path.join('pickle_data/', 'full_bom_transactions')
    if os.path.isfile(file_path):
        transactions = pd.read_pickle(file_path)
    else:
        transactions = pd.DataFrame()
    transactions = pd.concat([transactions, pd.read_pickle('pickle_data/'+by)], axis=0)
    transactions = transactions.drop_duplicates().reset_index(drop=True)
    return transactions


def make_rules(by: str):
    transactions = pd.read_pickle('pickle_data/'+by)
    te = TransactionEncoder()
    encoded_tr = pd.DataFrame(te.fit(transactions['Id']).transform(transactions['Id']), columns=te.columns_)
    frequent_itemsets = fpgrowth(encoded_tr, min_support=0.001, use_colnames=True, max_len=2)
    rules = association_rules(frequent_itemsets, metric="support", min_threshold=0)
    rules['antecedents'] = rules['antecedents'].apply(lambda x: list(x)[0])
    rules['consequents'] = rules['consequents'].apply(lambda x: list(x)[0])
    return rules


def get_bom_transactions():
    # Доступ к Graylog
    url = 'http://dev.lep10.ru:9000/api/search/messages'
    params = {
        'query': 'message:"BOM created" AND level:6',
        'pretty': 'true',
        'timerange': '1w',
        'fields': 'bom'
    }
    headers = {'Accept': 'application/json'}
    with open('graytoken.txt', 'r') as token:
        token = token.read()
        auth = (token, 'token')

    #Получение ответа от API
    response = requests.get(url, headers=headers, auth=auth, params=params).json()

    #Преобразование в список транзакций
    transactions = pd.DataFrame(columns=['StructureId', 'Id', 'TypeId'])
    for bom in range(len(response['datarows'])):
        transactions = pd.concat((transactions, pd.DataFrame(json.loads(response['datarows'][bom][0])['structures'][:]) \
                                  .rename(columns={'structure_name': 'StructureId', 'parts': 'Id', 'structure_type': 'TypeId'})), axis=0)
    transactions = transactions[transactions['Id'].apply(lambda x: np.array(x).size > 0)]
    transactions['Id'] = transactions['Id'].apply(lambda x: tuple(x))
    transactions = transactions.drop_duplicates().reset_index(drop=True)
    return transactions


def get_predictions(request, type_id, rules: str):
    df = pd.read_pickle('pickle_data/df')
    rules = pd.read_pickle('pickle_data/'+rules)
    # Фильтруем датафрейм по TypeID, преобразуем в Series
    data = df[df['TypeId'] == type_id]['Id']

    predictions = pd.DataFrame()
    for element in request:
        # Находим все ассоциации по элементу, удаляем лишние столбцы
        predict = rules[rules['antecedents'] == element][['antecedents', 'consequents', 'consequent support', 'confidence']]
        # Проверяем, чтобы предлагаемые элементы не содержались в запросе
        predict = predict[~predict['consequents'].isin(request)]
        # Оставляем в Series только найденные рекомендации
        data = data[data.isin(predict['consequents'])]
        # Приводим ответ в соответствие с отфильтрованным результатом (исключаем из рекомендаций не подходящие TypeId)
        predict = predict[predict['consequents'].isin(data)]
        # Вычисляем метрику
        predict['metric'] = predict['consequent support'] * 0.45 + predict['confidence'] * 0.55
        predict = predict.sort_values(by='metric', ascending=False)
        # Удаляем из ответа повторения по id в итоговом предикте
        try: predict = predict[~predict['consequents'].isin(predictions['consequents'])]
        except: pass
        # Берем верхние строки
        predict = predict[:math.ceil(25 / len(request))]
        # Добавляем ответ для одного элемента в общие результаты
        predictions = pd.concat([predictions, predict], axis=0)

    predictions = list(predictions.sort_values(by='metric', ascending=False).head(25)['consequents'].sort_values())
    return predictions


def overlap_checking(request, type_id, rules: str):
    kf = KFold(n_splits=3, shuffle=True, random_state=42)
    overlap = []
    for check_index, req_index in kf.split(request):
        # Передаем в запрос 1 фолд
        request_part = request[req_index]
        # Элементы, которых нет в запросе - ожидаемый ответ
        actual = request[check_index]
        # Получаем рекомендации
        predictions = get_predictions(request_part, type_id, rules)
        # Находим общие элементы в ответе и в ожидаемом ответе
        common_elements = set(actual) & set(predictions)
        overlap.append(len(common_elements) / len(actual))

    return np.mean(overlap)


def send_overlap_to_gray(func, metric_value):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    record = logging.LogRecord(
        name='Dagster Output',
        func=func,
        msg='check_overlap',
        level=20,
        pathname=None,
        lineno=None,
        exc_info=None,
        args=None,
    )
    logger.addHandler(GelfUdpHandler(
        host='dev.lep10.ru',
        port=12204,
        debug=True,
        include_extra_fields=True,
        metric_value=metric_value)
    )
    logger.handle(record)
