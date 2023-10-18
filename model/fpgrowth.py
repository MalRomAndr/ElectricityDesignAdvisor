import pandas as pd
import os
import math
from mlxtend.frequent_patterns import fpgrowth, association_rules
from mlxtend.preprocessing import TransactionEncoder


class FPGrowthRecommender:
    def __init__(self):
        self.rules = None
        self.data_folder = '../model/data/'

    def fit(self, dataset):
        te = TransactionEncoder()
        encoded = pd.DataFrame(te.fit(dataset['Id']).transform(dataset['Id']), columns=te.columns_)
        frequent_itemsets = fpgrowth(encoded, min_support=0.0001, use_colnames=True, max_len=2)
        rules = association_rules(frequent_itemsets, metric="support", min_threshold=0)
        rules['antecedents'] = rules['antecedents'].apply(lambda x: list(x)[0])
        rules['consequents'] = rules['consequents'].apply(lambda x: list(x)[0])
        self.rules = rules

    def predict(self, request):
        type_id = request['structure_type']
        parts = request['parts']
        df_path = os.path.join(self.data_folder, 'df')
        if os.path.isfile(df_path):
            df = pd.read_pickle(df_path)
        else:
            raise FileNotFoundError('Dataframe from database not found')

        # Составляем список деталей, которые соответствуют TypeId в запросе
        ids = df[df['TypeId'] == type_id]['Id']
        predictions = pd.DataFrame()
        for element in parts:
            # Находим все ассоциации по элементу, удаляем лишние столбцы
            answer = self.rules[self.rules['antecedents'] == element][
                [
                    'antecedents',
                    'consequents',
                    'consequent support',
                    'confidence'
                ]
            ]
            # Проверяем, чтобы предлагаемые элементы не содержались в запросе
            answer = answer[~answer['consequents'].isin(parts)]
            # Оставляем в списке деталей только найденные рекомендации
            ids = ids[ids.isin(answer['consequents'])]
            # Приводим ответ в соответствие с отфильтрованным результатом
            # (исключаем из рекомендаций не подходящие TypeId)
            answer = answer[answer['consequents'].isin(ids)]
            # Вычисляем метрику
            answer['metric'] = answer['consequent support'] * 0.45 + answer['confidence'] * 0.55
            answer = answer.sort_values(by='metric', ascending=False)
            # Удаляем из ответа повторения по id в итоговом предикте
            try:
                answer = answer[~answer['consequents'].isin(predictions['consequents'])]
            except:
                pass
            # Берем верхние строки
            answer = answer[:math.ceil(25 / len(parts))]
            # Добавляем ответ для одного элемента в общие результаты
            predictions = pd.concat([predictions, answer], axis=0)
        predictions = predictions.sort_values(by='metric', ascending=False).head(25)['consequents'].sort_values()
        return list(predictions)
