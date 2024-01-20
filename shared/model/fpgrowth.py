"""
Модуль для подсчета ассоциативных правил для программы SmartLine
"""
import math
import os
import pandas as pd
from mlxtend.frequent_patterns import fpgrowth, association_rules
from mlxtend.preprocessing import TransactionEncoder


class FPGrowthRecommender:
    """
    Класс для подсчета ассоциативных правил, алгоритм fpgrowth
    """
    def __init__(self, df_path: str):
        """:param df_path: Путь к файлу датафрейма (pickle)
        
        Вид DataFrame:
        | Id         | TypeId    |
        | ---------- | --------- |
        | d10 (цинк) | support10 |
        | COT37R     | commCable |

        DataFrame нужен для фильтрации правил по типу опоры и удаления неизвестных деталей.
        """
        self.rules = None

        if os.path.isfile(df_path):
            self.df = pd.read_pickle(df_path)
        else:
            raise FileNotFoundError("Dataframe from database not found")


    def fit(self, dataset):
        """
        Подсчитать associations rules

        :param dataset: Таблица транзакций.

        | StructureId | Id                   |
        | ----------- | -------------------- |
        | П20-3Н      | (N 640, P 616, P 72) |
        | ППоБ10-3Н   | ("d10", "COT37R")    |
        """
        te = TransactionEncoder()
        encoded = pd.DataFrame(te.fit(dataset["Id"]).transform(dataset["Id"]), columns=te.columns_)
        frequent_itemsets = fpgrowth(encoded, min_support=0.0001, use_colnames=True, max_len=2)
        rules = association_rules(frequent_itemsets, metric="support", min_threshold=0)
        rules["antecedents"] = rules["antecedents"].apply(lambda x: list(x)[0])
        rules["consequents"] = rules["consequents"].apply(lambda x: list(x)[0])
        self.rules = rules


    def predict(self, request):
        """Найти в правилах подходящие детали

        :param request: Запрос json.

        Пример запроса:

        {
            "structure_type": "support",
            "parts": [
                "ЗП6",
                "П-3и",
                "СВ95-3",
                "У4",
                "COT36.2"
            ],
            "structure_name" (не используется): "А11"
        }

        Возвращает:

        Список Id рекомендованных деталей
        """
        type_id = request["structure_type"]
        parts = request["parts"]

        # Составляем список деталей, которые соответствуют TypeId в запросе
        ids = self.df[self.df["TypeId"] == type_id]["Id"]

        predictions = pd.DataFrame()

        for part_id in parts:
            # Находим все ассоциации по элементу, удаляем лишние столбцы
            answer = self.rules[self.rules["antecedents"] == part_id][
                [
                    "antecedents",
                    "consequents",
                    "consequent support",
                    "confidence"
                ]
            ]
            # Проверяем, чтобы предлагаемые элементы не содержались в запросе
            answer = answer[~answer["consequents"].isin(parts)]

            # Оставляем в списке деталей только найденные рекомендации
            ids = ids[ids.isin(answer["consequents"])]

            # Приводим ответ в соответствие с отфильтрованным результатом
            # (исключаем из рекомендаций не подходящие TypeId и неизвестные детали)
            answer = answer[answer["consequents"].isin(ids)]

            # Вычисляем метрику
            answer["metric"] = answer["consequent support"] * 0.45 + answer["confidence"] * 0.55
            answer = answer.sort_values(by="metric", ascending=False)

            # Удаляем из ответа повторения по id в итоговом предикте
            try:
                answer = answer[~answer["consequents"].isin(predictions["consequents"])]
            except:
                pass

            # Берем верхние строки
            answer = answer[:math.ceil(25 / len(parts))]

            # Добавляем ответ для одного элемента в общие результаты
            predictions = pd.concat([predictions, answer], axis=0)

        predictions = predictions.sort_values(by="metric", ascending=False) \
                                    .head(25)["consequents"] \
                                    .sort_values()
        return list(predictions)
