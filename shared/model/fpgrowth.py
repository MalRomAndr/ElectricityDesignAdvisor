"""
Модуль для подсчета ассоциативных правил для программы SmartLine
"""
import math
import pandas as pd
from mlxtend.frequent_patterns import fpgrowth, association_rules
from mlxtend.preprocessing import TransactionEncoder


class FPGrowthRecommender:
    """
    Класс для подсчета ассоциативных правил, алгоритм fpgrowth
    """
    def __init__(self, df: str):
        """
        Parameters
        ----------
        df: DataFrame
            Вспомогательный датафрейм для фильтрации правил по типу опоры
            и удаления ненужных деталей

        Вид DataFrame:  | Id | Name | SupplierId | StructureId | TypeId |
        """
        self.rules = None
        self.df = df

        if not isinstance(df, pd.DataFrame):
            raise TypeError

        # Id производителей-конкурентов
        self.competitors = ["EKF", "Niled", "Энсто", "ООО «МЗВА-ЧЭМЗ»", "IEK", "SICAME"]

    def fit(self, dataset):
        """
        Подсчитать associations rules

        Parameters
        ----------
        dataset: Таблица транзакций

        | StructureId | Id                   |
        | ----------- | -------------------- |
        | П20-3Н      | (N 640, P 616, P 72) |
        | ППоБ10-3Н   | ("d10", "COT37R")    |
        """
        te = TransactionEncoder()
        encoded = pd.DataFrame(te.fit(dataset.Id).transform(dataset.Id), columns=te.columns_)
        frequent_itemsets = fpgrowth(encoded, min_support=0.0001, use_colnames=True, max_len=2)
        rules = association_rules(frequent_itemsets, metric="support", min_threshold=0)

        # Конвертируем frozenset в строку:
        rules["antecedents"] = rules["antecedents"].apply(lambda x: list(x)[0])
        rules["consequents"] = rules["consequents"].apply(lambda x: list(x)[0])

        self.rules = rules


    def predict(self, request):
        """
        Найти в правилах подходящие детали

        Parameters
        ----------
        request: Запрос json

        Пример запроса:

        {
            "structure_type": "support",
            "parts": [
                "ЗП6",
                "П-3и",
                "СВ95-3",
                "У4",
                "COT36.2R"
            ],
            "structure_name" (не используется): "А11"
        }

        Returns
        -----------
        Список Id рекомендованных деталей
        """
        type_id = request["structure_type"]
        parts = request["parts"]

        # Список производителей в запросе:
        suppliers_from_request = self.df[self.df.Id.isin(parts)].SupplierId.unique()

        # Если из списка конкурентов удалить тех, чья продукция есть в запросе,
        # то из оставшихся получится в черный список - их продукцию надо скрыть.
        suppliers_black_list = [i for i in self.competitors if i not in suppliers_from_request]

        # Если в запосе нет ни одного из конкурентов, то разрешаем всех:
        if len(suppliers_black_list) == len(self.competitors):
            suppliers_black_list = []

        # Составляем список Id деталей, которые соответствуют по TypeId и производителю:
        ids = self.df.loc[
            (self.df.TypeId == type_id) &
            ~self.df.SupplierId.isin(suppliers_black_list)
            ].Id.unique()

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

            # Приводим ответ в соответствие с отфильтрованным результатом
            # (исключаем из рекомендаций не подходящие TypeId и неизвестные детали)
            answer = answer[answer["consequents"].isin(ids)]

            # Вычисляем метрику
            answer["metric"] = answer["consequent support"] * 0.45 + answer["confidence"] * 0.55
            answer = answer.sort_values(by="metric", ascending=False)

            # Удаляем из ответа повторения по id в итоговом предикте
            try:
                answer = answer[~answer["consequents"].isin(predictions["consequents"])]
            except KeyError:
                pass

            # Берем верхние строки
            answer = answer[:math.ceil(25 / len(parts))]

            # Добавляем ответ для одного элемента в общие результаты
            predictions = pd.concat([predictions, answer], axis=0)

        predictions = predictions.sort_values(by="metric", ascending=False) \
                                    .head(25)["consequents"] \
                                    .sort_values()
        return list(predictions)
