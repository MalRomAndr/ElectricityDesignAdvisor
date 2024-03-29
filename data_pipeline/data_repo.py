import pandas as pd
import numpy as np
import sqlite3
import os
import requests
import json


class DataRepo:
    def __init__(self):
        self.data_folder = "data/"

    def __check_db_composition(self, conn) -> bool:
        """
        Валидация структуры БД
        """
        cursor = conn.cursor()
        db_composition = dict(
            Parts=["Id", "Name"],
            StructuresParts=["StructureId", "PartId"],
            Structures=["Id", "TypeId"],
            Conductors=["PartId", "TypeId"]
        )
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        if set([row[0] for row in cursor.fetchall()]) >= set(db_composition.keys()):
            n = []
            for t in db_composition.keys():
                cursor.execute(f"PRAGMA table_info({t})")
                if set([row[1] for row in cursor.fetchall()]) >= set(db_composition[t]):
                    n.append(1)
            if sum(n) == len(db_composition.keys()):
                cursor.close()
                return True
            else:
                cursor.close()
                return False
        else:
            cursor.close()
            return False

    def create_df(self, path: str) -> pd.DataFrame:
        """
        Создать датафрейм
        """
        if os.path.isfile(path):
            conn = sqlite3.connect(path)
        else:
            raise FileNotFoundError("Path to database file is incorrect")

        if not self.__check_db_composition(conn):
            conn.close()
            raise ValueError("Database file have a wrong composition")

        # Подгружаем таблицы из sql
        data_parts = pd.read_sql("SELECT Id, Name, SupplierId FROM Parts;", con=conn)
        data_structures_parts = pd.read_sql("SELECT StructureId, PartId FROM StructuresParts;", con=conn)
        data_structures = pd.read_sql("SELECT Id, TypeId FROM Structures;", con=conn)
        data_conductors = pd.read_sql("SELECT PartId, TypeId FROM Conductors;", con=conn)
        conn.close()

        # Объединяем в датафрейм
        df = data_parts.merge(
            data_structures_parts,
            left_on="Id",
            right_on="PartId",
            how="outer"
        ).drop("PartId", axis=1)

        data_structures.rename(columns={"Id": "Id_str"}, inplace=True)

        df = df.merge(
            data_structures,
            left_on="StructureId",
            right_on="Id_str"
        ).drop(labels="Id_str", axis=1)

        data_conductors.rename(columns={"PartId": "Id"}, inplace=True)

        df = pd.concat([df, data_conductors], axis=0)

        # Отсекаем лишнее в наименованиях опор
        df["StructureId"] = df["StructureId"].str.split("_").str[0]

        # Заполняем пропуски в данных
        df[["Name", "StructureId", "SupplierId"]] = \
            df[["Name", "StructureId", "SupplierId"]].fillna("")

        # Удаляем строки дубликаты
        df = df.drop_duplicates().reset_index(drop=True)
        return df


    def create_dataset_from_db(self, path: str) -> pd.DataFrame:
        """
        Создать датафрейм из БД
        """
        df: pd.DataFrame = self.create_df(path)
        transactions: pd.DataFrame = df.pivot_table(
            index=["StructureId"],
            aggfunc=pd.Series.unique,
            values=["Id", "TypeId"]).reset_index(drop=False).drop(0, axis=0)
        transactions["TypeId"] = transactions["TypeId"].apply(lambda x: x[0])
        transactions["Id"] = transactions["Id"].apply(lambda x: tuple(x))
        transactions = transactions.drop_duplicates().reset_index(drop=True)
        return transactions


    def create_dataset_via_api(self, path: str) -> pd.DataFrame:
        """
        Создать датафрейм из грейлога
        """
        # Доступ к Graylog
        params = {
            "query": "message:'BOM created' AND level:6",
            "timerange": "1M",
            "fields": "bom",
            "size": 9999
        }
        headers = {"Accept": "application/json"}
        token = os.getenv("GRAYLOG_TOKEN")
        auth = (token, "token")
        # Получение ответа от API
        response = requests.get(
            path,
            headers=headers,
            auth=auth,
            params=params,
            timeout=10
        ).json()
        # Преобразование в список транзакций
        transactions: pd.DataFrame = pd.DataFrame(columns=["StructureId", "Id", "TypeId"])
        for bom in range(len(response["datarows"])):
            transactions = pd.concat(
                (
                    transactions,
                    pd.DataFrame(
                        json.loads(response["datarows"][bom][0])["structures"][:]).rename(
                        columns={
                            "structure_name": "StructureId",
                            "parts": "Id",
                            "structure_type": "TypeId"
                        }
                    )
                ), axis=0
            )
        transactions = transactions[transactions["Id"].apply(lambda x: np.array(x).size > 0)]
        transactions["Id"] = transactions["Id"].apply(lambda x: tuple(x))
        transactions = transactions.drop_duplicates().reset_index(drop=True)
        return transactions
