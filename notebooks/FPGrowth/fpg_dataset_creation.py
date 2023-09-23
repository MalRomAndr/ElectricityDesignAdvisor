import pandas as pd
import sqlite3
import pickle




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


with open('fpg_df.pickle', 'wb') as file:
    pickle.dump(df, file)
