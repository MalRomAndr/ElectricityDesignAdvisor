import joblib
import pandas as pd
import sqlite3


# Импорт данных из базы данных
conn = sqlite3.connect('../../FastAPI/database.db')
data_Parts = pd.read_sql("select Id, Name, HeadingId, CategoryId from Parts;", con=conn)
data_StructuresParts = pd.read_sql("select StructureId, PartId from StructuresParts;", con=conn)
data_Structures = pd.read_sql("select Id, StandardProjectId, TypeId from Structures;", con=conn)
data_StandardProjects = pd.read_sql("select Id, ImageIndex from StandardProjects;", con=conn)
data_Conductors = pd.read_sql("select PartId, TypeId, Diameter, CrossSection from Conductors;", con=conn)

# Объединение таблиц
df = data_Parts.merge(data_StructuresParts, left_on='Id', right_on='PartId', how='outer').drop('PartId', axis=1)
df = df.merge(data_Structures, left_on='StructureId', right_on='Id', how='outer').drop('Id_y', axis=1)
data_Conductors.rename(columns={'PartId': 'Id_x'}, inplace=True)
df = pd.concat([df, data_Conductors], axis=0)
df = df.merge(data_StandardProjects, left_on='StandardProjectId', right_on='Id', how='left').drop('Id', axis=1)


# Обработка пропусков и форматирование значений
df['StructureId'] = df['StructureId'].str.split('_').str[0]
df['ImageIndex'] = df['ImageIndex'] + 1

df[['Name', 'StructureId', 'TypeId']] = df[['Name', 'StructureId', 'TypeId']].fillna('')
df = df.fillna(0)

# Удаление дубликатов
df = df.drop_duplicates().reset_index(drop=True)

# Приведение типов данных в соответствие
df[['HeadingId', 'CategoryId', 'StandardProjectId', 'ImageIndex']] = \
    df[['HeadingId', 'CategoryId', 'StandardProjectId', 'ImageIndex']].astype('int32')

joblib.dump(df, 'knn_df.pkl')
