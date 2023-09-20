import joblib
import pandas as pd
import numpy as np

from fastapi import FastAPI, Query
from enum import Enum


df = joblib.load('df.pkl')
encoder_struct = joblib.load('encoder_struct.pkl')
encoder_type = joblib.load('encoder_type.pkl')
scaler = joblib.load('scaler.pkl')
model = joblib.load('model.pkl')




structures = df['StructureId'].unique()
class ItemEnum(str, Enum):
    Struecture0 = structures[np.random.randint(0, 2268, 1)][0]
    Struecture1 = structures[np.random.randint(0, 2268, 1)][0]
    Struecture2 = structures[np.random.randint(0, 2268, 1)][0]
    Struecture3 = structures[np.random.randint(0, 2268, 1)][0]
    Struecture4 = structures[np.random.randint(0, 2268, 1)][0]
    Struecture5 = structures[np.random.randint(0, 2268, 1)][0]
    Struecture6 = structures[np.random.randint(0, 2268, 1)][0]
    Struecture7 = structures[np.random.randint(0, 2268, 1)][0]
    Struecture8 = structures[np.random.randint(0, 2268, 1)][0]
    Struecture9 = structures[np.random.randint(0, 2268, 1)][0]
    item1 = 'А11'


app = FastAPI()
@app.get("/")
async def request(user_request: ItemEnum = Query(None, description="Выберите наименование Опоры из списка")):

    #encoded_request = encoder_struct.transform([user_request])[0]
    request = df[df['StructureId'] == user_request]

    results = pd.DataFrame()
    for _ in range(request.shape[0]):
        element = request.iloc[[_]].drop(columns=['Id_x', 'Name']) #берем один элемент из запроса
        element['StructureId'] = encoder_struct.transform(element['StructureId'])
        element['TypeId'] = encoder_type.transform(element['TypeId'])
        element = scaler.transform(element)
        answer = model.kneighbors(element) #находим соседей
        result = df.loc[answer[1][0]] #по индексу выбираем элементы из основного датафрейма
        result['distance'] = answer[0][0] #добавляем столбец с расстоянием
        result = result[~result['Id_x'].isin(request['Id_x'])] #удаялем те элементы, которые присутствуют в запросе
        try:
            result = result[~result['Id_x'].isin(results['Id_x'])] #удаляем те элементы, которые уже есть в финальном результате
        except:
            pass
        result = result.drop_duplicates(subset=['Id_x']) #удаляем дубли по индексу
        result = result[:int(np.ceil(25 / request.shape[0]))] #оставляем только верхние элементы
        results = pd.concat([results, result], axis=0) #присоединяем к финальному результату результат по одному элементу

    results = results.sort_values(by='distance').head(25).sort_values(by='Name')

    request_rows = request[['Id_x', 'Name']].apply(lambda x: f"{x['Id_x']} - {x['Name']}", axis=1)
    results_rows = results[['Id_x', 'Name', 'distance']].apply(lambda x: f"{x['Id_x']} - {x['Name']} ({x['distance']})", axis=1)

    return {'В составе выбранной опоры следующие элементы': list(request_rows),
            'Вам могут подойти эти элементы': list(results_rows)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
