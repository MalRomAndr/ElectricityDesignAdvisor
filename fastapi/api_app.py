import pandas as pd
import numpy as np
import uvicorn
import joblib

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List

# Load Processed Data for KNN
df = pd.read_csv('data_040923.csv')
# Load KNN model
knn_model = joblib.load("knn_model")
# Load fitted decoder
decoder = joblib.load('decoder_strId_040923')

# Instantiate FastAPI app
app = FastAPI()

# Create Pydantic BaseModels
class PartItem(BaseModel):
    Id_x: str
    StructureId: str

class JsonData(BaseModel):
    structure_type: str
    structure_id: str
    parts: List[PartItem]

@app.post("/")
async def predict(data: JsonData):
    structure_type = data.structure_type
    parts = data.parts
    parts_list = [{"Id_x": part.Id_x, "StructureId": part.StructureId} for part in parts]

    # Get the lists of "Id_x" and "StructureId" values from parts_list
    # Encode "StructureId" with previously fitted encoder
    idxs = [part['Id_x'] for part in parts_list]
    st_ids = decoder.transform([part['StructureId'].split('_')[0] for part in parts_list])

    # Filter the data for rows in question
    query = df[(df['Id_x'].isin(idxs)) & (df['StructureId'].isin(st_ids))]

    # Get the recommendations with KNN
    results = pd.DataFrame()
    for _ in range(query.shape[0]):
        element = query.iloc[_].to_frame().T.drop(columns=['Id_x', 'Name'])
        answer = knn_model.kneighbors(element)
        result = df.loc[answer[1][0]]
        result['distance'] = answer[0][0]
        result = result[~result['Id_x'].isin(query['Id_x'])]
        result = result.drop_duplicates(subset=['Id_x'])
        result = result[:int(np.ceil(25 / query.shape[0] + 1))]
        results = pd.concat([results, result], axis=0)

        results = results.drop_duplicates(subset=['Id_x']).sort_values(by='distance').head(25).sort_values(by='Name')

    # Transform results DataFrame into dictionary
    results_dict = results.to_dict(orient='records')

    # Return the recommendations
    return {"predictions": results_dict}

if __name__ == '__main__':
    uvicorn.run(app, host="127.0.0.1", port=8000)

# @app.get('/message')
# async def api_hello_api():
#     return {"message": "This is my first API response"}
#%%
