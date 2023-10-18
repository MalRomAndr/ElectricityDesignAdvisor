import joblib
import sys
import os
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
model_path = os.path.dirname(os.path.abspath('../model/fpgrowth.py'))
sys.path.append(model_path)


model = joblib.load('../model/data/model')

app = FastAPI()


class Item(BaseModel):
    structure_type: str
    structure_id: str
    parts: list


@app.post("/api/")
async def predict(item: Item):
    request = jsonable_encoder(item)
    predictions = model.predict(request)
    return dict(predictions=predictions)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8008)
