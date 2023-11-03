import joblib
import sys
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

# Добавляем в системные пути папку с классом модели (fpgrowth.py),
# чтобы потом она успешно загрузилась:
sys.path.append("../shared/model")

# Грузим модель:
model = joblib.load('../shared/model/model')

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
