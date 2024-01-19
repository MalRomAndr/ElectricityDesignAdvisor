"""
API для рекомендаций
"""
import sys
import joblib
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder

# Добавляем в системные пути папку с классом модели (fpgrowth.py),
# чтобы потом она успешно загрузилась:
sys.path.append("../shared/model")

# Грузим модель:
model = joblib.load("../shared/model/model")

app = FastAPI()

class Item(BaseModel):
    """
    DTO запроса
    """
    structure_type: str # Тип сборочной единицы (опора 10 кВ, опора 0,4 кВ и т.д.)
    structure_id: str   # Имя сборочной единицы
    parts: list         # Список Id деталей, из которых состоит сборочная единица


@app.post("/api/")
async def predict(item: Item):
    """
    Получить рекомендации
    """
    request = jsonable_encoder(item)
    predictions = model.predict(request)
    return {"predictions": predictions}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8008)
