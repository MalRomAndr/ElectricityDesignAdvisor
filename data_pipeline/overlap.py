import numpy as np
from sklearn.model_selection import KFold


class Overlap:
    def __init__(self):
        self.request = None
        self.model = None

    def check_overlap(self, request, model):
        kf = KFold(n_splits=3, shuffle=True, random_state=42)
        overlap = []
        parts = np.array(request["parts"])
        type_id = request["structure_type"]
        for check_index, req_index in kf.split(parts):
            # Передаем в запрос 1 фолд
            request_part = {
                "parts": list(parts[req_index]),
                "structure_type": type_id
            }
            # Элементы, которых нет в запросе - ожидаемый ответ
            actual = parts[check_index]
            # Получаем рекомендации
            predictions = model.predict(request_part)
            # Находим общие элементы в ответе и в ожидаемом ответе
            common_elements = set(actual) & set(predictions)
            overlap.append(len(common_elements) / len(actual))

        return np.mean(overlap)
