# Используем официальный образ Python
FROM python:3.12

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файлы зависимостей и requirements.txt в контейнер
COPY requirements.txt .

# Устанавливаем зависимости с использованием pip
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все файлы проекта в контейнер
COPY api_app.py .

# Указываем команду для запуска FastAPI приложения
CMD ["uvicorn", "api_app:app", "--host", "0.0.0.0", "--port", "8000"]
