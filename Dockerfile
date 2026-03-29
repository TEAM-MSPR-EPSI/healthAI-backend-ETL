FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY etl.py etl_ingredient.py etl_exercise.py etl_load.py api.py ./

EXPOSE 8000

CMD ["python", "api.py"]