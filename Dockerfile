FROM python:3.10.12

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# копируем и автоматически выполняем запросы по созданию схем
# COPY ./src/scripts/sql/ddl.sql /docker-entrypoint-initdb.d/
COPY . .

CMD [ "python3", "src/1.py" ]
