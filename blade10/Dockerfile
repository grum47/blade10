FROM python:3.9-slim

WORKDIR /usr/src/blade10

COPY blade10/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY blade10/ ./

CMD ["python", "blade10.py"]
