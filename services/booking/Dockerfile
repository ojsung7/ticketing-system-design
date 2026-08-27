# build context = repo root. api 와 worker 가 이 이미지를 공유한다(command 로 구분).
FROM python:3.12-slim
WORKDIR /code
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY services/booking ./services/booking
EXPOSE 8002
CMD ["uvicorn", "services.booking.main:app", "--host", "0.0.0.0", "--port", "8002"]
