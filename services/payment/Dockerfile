# build context = repo root
FROM python:3.12-slim
WORKDIR /code
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY services/payment ./services/payment
EXPOSE 8003
CMD ["uvicorn", "services.payment.main:app", "--host", "0.0.0.0", "--port", "8003"]
