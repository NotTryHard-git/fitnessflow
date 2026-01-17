FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Генерация gRPC кода
COPY proto/ proto/
RUN python -m grpc_tools.protoc \
    -I./proto \
    --python_out=. \
    --grpc_python_out=. \
    proto/schedule.proto

COPY service/schedule.py .

EXPOSE 50051

CMD ["python", "schedule.py"]