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
    proto/booking.proto

COPY service/booking.py .

EXPOSE 50053

CMD ["python", "booking.py"]