FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/notification.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. notification.proto

COPY service/notification.py .

EXPOSE 50055

CMD ["python", "notification.py"]