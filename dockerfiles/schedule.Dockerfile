FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/schedule.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. schedule.proto

COPY service/schedule.py .

EXPOSE 50051

CMD ["python", "schedule.py"]