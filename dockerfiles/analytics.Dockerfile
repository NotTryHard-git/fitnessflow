FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/analytics.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. analytics.proto

COPY service/analytics.py .

EXPOSE 50056

CMD ["python", "analytics.py"]