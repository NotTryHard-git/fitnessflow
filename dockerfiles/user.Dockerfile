FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/user.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. user.proto

COPY service/user.py .

EXPOSE 50052

CMD ["python", "user.py"]