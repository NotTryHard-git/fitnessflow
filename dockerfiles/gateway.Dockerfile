FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/*.proto .
RUN for proto in *.proto; do python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. $proto; done

COPY gateway.py .

EXPOSE 8080

CMD ["python", "gateway.py"]