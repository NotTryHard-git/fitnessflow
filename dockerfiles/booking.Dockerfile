FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/booking.proto .
COPY proto/user.proto .
COPY proto/schedule.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. booking.proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. user.proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. schedule.proto

COPY service/booking.py .

EXPOSE 50053

CMD ["python", "booking.py"]