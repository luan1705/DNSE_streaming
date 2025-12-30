FROM python:3.10-slim

# Cài đặt timezone Việt Nam
RUN apt-get update && apt-get install -y tzdata \
    && ln -snf /usr/share/zoneinfo/Asia/Ho_Chi_Minh /etc/localtime \
    && echo "Asia/Ho_Chi_Minh" > /etc/timezone \
    && apt-get clean

# Set working directory
WORKDIR /app

# Copy toàn bộ project (cả eboard_table và List)
COPY . /app

# Cài dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Thêm PYTHONPATH để import được List/
ENV PYTHONPATH=/app

# Chạy app (dùng absolute import trong streaming1.py)
#CMD ["python3", "eboard_table/eboard_table_hose1.py"]
