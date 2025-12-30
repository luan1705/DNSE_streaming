# db_writer/Dockerfile
FROM python:3.10-slim

ENV TZ=Asia/Ho_Chi_Minh

RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
  && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
  && echo $TZ > /etc/timezone \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# copy requirements trước để tận dụng cache
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r /app/requirements.txt

# copy code (an toàn hơn nếu sau này db_writer.py import thêm file khác)
COPY . /app

CMD ["python3", "-u", "/app/db_writer.py"]
