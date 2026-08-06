FROM python:3.12-alpine

# tzdata 提供 zoneinfo 数据，supervisor.py 用它计算 Asia/Shanghai 时间
RUN apk add --no-cache tzdata

WORKDIR /app
COPY lingowhale2rss.py supervisor.py ./

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["python", "supervisor.py"]
