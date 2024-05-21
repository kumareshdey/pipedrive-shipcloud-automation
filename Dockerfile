FROM python:3.10

WORKDIR /app

COPY . /app

RUN apt-get update && apt-get install -y cron

RUN pip install --no-cache-dir -r requirements.txt

COPY scheduler.sh /app/scheduler.sh
RUN chmod +x /app/scheduler.sh

RUN /app/scheduler.sh

CMD ["cron", "-f"]
