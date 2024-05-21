
crontab -l > mycron

echo "0 11 * * * /usr/local/bin/python /app/main.py >> /var/log/cron.log 2>&1" >> mycron
echo "0 17 * * * /usr/local/bin/python /app/main.py >> /var/log/cron.log 2>&1" >> mycron

crontab mycron
rm mycron

cron -f
