## PySpark Batch Pipeline With Postgres As Source And Sink, Airflow and Alerts

### Pipeline Flow:
1. Airflow trigger job everyday.
2. Source data from postgres tables with date=today filter.
3. Calculating related metrics: [Educational DB](https://github.com/redb17/designing-educational-database-postgres).
4. Writing metrics to postgres as sink.
5. Email alert when job finishes.

### Email Alerts Settings In airflow.cfg
```conf
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = gmail_username
smtp_password = gmail_app_password
smtp_port = 587
smtp_mail_from = email_sender@example.com
smtp_timeout = 30
smtp_retry_limit = 5
```

