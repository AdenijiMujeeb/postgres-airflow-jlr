import smtplib
import logging
from email.mime.text import MIMEText
from airflow.providers.postgres.hooks.postgres import PostgresHook


def check_data_quality(conn_id):

    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    # Define columns to check for null values
    columns = ['gen_id', 'sales_price']

    # Check for null values in each column
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for col in columns:
                cur.execute(f"SELECT COUNT(*) FROM transformed_data.profit_view WHERE {col} IS NULL")
                null_count = cur.fetchone()[0]
                if null_count > 0:
                    message = f"Data quality check failed: {null_count} null values found in column {col}"
                    logging.info(message)
                    send_email_notification(message)
                    raise ValueError(message)
                
    # Log the success message
    logging.info("Data quality check successful")

def send_email_notification(message):
    # Set up SMTP connection to email server
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login('naimatoyewale@gmail.com', 'oyedoyin')

    # Set up email message
    msg = MIMEText(message)
    msg['Subject'] = 'Data Quality Check Failed'
    msg['From'] = 'naimatoyewale@gmail.com'
    msg['To'] = 'adenijimujeeb@gmail.com'

    # Send email message
    server.sendmail('naimatoyewale@gmail.com', ['adenijimujeeb@gmail.com'], msg.as_string())
    print("Sent email: ", message)

    # Close SMTP connection
    server.quit()