"""Email sending functionality."""
import smtplib
import ssl
from email.mime.text import MIMEText
from config.settings import SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASS, FROM_EMAIL, TO_EMAIL


def send_email(subject: str, body: str):
    """Send an email notification."""
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = FROM_EMAIL
    msg['To'] = TO_EMAIL

    context = ssl.create_default_context()  # For better SSL handling

    try:
        if SMTP_PORT == 465:
            # Use SMTP_SSL for port 465 (SSL)
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(FROM_EMAIL, TO_EMAIL, msg.as_string())
        else:
            # Use SMTP with STARTTLS for port 587 or 25
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls(context=context)
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(FROM_EMAIL, TO_EMAIL, msg.as_string())
    except Exception as e:
        from helpers.logger import logger
        logger.error(f"Error sending email: {e}")

