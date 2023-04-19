import smtplib
import csv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication



def send_mail(cdate):
    # Define sender and recipient email addresses
    sender_email = 'xagent.amber@gmail.com'
    recipient_email = 'mohammed.musthafa.dev@gmail.com'

    passwd = 'jpiupfucjtflepsd'

    # Create message container
    msg = MIMEMultipart()
    msg['Subject'] = 'Scraped Business Data from - AWS'

    # Attach CSV file to email
    with open('Data/' + cdate + '.csv', 'rb') as file:
        attachment = MIMEApplication(file.read(), _subtype='csv')
        attachment.add_header('Content-Disposition', 'attachment', filename='data.csv')
        msg.attach(attachment)

    # Connect to SMTP server and send email
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(sender_email, passwd) # Replace 'password' with your actual password
        server.sendmail(sender_email, recipient_email, msg.as_string())

