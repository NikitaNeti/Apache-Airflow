import imaplib
import email
import csv

IMAP_SERVER = 'imap.gmail.com'
EMAIL_ADDRESS = 'nikitaneti21@gmail.com'
EMAIL_PASSWORD = 'xtjwgqgphjfpsnlm'

def connect_to_email():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    mail.select('inbox')  # You can choose a different mailbox if required
    return mail

def search_emails(mail, sender, subject):
    data = mail.search(None, f'(FROM "{sender}")', f'(SUBJECT "{subject}")')
    email_ids = data[0].split()
    return email_ids

