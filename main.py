import email, getpass, imaplib, os, sys
from dateparser.search import search_dates
import datetime
from dateutil import parser
from datetime import datetime, timedelta
import os
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import re
import cx_Oracle
import time


df1 = pd.DataFrame()

path = 'File Path'

folder = os.fsencode(path)

filenames = []
files = []

for file in os.listdir(folder):
    filename = os.fsdecode(file)
    if filename.endswith(('.xlsx')):  # whatever file types you're using...
        filenames.append(filename)
        files.append(filename[16:-5])

detach_dir = '.'

# user = input("Gmail username: ")
user = "##provide gmail ID##"
# pwd = getpass.getpass("Password: ")
pwd = '##provide your password##'
sender_email = 'insert sender email'

# Connecting to the gmail imap server
m = imaplib.IMAP4_SSL("imap.gmail.com")
m.login(user, pwd)

# Select the mailbox
m.select('"[Gmail]/All Mail"')

today = datetime.today()
# cutoff = today - timedelta(days=2)
dt = today.strftime('%d-%b-%Y')

resp, items = m.search(None, '(SINCE %s) (FROM "Sender Email")' % (dt,))
items = items[0].split()

counter = -1

extracted_dates = []

for emailid in items:
    resp, data = m.fetch(emailid, "(RFC822)")
    email_body = data[0][1]
    mail = email.message_from_bytes(email_body)

    dates = search_dates(mail['Received'])

    dt = dates[0][1].date()

    if str(dt) in files:
        continue

    else:
        if mail.get_content_maintype() != 'multipart':
            continue

        subject = ""

        if mail["subject"] is not None:
            subject = mail["subject"]

        print("[" + mail["From"] + "] :" + subject)

        if dates is not None:
            for d in dates:
                d1 = d[1].date()
                print(d1)

                if ('File Name' + str(d1) + '.xlsx' in filenames):
                    print('Pre Fetched File')
                    continue

                else:
                    if (str(d1) in extracted_dates):
                        continue

                    else:
                        extracted_dates.append(str(d1))
                        for part in mail.walk():
                            if part.get_content_maintype() == 'multipart':
                                continue

                            if part.get('Content-Disposition') is None:
                                continue

                            filename = part.get_filename()
                            counter += 1

                            counter = str(counter)

                            att_path = os.path.join(detach_dir, filename)

                            if not os.path.isfile(att_path):
                                fp = open(att_path, 'wb')
                                fp.write(part.get_payload(decode=True))
                                fp.close()
                                count = int(counter)
                                print(count)
                                os.rename('' + filename, 'File Name' + extracted_dates[count] + '.xlsx')
                                df = pd.read_excel(r'File Name' + extracted_dates[count] + '.xlsx',
                                                   engine='openpyxl', skiprows=[0, 1])
                                df.drop(['col1'], axis=1)
                                df['col2'].fillna(method='ffill', inplace=True)
                                df['col3'].fillna(method='ffill', inplace=True)
                                df['col4'] = extracted_dates[count]
                                df.rename({'column_name:new_column'}, axis=1, inplace=True)
                                df = df.astype(
                                    {'col5': str, 'col6': str, 'col7': str})
                                df = df.dropna(
                                    subset=['col5', 'col6', 'col7', "col8"])
                                df1 = df1.append(df, ignore_index=True)

                            print('<<<<>>>>>')

                            counter = int(counter)


        else:
            extracted_dates.append('None')

# if file does not exist write header
if not os.path.isfile('File.csv'):
    df1.to_csv('File.csv', encoding='utf-8')

else:  # else it exists so append without writing the header

    df1.to_csv('File.csv', encoding='utf-8', header=False)

print(df1)

os.chdir("vpn batch file path")
os.startfile("connect_vpn.bat")
print('vpn started')

time.sleep(10)

os.chdir("Insert Path")

conn = create_engine('oracle:engine').connect()

dff = pd.read_csv('file.csv', encoding='ISO-8859-1', header=None)
dff = dff.drop([0], axis=1)

dff.columns = ['col1', 'col2', 'col3', 'col4', 'col5', 'col6',
               'col7', 'col8', 'col9']

dff.to_sql(name='tablename', con=conn, if_exists='append', index=False)

dfff = pd.read_sql('SELECT * FROM tablename', conn)
print(dfff)

time.sleep(20)

os.chdir("vpn batch file path")
os.startfile("disconnect_vpn.bat")
print('vpn ended')