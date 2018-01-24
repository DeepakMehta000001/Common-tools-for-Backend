#pip install celery-with-redis==3.0
#command to run - celery worker --loglevel=info -A worker --beat
#function - Continuous task worker using Celery and Redis

import datetime
from celery import Celery
from celery import group
import variables
import requests
app = Celery('worker', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')




@app.task
def add_days(days):
    # Import smtplib for the actual sending function
    import smtplib
    import MySQLdb
    from email.mime.text import MIMEText

    db = MySQLdb.connect(host=variables.MYSQL_DIRECTORY_HOST,    # your host, usually localhost
                         user=variables.MYSQL_DIRECTORY_USER,         # your username
                         passwd=variables.MYSQL_DIRECTORY_PASSWORD,  # your password
                         db=variables.MYSQL_DIRECTORY_DB,  # name of the data base
                         )

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)

    # Use all the SQL you like
    cur.execute("SELECT * FROM url_queue where exe=0 order by ID desc")

    #cur.execute("SELECT * FROM user limit 10")

    # print all the first cell of all the rows
    result = cur.fetchall()
    if result:
        for row in result:
            url = row['url']
            params = row['params']
            data = row['data']
            method = row['method']
            if method=='get':
                response = requests.get(url)
                if response.status_code==200:
                    return "Success"
                else:
                    return "Failure"
            #cur.execute('UPDATE url_queue SET exe=1 WHERE id=' + str(row['id']))
    else:
        print('No pending tasks')

    db.commit()
    db.close()
    #return datetime.datetime.now()+datetime.timedelta(days=days)

app.conf.update(
    CELERYBEAT_SCHEDULE = {
        'multiply-each-10-seconds' : {
            'task': 'worker.add_days',
            'schedule': datetime.timedelta(seconds=10),
            'args': (2,)
        }
    }
)
