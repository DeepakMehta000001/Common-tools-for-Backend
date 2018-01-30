import codecs
import pickle
import requests
import time

def url_task(method,url,data='',params='',headers=''):
    new_url_task = UrlQueue(url=url,method=method,params=codecs.encode(pickle.dumps(params), "base64").decode(),headers=codecs.encode(pickle.dumps(headers), "base64").decode(),
                            data=codecs.encode(pickle.dumps(data), "base64").decode(),time=datetime.now())
    db.session.add(new_url_task)
    db.session.flush()
    db.session.commit()

#This is runner function kept in some file to import into code. and URL hitting through a cron. 


def hit_url():
    import MySQLdb
    db = MySQLdb.connect(host=variables.MYSQL_DIRECTORY_HOST,    # your host, usually localhost
                         user=variables.MYSQL_DIRECTORY_USER,         # your username
                         passwd=variables.MYSQL_DIRECTORY_PASSWORD,  # your password
                         db=variables.MYSQL_DIRECTORY_DB,  # name of the data base
                         )

    cur = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM url_queue where exe=0 order by ID desc")
    result = cur.fetchall()
    if result:
        for row in result:
            success = False
            url = row['url']
            params = pickle.loads(codecs.decode(row['params'].encode(), "base64"))
            data = pickle.loads(codecs.decode(row['data'].encode(), "base64"))
            headers = pickle.loads(codecs.decode(row['headers'].encode(), "base64"))
            method = row['method']
            try:
                if method=='get':
                    response = requests.get(url,params=params,headers=headers)
                    if response.status_code==200:
                        success = True
                    else:
                        raise Exception("status code = "+str(response.status_code))

                if method== 'post':
                    response = requests.post(url,json=data,headers=headers)
                    if response.status_code==200:
                        success = True
                        print( "Success")
                    else:
                        raise Exception("status code = "+str(response.status_code))
                if success:
                    cur.execute('UPDATE url_queue SET exe=1 WHERE id=' + str(row['id']))
                else:
                    cur.execute('UPDATE url_queue SET '+ 'error= " '+ str(response.status_code) +' " , exe=1 WHERE id=' + str(row['id']))
            except Exception as e:
                error_message = str(e)
                cur.execute('UPDATE url_queue SET exe=1 WHERE id=' + str(row['id']))
                cur.execute("UPDATE url_queue SET error="+'"'+error_message+'"'+" WHERE id=" + str(row['id']))
    else:
        print('No pending tasks')

    db.commit()
    db.close()

if __name__ == '__main__':
    while True:
        time.sleep(10)
        hit_url()


#---------------------Table-----------------------------------------

class UrlQueue(db.Model):
    __tablename__ = "url_queue"
    id = db.Column('id', db.Integer, primary_key=True, nullable=False)
    url = db.Column('url', db.String(300), nullable=False)
    error = db.Column('error', db.String(300), nullable=True)
    method = db.Column('method', db.String(7), nullable=False)
    headers = db.Column('headers', db.String(500))
    params = db.Column('params', db.String(500))
    data = db.Column('data', db.String(1000))
    exe = db.Column('exe',db.SmallInteger(),default=0)
    time = db.Column('time', db.DateTime,nullable=False)
