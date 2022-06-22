from google.cloud import storage
import sqlalchemy
from sqlalchemy.sql import select
import os

from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/', methods=['GET','POST', 'PUT'])
def root():
    return render_template('index.html', msg=[''])


@app.route('/file_upload', methods=['GET','POST', 'PUT'])
def upload():
    global FILE_NAME
    global FILE_SIZE
    file = request.files.get('file')
    FILE_NAME = str(file)[15:str(file).index("' (")]
    file.save("/tmp/tmp.txt")
    FILE_SIZE = os.stat("/tmp/tmp.txt").st_size
    os.remove("/tmp/tmp.txt")
    print("File Name: ", str(file), "   FILE_NAME: ", FILE_NAME, "   File Size: ", FILE_SIZE)
    if file is None or str(file) == "<FileStorage: '' ('application/octet-stream')>":
        return render_template('index.html', msg=["Please Choose a file"])
    client = storage.Client()
    bucket = client.get_bucket("cco-final")
    blob = bucket.blob(file.filename)
    try:
        blob.upload_from_string(
            file.read(),
            content_type=file.content_type
        )
        # get file pool_size
        # reference: https://stackoverflow.com/questions/15772975/flask-get-the-size-of-request-files-object
        # If using disk :
        # request.files['file'].save("/tmp/foo")
        # size = os.stat("/tmp/foo").st_size
        # # If not using disk :
        # blob = request.files['file'].read()
        # size = len(blob)
    except Exception as e:
        return render_template('index.html', msg=["Upload Failed with exception: ", str(e)])
    update_database()
    return render_template('index.html', msg=["Upload Successful"])

def update_database():
    connection_name = "cco-final:europe-west4:cco-db"
    db_password = "cco"
    db_name = "attribute"
    db_user = "root"
    driver_name = 'mysql+pymysql'
    query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})
    stmt = sqlalchemy.text('SELECT * FROM tasks')

    db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
        drivername=driver_name,
        username=db_user,
        password=db_password,
        database=db_name,
        query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )
    try:
        query_str = 'INSERT INTO tasks (File, File_Size, IF_started) VALUES ({}, {}, {})'.format(str(FILE_NAME), str(FILE_SIZE), "0")
        with db.connect() as conn:
            show_labels = sqlalchemy.text(query_str)
            conn.execute(show_labels)
    except Exception as e:
        print(str(e))
        return 'Error on SQL Update: {}'.format(str(e))

@app.route('/job_attribute', methods=['GET','POST', 'PUT'])
def job_attribute():
    # labels = ['dummy1', 'dummy2', 'dummy3', 'dummy4', 'dummy5']
    # content = [['0', '1', '2', '3', '4'], ['0', '1', '2', '3', '4'], ['0', '1', '2', '3', '4']]

    # reference: https://codelabs.developers.google.com/codelabs/connecting-to-cloud-sql-with-cloud-functions#2
    connection_name = "cco-final:europe-west4:cco-db"
    db_password = "cco"
    db_name = "attribute"
    db_user = "root"
    driver_name = 'mysql+pymysql'
    query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})
    stmt = sqlalchemy.text('SELECT * FROM tasks')

    db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
        drivername=driver_name,
        username=db_user,
        password=db_password,
        database=db_name,
        query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )

    labels = []
    content = []
    try:
        with db.connect() as conn:
            show_labels = sqlalchemy.text('SHOW FIELDS FROM tasks')
            labels = conn.execute(show_labels)
            labels = [l[0] for l in labels]
            print("labels: ", labels)
    except Exception as e:
        print(str(e))
        return 'Error: {}'.format(str(e))

    try:
        with db.connect() as conn:
            show_tasks = select(sqlalchemy.text('* from tasks'))
            content = conn.execute(show_tasks)
            print("content: ", content)
    except Exception as e:
        print(str(e))
        return 'Error: {}'.format(str(e))
    return render_template('job_attribute.html', labels=labels, content=content)


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
