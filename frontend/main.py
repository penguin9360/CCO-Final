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
    file = request.files.get('file')
    print("File Name: ", str(file))
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
    except Exception as e:
        return render_template('index.html', msg=["Upload Failed with exception: ", str(e)])

    return render_template('index.html', msg=["Upload Successful"])

@app.route('/job_attribute', methods=['GET','POST', 'PUT'])
def job_attribute():
    # labels = ['dummy1', 'dummy2', 'dummy3', 'dummy4', 'dummy5']
    # content = [['0', '1', '2', '3', '4'], ['0', '1', '2', '3', '4'], ['0', '1', '2', '3', '4']]

    # reference: https://codelabs.developers.google.com/codelabs/connecting-to-cloud-sql-with-cloud-functions#2
    connection_name = "cco-final:europe-west4:cco-db"
    db_password = "cco"
    db_name = "CCO_db"
    db_user = "root"
    driver_name = 'mysql+pymysql'
    query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})
    stmt = sqlalchemy.text('SELECT * FROM info')

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
            s0 = sqlalchemy.text('SHOW FIELDS FROM info')
            labels = conn.execute(s0)
            labels = [l[0] for l in labels]
            print("labels: ", labels)
    except Exception as e:
        print(str(e))
        return 'Error: {}'.format(str(e))

    try:
        with db.connect() as conn:
            s1 = select(sqlalchemy.text('* from info'))
            content = conn.execute(s1)
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
