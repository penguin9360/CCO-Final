from google.cloud import storage
import sqlalchemy
from sqlalchemy.sql import select
import os
import multiprocessing
from multiprocessing import Process
import subprocess
from subprocess import check_output

from flask import Flask, render_template, request

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 250 * 1024 * 1024 # max 250MB
CHUNK_SIZE_DEFAULT = 5 * 1024 * 1024 # chunk size is 5 MB by default
MP = False

# SQL config stuff
connection_name = "cco-final:europe-west4:cco-db"
db_password = "cco"
db_name = "attribute"
db_user = "root"
driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

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


@app.route('/', methods=['GET','POST', 'PUT'])
def root():
    return render_template('index.html', msg=[''])


@app.route('/file_upload', methods=['GET','POST', 'PUT'])
def upload_init():
    global FILE_NAME
    global FILE_SIZE
    file = request.files.get('file')

    if file is None or str(file) == "<FileStorage: '' ('application/octet-stream')>":
        return render_template('index.html', msg=["Please Choose a file"])

    FILE_NAME = str(file)[15:str(file).index("' (")]
    # FILE_NAME = file.filename

    try:
        file.save("/tmp/tmp.txt")
        tmp_file = open("/tmp/tmp.txt", "r")
        FILE_SIZE = os.stat("/tmp/tmp.txt").st_size
        content_type = file.content_type
        update_database()
        try:
            file_string = tmp_file.read()
            # debug
            # render_template('index.html', msg=["File content: ", file_string[:min(50, len(file_string) - 1)]])
            if file_string is None:
                return render_template('index.html', msg=["File string is None: ", str(e)])
            chunk(file_string, FILE_NAME)
            notify_merge(content_type)

        except Exception as e:
            return render_template('index.html', msg=["File read failed: ", str(e)])
        os.remove("/tmp/tmp.txt")
    except Exception as e:
        return render_template('index.html', msg=["Error creating a temp file: ", str(e)])
    # print("File Name: ", str(file), "   FILE_NAME: ", FILE_NAME, "   File Size: ", FILE_SIZE)

    return render_template('index.html', msg=["Upload Successful"])


def chunk(content, incoming_file):
    global chunk_num
    chunked_content = []
    chunk_size = CHUNK_SIZE_DEFAULT
    chunk_num = 0
    # dummy chunk for testing
    # chunked_content = [content[:int(len(content) / 2)], content[int(len(content) / 2):]]
    # chunk_num = 2

    # the read-from-file approach
    # while True:
    #     chunk = file.read(chunk_size)
    #     if not chunk:
    #         break
    #     yield chunk
    #     chunk_num += 1
    #     print("chunk_num: ", chunk_num, "  chunk_content: ", chunk[:20])
    #     chunked_content.append(chunk)

    # read from memory
    i = 0
    while i < len(content):
        if i + chunk_size >= len(content) - 1:
            chunked_content.append(content[i:len(content) - 1])
            chunk_num += 1
            break
        else:
            chunked_content.append(content[i:i+chunk_size])
            # print("chunk_num: ", chunk_num + 1, "  chunk_content: ", content[i:i + 20])
            i += (chunk_size + 1)
        chunk_num += 1

    upload(chunked_content, incoming_file)


def upload(chunked_content, incoming_file):
    chunk_index = 0

    if MP:
        # # The MP approach - works with small files on GAE; won't throw 413

        pool = multiprocessing.Pool()
        for i in range(0, len(chunked_content)):
            pool.apply_async(multi_process_upload, args=(chunked_content[i], incoming_file, i))
        pool.close()
        pool.join()
    else:
        # The old single threaded appraoch - works with small files but throws 413 on large files

        for content in chunked_content:
            #chunk_name = incoming_file[:-4]+"-chunked-"+str(randint(0, 1000000))
            chunk_name = incoming_file[:-4]+"-chunked-"+str(chunk_index)
            newfile = chunk_name + ".txt"
            client = storage.Client()
            bucket = client.get_bucket("chunk-bucket")
            blob = bucket.blob(newfile)
            try:
                blob.upload_from_string(
                    content
                )
            except Exception as e:
                return render_template('index.html', msg=["Upload Failed, ", str(e)])
            update_chunk_sql(chunk_name, incoming_file)
            chunk_index += 1

        # # The shell approach - not working on GAE but works locally

        # for i in range(0, len(chunked_content)):
        #     chunk_name = incoming_file[:-4]+"-chunked-"+str(i)
        #     with open("/tmp/"+chunk_name+".txt", "w+") as chunk_tmp:
        #         chunk_tmp.write(chunked_content[i])
        #     print("current path: ", check_output(["pwd"]))
        #     print(subprocess.check_output(["gsutil", "cp", "/tmp/"+chunk_name+".txt", "gs://chunk-bucket"]))
        #     update_chunk_sql(chunk_name, incoming_file)
        #
        # for j in range(0, len(chunked_content)):
        #     tmp_chunk = incoming_file[:-4]+"-chunked-"+str(j)
        #     os.remove("/tmp/"+tmp_chunk+".txt")

        # # # The new-script approach - working on GAE only with small files
        #
        # for i in range(0, len(chunked_content)):
        #     chunk_name = incoming_file[:-4]+"-chunked-"+str(i)
        #     with open("/tmp/"+chunk_name+".txt", "w+") as chunk_tmp:
        #         chunk_tmp.write(chunked_content[i])
        #     print("current path: ", check_output(["pwd"]))
        #     print(subprocess.call(["python3", "uploader.py", chunk_name+".txt"]))
        #     update_chunk_sql(chunk_name, incoming_file)
        #
        # for j in range(0, len(chunked_content)):
        #     tmp_chunk = incoming_file[:-4]+"-chunked-"+str(j)
        #     os.remove("/tmp/"+tmp_chunk+".txt")

def multi_process_upload(content, incoming_file, i):
        #chunk_name = incoming_file[:-4]+"-chunked-"+str(randint(0, 1000000))
        chunk_index = i
        chunk_name = incoming_file[:-4]+"-chunked-"+str(chunk_index)
        newfile = chunk_name + ".txt"
        client = storage.Client()
        bucket = client.get_bucket("chunk-bucket")
        blob = bucket.blob(newfile)
        try:
            blob.upload_from_string(
                content
            )
        except Exception as e:
            return render_template('index.html', msg=["Upload Failed, ", str(e)])
        update_chunk_sql(chunk_name, incoming_file)


def update_chunk_sql(chunk_name, incoming_file):
    # print("File: ", incoming_file, "  chunkID: ", chunk_name)

    # write chunk info into chunks table
    try:
        query_str = 'INSERT INTO chunks (File, Chunk_ID, Sort_Done, Palindrome_Done, Palindrome_Num, Longest_Len) VALUES ("{}", "{}", 0, 0, 0, 0)'.format(str(incoming_file), str(chunk_name))
        with db.connect() as conn:
            conn.execute(sqlalchemy.text(query_str))
    except Exception as e:
        print(str(e))
        return 'Error on SQL Update: {}'.format(str(e))

    # update the main "tasks" table with the chunk number and initialize everything else
    # update tasks set Chunk_Num = 2, Progress = "0/2", Palindrome_Num = 0, Palindrome_Longest = 0 where File = "dummy1.txt"
    try:
        query_str = 'UPDATE tasks SET Chunk_Num = {}, Progress = "0/{}", Palindrome_Num = 0, Palindrome_Longest = 0 WHERE File = "{}"'.format(str(chunk_num), str(chunk_num * 2), str(incoming_file))
        with db.connect() as conn:
            conn.execute(sqlalchemy.text(query_str))
    except Exception as e:
        print(str(e))
        return 'Error on SQL Update: {}'.format(str(e))


def update_database():
    try:
        query_str = 'INSERT INTO tasks (File, File_Size) VALUES ("{}", "{}")'.format(str(FILE_NAME), str(FILE_SIZE))
        with db.connect() as conn:
            show_labels = sqlalchemy.text(query_str)
            conn.execute(show_labels)
    except Exception as e:
        print(str(e))
        return 'Error on SQL Update: {}'.format(str(e))


def notify_merge(content_type):
    try:
        client = storage.Client()
        bucket = client.get_bucket("cco-final")
        blob = bucket.blob(FILE_NAME)
        blob.upload_from_string(
            FILE_NAME,
            content_type=content_type
        )
    except Exception as e:
        return render_template('index.html', msg=["Notification to merge Failed with exception: ", str(e)])


@app.route('/job_attribute', methods=['GET','POST', 'PUT'])
def job_attribute():
    # labels = ['dummy1', 'dummy2', 'dummy3', 'dummy4', 'dummy5']
    # content = [['0', '1', '2', '3', '4'], ['0', '1', '2', '3', '4'], ['0', '1', '2', '3', '4']]

    # reference: https://codelabs.developers.google.com/codelabs/connecting-to-cloud-sql-with-cloud-functions#2
    labels = []
    content = []
    try:
        with db.connect() as conn:
            show_labels = sqlalchemy.text('SHOW FIELDS FROM tasks')
            labels = conn.execute(show_labels)
            labels = [l[0] for l in labels]
            # print("labels: ", labels)
    except Exception as e:
        print(str(e))
        return 'Error: {}'.format(str(e))

    try:
        with db.connect() as conn:
            show_tasks = select(sqlalchemy.text('* from tasks'))
            content = conn.execute(show_tasks)
            # print("content: ", content)
    except Exception as e:
        print(str(e))
        return 'Error: {}'.format(str(e))
    return render_template('job_attribute.html', labels=labels, content=content)


@app.errorhandler(413)
def handle_error(e):
    return render_template('index.html', msg=[str(e)]), 413

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host='127.0.0.1', port=8884, debug=True)
