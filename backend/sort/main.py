from random import randint
from google.cloud import storage
import sqlalchemy
from sqlalchemy.sql import select
from time import sleep

WAIT_TIME = 0.2
debug_lines = 10

def sort(content, incoming_file):
    sorted_content = ""
    # lines_ori = content.decode('utf-8').split("\n")
    # lines = lines_ori.copy()
    lines = content.decode('utf-8').split("\n")
    lines.sort()
    lines = list(filter(None, lines))
    for i in range(0, len(lines)):
        sorted_content += (lines[i] + "\n")

    # for i in range(0, debug_lines):
    #     print("Original str: ", lines_ori[i], "    | sorted str: ", lines[i])
    upload(sorted_content, incoming_file)


def check_and_update_sql(incoming_file):
    last_worker = False
    actual_file_name = incoming_file[:incoming_file.index("-chunked-")]+".txt"
    # print("actual_file_name: ", actual_file_name)

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

    # check if the current worker is the last one in the queue

    # get Chunk_Num from tasks
    try:
        with db.connect() as conn:
            query_str = 'Chunk_Num from tasks where File = "{}"'.format(str(actual_file_name))
            get_chunk_num = select(sqlalchemy.text(query_str))
            # add delay to data fetch to make sure concurrent write attempts finish
            sleep(WAIT_TIME)
            chunk_num_query = conn.execute(get_chunk_num).mappings().all()
            # print("chunk num query: ", chunk_num_query)
    except Exception as e:
        print(str(e))
        return 'Error on SQL Query: {}'.format(str(e))

    # get number of sort workers who are done
    try:
        with db.connect() as conn:
            query_str = 'Sort_Done from chunks where File = "{}"'.format(str(actual_file_name))
            get_sort_status = select(sqlalchemy.text(query_str))
            # add delay to data fetch to make sure concurrent write attempts finish
            sleep(WAIT_TIME)
            sort_status_result = conn.execute(get_sort_status).mappings().all()
            # print("Sort Worker Status: ", sort_status_result)
    except Exception as e:
        print(str(e))
        return 'Error on SQL Query: {}'.format(str(e))

    # get number of palindrome workers who are done
    try:
        with db.connect() as conn:
            query_str = 'Palindrome_Done from chunks where File = "{}"'.format(str(actual_file_name))
            get_palindrome_status = select(sqlalchemy.text(query_str))
            # add delay to data fetch to make sure concurrent write attempts finish
            sleep(WAIT_TIME)
            palindrome_status_result = conn.execute(get_palindrome_status).mappings().all()
            # print("Sort Worker Status: ", palindrome_status_result)
    except Exception as e:
        print(str(e))
        return 'Error on SQL Query: {}'.format(str(e))

    num_sort_finished = 0
    for i in sort_status_result:
        # print("elements in sort_status_result: ", str(i))
        if '1' in str(i):
            num_sort_finished += 1
    # print("num_sort_finished: ", num_sort_finished)
    chunk_num_result = chunk_num_query[0]['Chunk_Num']
    # print("chunk_num_result: ", chunk_num_result)
    if int(chunk_num_result) - num_sort_finished == 1:
        last_worker = True
        print("I'm the last sort worker for ", actual_file_name)
        # count the last worker as completed
        num_sort_finished += 1

    # update chunks
    Chunk_ID = incoming_file[:incoming_file.index(".txt")]
    try:
        query_str = 'UPDATE chunks SET Sort_Done = 1 WHERE Chunk_ID="{}"'.format(str(Chunk_ID))
        with db.connect() as conn:
            conn.execute(sqlalchemy.text(query_str))
    except Exception as e:
        print(str(e))
        return 'Error on SQL Update: {}'.format(str(e))

    num_total_finished = 0
    # get number of sort workers who are done
    try:
        with db.connect() as conn:
            query_str = 'Sort_Done from chunks where File = "{}"'.format(str(actual_file_name))
            get_sort_status = select(sqlalchemy.text(query_str))
            # add delay to data fetch to make sure concurrent write attempts finish
            sleep(WAIT_TIME)
            sort_status_result = conn.execute(get_sort_status).mappings().all()
            # print("Sort Worker Status: ", sort_status_result)
    except Exception as e:
        print(str(e))
        return 'Error on SQL Query: {}'.format(str(e))

    for i in range(0, chunk_num_result):
        # print("elements in sort_status_result: ", str(i))
        if '1' in str(sort_status_result[i]) and '1' in str(palindrome_status_result[i]):
            num_total_finished += 1

    try:
        query_str = 'UPDATE tasks SET Progress = "{}/{}" WHERE File = "{}"'.format(str(num_total_finished), str(int(chunk_num_result)), str(actual_file_name))
        with db.connect() as conn:
            conn.execute(sqlalchemy.text(query_str))
    except Exception as e:
        print(str(e))
        return 'Error on SQL Update: {}'.format(str(e))


    # if last_worker:
    #     trigger_merge()



# def trigger_merge():
#     # might need to create a new bucket and pass actual_file_name as txt file to trigger merge
#     print("Last file! Triggering merge!")



def fetch(incoming_file):
    bucket_name = "chunk-bucket"
    blob_name = incoming_file
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_string()
    sort(content, incoming_file)


def upload(content, incoming_file):
    newfile = incoming_file[:-4]+"-sorted.txt"
    client = storage.Client()
    bucket = client.get_bucket("sort-bucket")
    blob = bucket.blob(newfile)
    try:
        blob.upload_from_string(
            content
        )
    except Exception as e:
        return "Error: " + str(e)
    check_and_update_sql(incoming_file)

def hello_pubsub(event, context):
    incoming_file = ""
    if 'attributes' in event:
        name = str(event['attributes']['objectId'])
    else:
        name = ""
    incoming_file = name
    print("incoming_file: ", incoming_file)

    if incoming_file != "":
        fetch(incoming_file)
    else:
        raise Exception('Error! Incoming file does not exist or bad network')
