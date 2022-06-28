from google.cloud import storage
import sqlalchemy
from sqlalchemy.sql import select
from time import sleep

def merge_sort(incoming_file, content_list):
    content_list.sort()
    file_string = ""
    for i in content_list:
        file_string += str(i)
    # for i in range(0, 10):
    #     print("content_list: ", content_list[i])
    upload(file_string, incoming_file)


def check_sql(incoming_file):
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

    while True:
        # get Chunk_Num from tasks
        try:
            with db.connect() as conn:
                query_str = 'Chunk_Num from tasks where File = "{}"'.format(str(incoming_file))
                get_chunk_num = select(sqlalchemy.text(query_str))
                chunk_num_query = conn.execute(get_chunk_num).mappings().all()
                # print("chunk num query: ", chunk_num_query)
        except Exception as e:
            print(str(e))
            return 'Error on SQL Query: {}'.format(str(e))
        # get number of sort workers who are done
        try:
            with db.connect() as conn:
                query_str = 'Sort_Done from chunks where File = "{}"'.format(str(incoming_file))
                get_sort_status = select(sqlalchemy.text(query_str))
                sort_status_result = conn.execute(get_sort_status).mappings().all()
                # print("Sort Worker Status: ", sort_status_result)
        except Exception as e:
            print(str(e))
            return 'Error on SQL Query: {}'.format(str(e))

        num_finished = 0
        for i in sort_status_result:
            # print("elements in sort_status_result: ", str(i))
            if '1' in str(i):
                num_finished += 1
        print("num_finished: ", num_finished)
        chunk_num_result = chunk_num_query[0]['Chunk_Num']
        print("chunk_num_result: ", chunk_num_result)
        if int(chunk_num_result) == num_finished:
            print("All sort workers finished for ", incoming_file)
            break
        sleep(0.5)

    # get all chunks
    chunk_list = []
    try:
        with db.connect() as conn:
            query_str = 'Chunk_ID from chunks where File="{}"'.format(str(incoming_file))
            get_chunk_list = select(sqlalchemy.text(query_str))
            # add delay to data fetch to make sure concurrent write attempts finish
            sleep(0.4)
            chunk_list_result = conn.execute(get_chunk_list).mappings().all()
            # print("chunk_list_result: ", chunk_list_result)
    except Exception as e:
        print(str(e))
        return 'Error on SQL Query: {}'.format(str(e))

    if len(chunk_list_result) != 0:
        for i in chunk_list_result:
            chunk_list.append(str(i['Chunk_ID']))
            # print("chunk_list: ", chunk_list)
    else:
        print ("Error! Chunk list empty!")

    fetch(incoming_file, chunk_list)


def fetch(incoming_file, chunk_list):
    bucket_name = "sort-bucket"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    content_list = []

    for i in chunk_list:
        blob_name = i + "-sorted.txt"
        blob = bucket.blob(blob_name)
        content = blob.download_as_string()
        content_list.append(content.decode('utf-8'))

    merge_sort(incoming_file, content_list)


def upload(content, incoming_file):
    newfile = incoming_file[:-4]+"-final.txt"
    client = storage.Client()
    bucket = client.get_bucket("final-result-bucket")
    blob = bucket.blob(newfile)
    try:
        blob.upload_from_string(
            content
        )
    except Exception as e:
        return "Error during upload: " + str(e)

def hello_pubsub(event, context):
    incoming_file = ""
    if 'attributes' in event:
        name = str(event['attributes']['objectId'])
    else:
        name = ""
    incoming_file = name
    print("incoming_file: ", incoming_file)

    if incoming_file != "":
        check_sql(incoming_file)
    else:
        raise Exception('Error! Incoming file does not exist or bad network')
