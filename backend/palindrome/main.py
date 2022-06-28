from random import randint
from google.cloud import storage
import sqlalchemy
from sqlalchemy.sql import select
from time import sleep

WAIT_TIME = 0.2

def palindrome(content, incoming_file):
    # num_palindrome: amount of palindromes in this chunk
    # longest_len: length of the longest palindrome in this chunk

    longest_len = 0
    count = 0
    palindrome = []
    lines = []

    lines = content.decode('utf-8').split("\n")
    # print("lines len: ", len(lines))
    for line in lines:
        words = line.split() #split the lowercased line into words
        for w in words:
            if w == w[::-1]:
                count += 1
                palindrome.append(w)
        count += sum(w == w[::-1] for w in words)
    if len(palindrome) > 0:
        longest_len = len(max(palindrome, key=len, default=0))
    num_palindrome = len(palindrome)
    # print("num_palindrome: ", num_palindrome, "  longest_len: ", longest_len)

    update_sql(num_palindrome, longest_len, incoming_file)


def update_sql(num_palindrome, longest_len, incoming_file):
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

        num_palindrome_finished = 0
        for i in palindrome_status_result:
            # print("elements in palindrome_status_result: ", str(i))
            if '1' in str(i):
                num_palindrome_finished += 1
        # print("num_palindrome_finished: ", num_palindrome_finished)
        chunk_num_result = chunk_num_query[0]['Chunk_Num']
        # print("chunk_num_result: ", chunk_num_result)
        if int(chunk_num_result) - num_palindrome_finished == 1:
            last_worker = True
            # print("I'm the last palindrome worker for ", actual_file_name)
            # count the last worker as completed
            num_palindrome_finished += 1

        # update chunks
        Chunk_ID = incoming_file[:incoming_file.index(".txt")]
        try:
            query_str = 'UPDATE chunks SET Palindrome_Done = 1, Palindrome_Num = {}, Longest_Len = {}  WHERE Chunk_ID="{}"'.format(str(num_palindrome), str(longest_len), str(Chunk_ID))
            with db.connect() as conn:
                conn.execute(sqlalchemy.text(query_str))
        except Exception as e:
            print(str(e))
            return 'Error on SQL Update: {}'.format(str(e))

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

        num_total_finished = 0
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

        if last_worker:
            try:
                query_str = 'UPDATE tasks SET Palindrome_Longest = (SELECT MAX(Longest_Len) FROM chunks WHERE File = "{}") WHERE File = "{}";'.format(str(actual_file_name), str(actual_file_name))
                with db.connect() as conn:
                    conn.execute(sqlalchemy.text(query_str))
            except Exception as e:
                print(str(e))
                return 'Error on SQL Update: {}'.format(str(e))

            try:
                query_str = 'UPDATE tasks SET Palindrome_Num = (SELECT SUM(Palindrome_Num) FROM chunks WHERE File = "{}") WHERE File = "{}";'.format(str(actual_file_name), str(actual_file_name))
                with db.connect() as conn:
                    conn.execute(sqlalchemy.text(query_str))
            except Exception as e:
                print(str(e))
                return 'Error on SQL Update: {}'.format(str(e))


def fetch(incoming_file):
    bucket_name = "chunk-bucket"
    blob_name = incoming_file
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_string()
    # print("content type: ", type(content))
    palindrome(content, incoming_file)


def hello_pubsub(event, context):
    incoming_file = ""
    if 'attributes' in event:
        name = str(event['attributes']['objectId'])
    else:
        name = ""
    incoming_file = name
    # print("incoming_file: ", incoming_file)

    if incoming_file != "":
        fetch(incoming_file)
    else:
        raise Exception('Error! Incoming file does not exist or bad network')
