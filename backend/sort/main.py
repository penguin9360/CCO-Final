from random import randint
from google.cloud import storage

def sort(content, incoming_file):
    # do actual work below
    sorted_content = content
    upload(sorted_content, incoming_file)


def check_and_update_sql():
    # @TODO: update SQL when one worker finishes
    print("check if I'm the last worker and update my progress to SQL")


def fetch(incoming_file):
    bucket_name = "chunk-bucket"
    blob_name = incoming_file
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_string()
    sort(content, incoming_file)


def upload(content, incoming_file):
    newfile = incoming_file[:-4]+"-sorted-"+str(randint(0, 1000))+".txt"
    client = storage.Client()
    bucket = client.get_bucket("sort-bucket")
    blob = bucket.blob(newfile)
    try:
        blob.upload_from_string(
            content
        )
    except Exception as e:
        return "Error: " + str(e)
    check_and_update_sql()

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
