from random import randint
from google.cloud import storage

def merge(content, incoming_file):
    print("merge")
    # do actual work below
    mergeed_content = content
    upload(mergeed_content, incoming_file)


def fetch(incoming_file):
    # @TODO: need to fetch a series of files within the same task
    bucket_name = "sort-bucket"
    blob_name = incoming_file
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_string()
    merge(content, incoming_file)


def upload(content, incoming_file):
    newfile = incoming_file[:-4]+"-final-.txt"
    client = storage.Client()
    bucket = client.get_bucket("final-result-bucket")
    blob = bucket.blob(newfile)
    try:
        blob.upload_from_string(
            content
        )
    except Exception as e:
        return "Error: " + str(e)

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
