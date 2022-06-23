from random import randint
from google.cloud import storage

def chunk(content, incoming_file):
    print("chunk")
    # @TODO: refine chunk function
    chunked_content = [content[:len(content) / 2], content[len(content / 2):]]
    upload(chunked_content, incoming_file)


def fetch(incoming_file):
    bucket_name = "cco-final"
    blob_name = incoming_file
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_string()
    chunk(content, incoming_file)


def update_sql(chunk_name, incoming_file):
    print("File: ", incoming_file, "  chunkID: ", chunk_name)
    # @TODO: update sql


def upload(chunked_content, incoming_file):
    for content in chunked_content:
        chunk_name = incoming_file[:-4]+"-chunked-"+str(randint(0, 1000))
        newfile = chunk_name + ".txt"
        client = storage.Client()
        bucket = client.get_bucket("chunk-bucket")
        blob = bucket.blob(newfile)
        try:
            blob.upload_from_string(
                content
            )
        except Exception as e:
            return "Error: " + str(e)
        update_sql(chunk_name, incoming_file)

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
