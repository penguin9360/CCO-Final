from random import randint
from google.cloud import storage

def palindrome(content, incoming_file):
    # num_palindrome: amount of palindromes in this chunk
    # longest_len: length of the longest palindrome in this chunk
    print("palindrome")
    # @TODO: add palindrome function

    num_palindrome = 0
    longest_len = 0
    update_sql(num_palindrome, longest_len, incoming_file)


def update_sql(num_palindrome, longest_len, incoming_file):
    # @TODO: add SQL function
    print("Upload palindrome result to sql")

def fetch(incoming_file):
    bucket_name = "chunk-bucket"
    blob_name = incoming_file
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_string()
    palindrome(content, incoming_file)


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
