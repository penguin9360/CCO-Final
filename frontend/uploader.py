from google.cloud import storage
import sqlalchemy
from sqlalchemy.sql import select
import os
import sys
import multiprocessing
from multiprocessing import Process
import subprocess
from subprocess import check_output

from flask import Flask, render_template, request

def upload(argv):
    print("argv[1]: ", str(argv[1]))
    storage_client = storage.Client()
    bucket = storage_client.bucket("chunk-bucket")
    blob = bucket.blob(str(argv[1]))
    blob.upload_from_filename("/tmp/"+str(argv[1]))

if __name__ == "__main__":
    upload(sys.argv)
