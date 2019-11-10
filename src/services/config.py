import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

def accessKey():
    os.environ.get("AWS_ACCESS_KEY")

def secretKey():
    os.environ.get("AWS_SECRET_ACCESS_KEY")

def region():
    os.environ.get("AWS_REGION")

