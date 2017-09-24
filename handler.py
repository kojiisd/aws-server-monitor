import json
import boto3
import sys
import os
from boto3.dynamodb.conditions import Key, Attr

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'lib'))

import requests

BUCKET_NAME = os.environ['BUCKET_NAME']
OBJECT_NAME = os.environ['OBJECT_NAME']
SNS_TOPICS_NAME = os.environ['SNS_TOPICS_NAME']
DDB_TABLE_NAME = os.environ['DDB_TABLE_NAME']

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    if len(BUCKET_NAME) == 0 or len(OBJECT_NAME) == 0 or len(SNS_TOPICS_NAME) == 0 or len(DDB_TABLE_NAME) == 0:
        print("Please input BUCKT NAME, OBJECT NAME, SNS_TOPICS_NAME and DDB_TABLE_NAME")
        sys.exit()
    target_json = get_target_servers()
    check_target_servers(target_json)

def get_target_servers():
    s3 = boto3.resource('s3')
    obj = s3.Object(BUCKET_NAME, OBJECT_NAME)
    response = obj.get()
    body = response['Body'].read()
    return body.decode('utf-8')

def check_target_servers(target_json):
    data = json.loads(target_json)
    servers = data['servers']

    error_servers = []

    for server in servers:
        name = server['name']
        url = server['url']
        try:
            res = requests.get(url)
            if res.status_code != 200:
                error_servers.append(server)
        except Exception:
            error_servers.append(server)

    if len(error_servers) == 0:
        print("Successful finished servers checking")
    else:
        response = send_error(name, url, error_servers)
        print("Error occured:")
        print(response)
        print(error_servers)

def send_error(name, url, error_servers):
    sns = boto3.client('sns')
    sns_message = "Error happens:\n\n" + json.dumps(error_servers, indent=4, separators=(',', ': '))

    subject = '[ServerMonitor] Error happens'
    response = sns.publish(
        TopicArn=SNS_TOPICS_NAME,
        Message=sns_message,
        Subject=subject
    )

    return response

def check_status(url, name):
    status_ok = True
    items = dynamodb.Table(DDB_TABLE_NAME).get_item(
            Key={
                 "url": "http://www.google.com",
                 "name": "google"
            }
        )

    status_ok = items['Item']['status']
    return status_ok

def add_server(url, name, status):
    dynamodb.Table(DDB_TABLE_NAME).put_item(
        Item={
                "url": url,
                "name": name,
                "status": status
        }
    )
