# prefect flow goes here
import requests
import boto3
import time
import pandas as pd
from prefect import flow, task




task
def get_queue_attributes(queue_url):
    try:
        sqs = boto3.client('sqs')
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['All']
        )
        Messages = int(response['Attributes']['ApproximateNumberOfMessages'])
        Incoming = int(response['Attributes']['ApproximateNumberOfMessagesDelayed'])
        print(f"Available Messages: {Messages}, Incoming Messages: {Incoming}")
        return Messages, Incoming


    except Exception as e:
        print(f"Error getting queue attributes: {e}")
        raise e
 
def delete_message(queue_url, receipt_handle):
    try:
        sqs = boto3.client('sqs')
        sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=receipt_handle)
    except Exception as e:
        print(f"Error deleting message: {e}")
        raise e


def get_message(queue_url):
    try:
        sqs = boto3.client('sqs')
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MessageSystemAttributeNames=['All'],
            MaxNumberOfMessages=1,
            VisibilityTimeout=60,
            MessageAttributeNames=['All'],
            WaitTimeSeconds=10
        )
        if 'Messages' in response:
            num = int(response['Messages'][0]['MessageAttributes']['order_no']['StringValue'])
            print(f"Recieved {num}")
            word = response['Messages'][0]['MessageAttributes']['word']['StringValue']
            print(f"Message is {word}")
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            delete_message(queue_url, receipt_handle)
            return num, word
        else:
            return None


    except Exception as e:
        print(f"Error getting message: {e}")
        raise e


@task
def collect_messages(queue_url):
    messages = []
    max_seconds = 900
    start_time = time.time()


    while len(messages) < 21:
        time_elapsed = time.time() - start_time
        if time_elapsed > max_seconds:
            print("waited too long")
            break


        available, delayed = get_queue_attributes(queue_url)
        if available > 0:
            while True:
                msg = get_message(queue_url)
                if not msg:
                    break
                messages.append(msg)
                print(f"collected {len(messages)}")
        elif delayed > 0:
            print(f"{delayed} still delayed")
        else:
            print("no messages")
        time.sleep(30)
    return messages


@task
def sort_messages(messages):
    if not messages:
        print("No messages")
        return None
    df = pd.DataFrame(messages, columns=['order_no', 'word'])
    df = df.sort_values(by=['order_no'])
    phrase = " ".join(df['word'].tolist())
    print(phrase)
    return phrase


@task
def send_solution(uvaid, phrase, platform):
    sqs = boto3.client('sqs')
    url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    try:
        response = sqs.send_message(
            QueueUrl=url,
            MessageBody="submission",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        print(f"Response: {response}")
    except Exception as e:
        print(f"Error sending message: {e}")
        raise e


@flow
def data_pipeline():
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mge9dn"


    payload = requests.post(url).json()
    queue_url = payload['sqs_url']


    messages = collect_messages(queue_url)
    phrase = sort_messages(messages)
    send_solution("mge9dn", phrase, "prefect")


if __name__ == "__main__":
    data_pipeline()
