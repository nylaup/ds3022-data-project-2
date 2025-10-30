# prefect flow goes here
#start with imports 
import requests
import boto3
import time
import pandas as pd
from prefect import flow, task, get_run_logger


@task
def get_queue_attributes(queue_url):
    #Gets attributes from queue and returns how many messages are available and how many are delayed
    logger = get_run_logger()
    try:
        sqs = boto3.client('sqs')
        response = sqs.get_queue_attributes( #use sqs to get attributes 
            QueueUrl=queue_url,
            AttributeNames=['All']
        )
        #create integer variables for number of messages and messages delayed
        Messages = int(response['Attributes']['ApproximateNumberOfMessages'])
        Incoming = int(response['Attributes']['ApproximateNumberOfMessagesDelayed'])
        logger.info(f"Available Messages: {Messages}, Incoming Messages: {Incoming}") #log
        return Messages, Incoming

    except Exception as e:
        logger.info(f"Error getting queue attributes: {e}")
        raise e
 
def delete_message(queue_url, receipt_handle):
    #delete messages once they have been read using receipt handle
    try:
        sqs = boto3.client('sqs')
        sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=receipt_handle)
    except Exception as e:
        print(f"Error deleting message: {e}")
        raise e

def get_message(queue_url):
    #function to get messages from queue and return their order number and phrase 
    logger = get_run_logger() 
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
            #collect number (as int) and message phrase 
            num = int(response['Messages'][0]['MessageAttributes']['order_no']['StringValue'])
            word = response['Messages'][0]['MessageAttributes']['word']['StringValue']
            logger.info(f"Order No. {num} Message is: {word}") #log 
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            delete_message(queue_url, receipt_handle) #delete messages once read 
            return num, word
        else: #if no messages then don't do anything 
            return None

    except Exception as e:
        logger.info(f"Error getting message: {e}")
        raise e

@task
def collect_messages(queue_url):
    #Task flow that checks for available messages and checks if available 
    messages = [] #empty list to store messages 
    start_time = time.time()
    logger = get_run_logger() 

    while len(messages) < 21: #exits loop if 21 messages collected 
        time_elapsed = time.time() - start_time #start timer 
        if time_elapsed > 900: #exit if waiting more than 900s
            logger.info("waited too long")
            break

        available, delayed = get_queue_attributes(queue_url) #get # of messages to process
        if available > 0: #start processing if available messages
            while True: 
                msg = get_message(queue_url) #call get message function and store output
                if not msg: #exit if no message 
                    break
                messages.append(msg) #append to list
                logger.info(f"collected {len(messages)}")
        elif delayed > 0: #log how many still incoming 
            logger.info(f"{delayed} still delayed")
        else:
            logger.info("no messages")
        time.sleep(30) #wait 30s before checking again
    return messages #return messages list once all collected 

@task
def sort_messages(messages):
    #task to compile messages into dataframe, sort, then compile into phrase 
    logger = get_run_logger()
    if not messages: # exit if error and messages list empty 
        logger.info("No messages")
        return None
    #create dataframe of messages with their number
    df = pd.DataFrame(messages, columns=['order_no', 'word'])  
    df = df.sort_values(by=['order_no']) #sort dataframe 
    #compile word column into list, then join into phrase separated by " "
    phrase = " ".join(df['word'].tolist()) 
    logger.info(f"The final phrase is: {phrase}")
    return phrase

@task
def send_solution(uvaid, phrase, platform):
    #Task to send compiled phrase with platform and student id to given url  
    logger = get_run_logger()
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
        logger.info(f"Response: {response}")
    except Exception as e:
        logger.info(f"Error sending message: {e}")
        raise e


@flow
def data_pipeline():
    #workflow of all tasks to collect and sort messages from url, and send 
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mge9dn"

    payload = requests.post(url).json() #use requests to call https post 
    queue_url = payload['sqs_url'] #index payload to get url 

    messages = collect_messages(queue_url) #collect messages from url using task 
    phrase = sort_messages(messages) #sort all messages given and compile 
    send_solution("mge9dn", phrase, "prefect") #send phrase finally compiled 

if __name__ == "__main__":
    data_pipeline()
