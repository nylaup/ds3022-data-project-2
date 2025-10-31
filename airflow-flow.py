# airflow DAG goes here
import requests
import boto3
import time
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable 
from airflow.decorators import dag, task

#Set default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Create DAG
dag = DAG(
    'message_assembler_data_pipeline',
    default_args=default_args,
    description='Compile message from sqs queue and submit phrase',
    schedule=timedelta(days=1),
    catchup=False,
)

@task()
def fetch_url():
    #First task to get url for sqs queue
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mge9dn"
    payload = requests.post(url).json() #use requests to call https post 
    queue_url = payload['sqs_url']
    return queue_url

@task()
def collect_messages(queue_url):
    #Task flow that checks for available messages and checks if available
    #Contains functions for smaller parts of task 
    def get_queue_attributes(queue_url):
        #Gets attributes from queue and returns how many messages are available and how many are delayed
        try:
            sqs = boto3.client('sqs', region_name='us-east-1',
                aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"), #get variables from airflow
                aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"))
            response = sqs.get_queue_attributes( #use sqs to get attributes 
                QueueUrl=queue_url,
                AttributeNames=['All'])
            #create integer variables for number of messages and messages delayed
            Messages = int(response['Attributes']['ApproximateNumberOfMessages'])
            Incoming = int(response['Attributes']['ApproximateNumberOfMessagesDelayed'])
            print(f"Available Messages: {Messages}, Incoming Messages: {Incoming}") #log
            return Messages, Incoming

        except Exception as e:
            print(f"Error getting queue attributes: {e}")
            raise e
    
    def delete_message(queue_url, receipt_handle):
        #delete messages once they have been read using receipt handle
        try:
            sqs = boto3.client('sqs', region_name='us-east-1',
                aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"))
            sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=receipt_handle)
        except Exception as e:
            print(f"Error deleting message: {e}")
            raise e
    
    def get_message(queue_url):
        #function to get messages from queue and return their order number and phrase 
        try:
            sqs = boto3.client('sqs', region_name='us-east-1',
                aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"))
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
                print(f"Order No. {num} Message is: {word}") #log 
                receipt_handle = response['Messages'][0]['ReceiptHandle']
                delete_message(queue_url, receipt_handle) #delete messages once read 
                return num, word
            else: #if no messages then don't do anything 
                return None

        except Exception as e:
            print(f"Error getting message: {e}")
            raise e
    
    messages = [] #empty list to store messages 
    start_time = time.time() 

    while len(messages) < 21: #exits loop if 21 messages collected 
        time_elapsed = time.time() - start_time #start timer 
        if time_elapsed > 960: #exit if waiting more than 900s
            print("waited too long")
            break

        available, delayed = get_queue_attributes(queue_url) #get # of messages to process
        if available > 0: #start processing if available messages
            while True: 
                msg = get_message(queue_url) #call get message function and store output
                if not msg: #exit if no message 
                    break
                messages.append(msg) #append to list
                print(f"collected {len(messages)}")
        elif delayed > 0: #log how many still incoming 
            print(f"{delayed} still delayed")
        else:
            print("no messages")
        time.sleep(30) #wait 30s before checking again
    return messages #return messages list once all collected 

@task()
def sort_messages(messages):
    #task to compile messages into dataframe, sort, then compile into phrase 
    if not messages: # exit if error and messages list empty 
        print("No messages")
        return None
    #create dataframe of messages with their number
    df = pd.DataFrame(messages, columns=['order_no', 'word'])  
    df = df.sort_values(by=['order_no']) #sort dataframe 
    #compile word column into list, then join into phrase separated by " "
    phrase = " ".join(df['word'].tolist()) 
    print(f"The final phrase is: {phrase}")
    return phrase

@task()
def send_solution(uvaid, phrase, platform):
    #Task to send compiled phrase with platform and student id to given url  
    sqs = boto3.client('sqs', region_name='us-east-1',
            aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"))
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
    
with dag:
    #With statement to add tasks to dag in order to create dependencies
    queue_url = fetch_url()
    messages = collect_messages(queue_url)
    phrase = sort_messages(messages)
    send_solution('mge9dn', phrase, 'airflow')