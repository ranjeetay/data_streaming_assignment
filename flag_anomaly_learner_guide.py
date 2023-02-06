
########################################################## LIBRARIES ######################################################################

# Import Airflow Operators
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor

# Import libraries to handle SQL and dataframes
import pandas as pd
import numpy as np

# Import functions from config
import os
import numpy as np

# Import Library to send Slack Messages
import requests

# Import libraries to keep time
from datetime import datetime

# --------------------------------
# CONSTANTS
# --------------------------------

# Set input and output paths & import the CSV into a dataframe
FILE_PATH_INPUT = '/home/airflow/gcs/data/input/'
FILE_PATH_OUTPUT = '/home/airflow/gcs/data/output/' 

#Read the file
file_name = os.listdir(FILE_PATH_INPUT)[0]
df = pd.read_csv(FILE_PATH_INPUT + file_name)

# Database Variables
database_ip = "34.96.166.20" #public ip
database_username = "root"
database_password = "root"

########################################################### SLACK MESSAGE SECTION ##############################################################

# Slack webhook link
slack_webhook = "https://hooks.slack.com/services/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 "

# --------------------------------
# Function Definitions
# --------------------------------

# Function to send messages over slack using the slack_webhook

def send_msg(text_string): 
    requests.post(slack_webhook, json={'text': text_string}) 

def send_report():
    # Write a body message which need to send in the slack message
    
    #1. Average high_avg
    
    high_avg = df['High'].avg
    message_1 ="Average high_avg "
    text_string = 'high_avg Value:{0}. {1}'.format(high_avg,message_1)
    send_msg(text_string)
    
    #2. Average Low_avg
    
    Low_avg = df['Low'].avg
    message_1 ="Average Low "
    text_string = 'Low_avg Value:{0}. {1}'.format(Low_avg,message_1)
    send_msg(text_string)
    
    
    #3.Average volume_avg
    
    volume_avg= df['Volume'].mean()
    message_1 ="Average of volume_avg "
    text_string = 'volume_avg Value:{0}. {1}'.format(volume_avg,message_1)
    send_msg(text_string)
    
    
    ############################################################### FLAG ANOMALY SECTION ############################################################


#3 Function to filter anomalies save it as a CSV file into Output folder and print the dataframe
# NOTE: consider the threshold is an anomaly if any data point exceeds will send a congratulations report if not it will send don't lose hope message in the logs
# Add print statements to each output dataframe so that it appears on the Logs

def flag_anomaly():
    volume_threshold = 20
    low_threshold = 50
    high_threshold = 200
    
    avg_rolling = close+df['date'].avg
    
    # Write a code to loop the columns such as points_awarded and the dataframe index.
    
    # write a condition to campare the if the points_awarded greater then coupon_threshold
    # If cross the threshold print("Congratulations") else print("Congratulations!, below report shows you have not acheived your limit")
    for i in range(len(df)): 
       if  df["avg_rolling"] < volume_threshold:
           body += " Congratulation! Find your average volume_theshold report here"
           body += "Volume:" +df.iloc[0,:]['Volume']
           text_string = 'body Value:{0}. {1}'.format(Volume,body)
           send_msg(text_string)
           df.to_csv(FILE_PATH_OUTPUT + 'avg_rolling_volume.csv')
           
        elif df["avg_rolling"] < high_threshold:
           body += " Congratulation! Find your average high_theshold report here"
           body += "High:" +df.iloc[0,:]['High']
           text_string = 'body Value:{0}. {1}'.format(High,body)
           send_msg(text_string)
           df.to_csv(FILE_PATH_OUTPUT + 'avg_rolling_high.csv')
           
        elif df["avg_rolling"] < low_threshold:
           body += " Congratulation! Find your average low_theshold report here"
           body += "Low:" +df.iloc[0,:]['Low']
           text_string = 'body Value:{0}. {1}'.format(Low,body)
           send_msg(text_string)
           df.to_csv(FILE_PATH_OUTPUT + 'avg_rolling_low.csv') 
       else:
          text_string = "Congratulations!,below report shows you have not acheived threshold limit."
          send_msg(text_string)
          df.to_csv(FILE_PATH_OUTPUT + 'error_avg_rolling.csv')   

##################################################################### DAG DEFINITION SECTION #######################################################

# Define the defualt args
default_args = {}

# Create the DAG object
with DAG(
    'Avg-Rolling report',
    default_args=default_args,
    description='DAG to validate Avg rolling report here',
    schedule_interval='*/1 * * * *',  # Replace with appropriate schedule after testing
    catchup=False
) as dag:

    start_task = DummyOperator(
        task_id='start',
        dag=dag
    )

    file_sensor = FileSensor(
        task_id='file_sensor_task',
        poke_interval=1,
        filepath=FILE_PATH_INPUT,
        timeout=5,
        dag=dag
    )

    send_report_task = PythonOperator(
    task_id='send_report_task',
    python_callable=send_report,
    dag=dag
    )

    flag_task = PythonOperator(
    task_id='flag_anomaly_task',
    python_callable=flag_anomaly,
    dag=dag
    )
    
    end_task = DummyOperator(
    task_id='end',
    dag=dag
    )

    start_task >> file_sensor >> send_report_task >> flag_task >> end_task
# --------------------------------
