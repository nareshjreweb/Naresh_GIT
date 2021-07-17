#!/usr/bin/env python
# coding: utf-8

from kafka import KafkaConsumer
import sys
import os
import json
import logging
import datetime
import time
import boto3 #using pip install boto3 to access SNS from python
from botocore.exceptions import ClientError

if __name__ == "__main__":
    #setting up python function to invoke command using "python kafka_produce_patient_vitals.py"
    os.environ["python"] = "/home/ec2-user/anaconda2/bin/python"
    #sns authentication
    sns = boto3.client("sns", region_name="us-east-1", aws_access_key_id='AKIAY3I43SR6E7MRMKGF', aws_secret_access_key='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')
    bootstrap_servers =['localhost:9092']
    topicName = 'Alerts_Message'
    #kafka consumer instatiation
    consumer = KafkaConsumer(topicName, group_id = 'my_group_id', bootstrap_servers=bootstrap_servers, auto_offset_reset ='earliest')
    client = sns

    try:
        for message in consumer:
            #print("%s:%d:%d Key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value ))
            #print(message.value)
            #pushing data to sns
            response = client.publish(
                TargetArn='arn:aws:sns:us-east-1:608335271036:Capstone_Alert_Message_Topic',
                Message=json.dumps({'default': json.dumps(message.value)}),
                MessageStructure='json',
                Subject='Health Alert Notification',
            )
        
    except KeyboardInterrupt:
        sys.exit()