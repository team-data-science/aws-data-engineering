from __future__ import print_function

import base64
import json
import boto3
from datetime import datetime

s3_client = boto3.client('s3')

# Converting datetime object to string
dateTimeObj = datetime.now()

#format the string
timestampStr = dateTimeObj.strftime("%d-%b-%Y-%H%M%S")

# this is the list for the records
kinesisRecords = []

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        # If you run into the error: [ERROR] TypeError: sequence item 0: expected str instance, bytes found
        # Add the ecoding into UTF8: 
        #payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        payload = base64.b64decode(record['kinesis']['data'])


        # append each record to a list
        kinesisRecords.append(payload)
        # this is just for logging
        # print("Decoded payload: " + payload)

    # make a string out of the list. Backslash n for new line in the s3 file
    ex_string = '\n'.join(kinesisRecords)

    # generate the name for the file with the timestamp
    mykey = 'output-' + timestampStr + '.txt'

    #put the file into the s3 bucket
    response = s3_client.put_object(Body=ex_string, Bucket='aws-de-project', Key= mykey)

    return 'Successfully processed {} records.'.format(len(event['Records']))
