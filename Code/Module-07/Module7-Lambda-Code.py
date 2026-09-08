import base64
import json
import boto3
import os
from datetime import datetime

s3_client = boto3.client("s3")

# Get the current datetime object
date_obj = datetime.now()

# Format the datetime object as a string
timestamp = date_obj.strftime("%Y-%m-%d-%H-%M-%S")

# Create an empty list to store the whole batch of records
kinesis_records = []

# Environment variable for S3 bucket name
bucket_name = os.environ["bucket_name"]


def lambda_handler(event, context):
    # Get some information of the size of the batch
    record_count = len(event["Records"])
    print(f"Received {record_count} records from Kinesis stream.")

    for record in event["Records"]:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")

        # Use this print statement for more verbose logging
        # print("Decoded payload: " + payload)

        kinesis_records.append(payload)

        # Convert the list of records to a JSON string
        json_data = json.dumps(kinesis_records)

        file_name = f"data_{timestamp}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_data,
            ContentType="application/json",
        )
        print(f"Successfully uploaded file to S3: {file_name}")
    return "Successfully processed {} records.".format(len(event["Records"]))
