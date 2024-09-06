import json
import boto3

def lambda_handler(event, context):

    print("MyEvent:")
    print(event)

    method = event['context']['http-method']

    if method == "GET":
        dynamo_client = boto3.client('dynamodb')

        im_invoice_id = event['params'][' querystring']['InvoiceNo']
        print(im_invoice_id)
        response = dynamo_client.get_item(TableName='Invoices', Key={'InvoiceNo': {'N': im_invoice_id}})
        print(response['Item'])

        return {
            'statusCode': 200,
            'body': json.dumps(response['Item'])
           }
