import json
import boto3

def lambda_handler(event, context):

    print("MyEvent:")
    print(event)

    method = event['context']['http-method']

    if method == "GET":
        dynamo_client = boto3.client('dynamodb')

        im_customerID = event['params']['querystring']['CustomerID']
        print(im_customerID)
        response = dynamo_client.get_item(TableName = 'Customers', Key = {'CustomerID':{'N': im_customerID}})
        print(response['Item'])

        #myreturn = "This is the return of the get"

        return {
            'statusCode': 200,
            'body': json.dumps(response['Item'])
           }
