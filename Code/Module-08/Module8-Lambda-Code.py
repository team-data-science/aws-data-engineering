import json
import base64
import boto3

from datetime import datetime

def lambda_handler(event, context):

    client = boto3.client('dynamodb')

    #print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        t_record = base64.b64decode(record['kinesis']['data'])

        # decode the bytes into a string
        str_record = str(t_record,'utf-8')

        #transform the json string into a dictionary
        dict_record = json.loads(str_record)

        # create Customer Row
        ############################

        customer_key = dict()
        customer_key.update({'CustomerID': {"N": str(dict_record['CustomerID'])}})


        ex_customer = dict()
        ex_customer.update({str(dict_record['InvoiceNo']): {'Value':{"S":'Some overview JSON for the UI goes here'},"Action":"PUT"}})

        response = client.update_item(TableName='Customers', Key = customer_key, AttributeUpdates = ex_customer)

        # Create Inventory Row
        #############################

        inventory_key = dict()
        inventory_key.update({'InvoiceNo': {"N": str(dict_record['InvoiceNo'])}})

        #create export dictionary
        ex_dynamoRecord = dict()

        #remove Invoice and Stock code from dynmodb record
        stock_dict = dict(dict_record)
        stock_dict.pop('InvoiceNo',None)
        stock_dict.pop('StockCode',None)

        #turn the dict into a json
        stock_json = json.dumps(stock_dict)

        #create a record (column) for the InvoiceNo
        #add the stock json to the column with the name of the stock number
        ex_dynamoRecord.update({str(dict_record['StockCode']): {'Value':{"S":stock_json},"Action":"PUT"}})

        #print(ex_dynamoRecord)
        response = client.update_item(TableName='Invoices', Key = inventory_key, AttributeUpdates = ex_dynamoRecord)


    return 'Successfully processed {} records.'.format(len(event['Records']))
