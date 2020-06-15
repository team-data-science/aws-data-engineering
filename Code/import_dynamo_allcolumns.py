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

        #initialize empty dictionary
        ex_dynamoRecord = dict()

        # Write all the records as a string for dynamo db.
        # Check the structure and datatypes in the boto3 documentation. There are other datatypes look into that
        for it in dict_record:
            ex_dynamoRecord.update({it: {"S": str(dict_record[it])} })


        # put this thing to dynamodb
        response = client.put_item(TableName='Transactions', Item = ex_dynamoRecord)

    return 'Successfully processed {} records.'.format(len(event['Records']))


#### Example for Table with Timestamp as key and just a string ####

# Converting datetime object to string
#dateTimeObj = datetime.now()

#format the string
#timestampStr = dateTimeObj.strftime("%d-%b-%Y-%H%M%S")

# create the string for later use as the key of the row
#mytimestamp = {"S": timestampStr}

#how to add the complete message as a string object into the dictionary
#mymessage= {"S": str_record}

# create the payload dict for dynamodb and add the columns
#ex_dynamoRecord = dict()
#ex_dynamoRecord["Timestamp"] = mytimestamp
#ex_dynamoRecord["Message"] = mymessage

# use the write from above to write it


    # Input string from the dataset
        #{"InvoiceNo":536365,"StockCode":"84406B","Description":"CREAM CUPID HEARTS COAT HANGER","Quantity":8,"InvoiceDate":"12\/1\/2010 8:26","UnitPrice":2.75,"CustomerID":17850,"Country":"United Kingdom"}

    # Write the attributes from each transaction with the right type N=number S=String by modifying each element of the dictionary
        # I have not tested this but it should work
        #ex_dynamoRecord.update({"InvoiceNo": {"N": str(dict_record["InvoiceNo"])} })
        #ex_dynamoRecord.update({"StockCode": {"S": str(dict_record["StockCode"])} })
        #ex_dynamoRecord.update({"Description": {"S": str(dict_record["Description"])} })
        #ex_dynamoRecord.update({"Quantity": {"N": str(ict_record["Quantity"])} })
        #ex_dynamoRecord.update({"InvoiceDate": {"S": str(dict_record["InvoiceDate"])} })
        #ex_dynamoRecord.update({"UnitPrice": {"N": str(dict_record["UnitPrice"])} })
        #ex_dynamoRecord.update({"CustomerID": {"N": str(dict_record["CustomerID"])} })
        #ex_dynamoRecord.update({"Country": {"N": str(dict_record["Country"])} })
