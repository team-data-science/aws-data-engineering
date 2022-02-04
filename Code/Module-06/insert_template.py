import pandas as pd
import requests

'''
Updates: 
- 2021-02-04: Created the TestSample.csv file in the new data folder and added the path to it here . This way you don't have to create it yourself

Notes:
- If you run into a json-body not found error, than you forgot to configure the application/json mapping template in the method
- Some students had the problem: They get 200 here but the ClodWatch log of the Lambda says KeyError: 'context'  -- I can only force this when I dont't send in a payload to the API. Make sure you send data
- If you get 403 error: Make sure to add the resource name you created in API gateway to the URL . In my case "hello". If you just copy out your URL from the "stage" in the UI then this is missing. 
'''

# URL of your endpoint
URL = "https://xxxxxx.execute-api.us-east-1.amazonaws.com/prod/hello"


#read the testfile
data = pd.read_csv('data/TestSample.csv', sep = ',')

# write a single row from the testfile into the api
#export = data.loc[2].to_json()
#response = requests.post(URL, data = export)
#print(response)

# write all the rows from the testfile to the api as put request
for i in data.index:
    try:
        # convert the row to json
        export = data.loc[i].to_json()

        #send it to the api
        response = requests.post(URL, data = export)

        # print the returncode
        print(export)
        print(response)
    except:
        print(data.loc[i])
