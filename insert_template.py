import pandas as pd
import requests

# URL of your endpoint
URL = "https://xxxxxxxxx.execute-api.us-east-1.amazonaws.com/P/hello"

#read the testfile
data = pd.read_csv('Testsample.csv', sep = ',')

# write a single row from the testfile into the api
#export = data.loc[2].to_json()
#response = requests.post(URL, data = export)
#print(response)

# write all the rows from the testfile to the api as put request
for i in data.index:
    # convert the row to json
    export = data.loc[i].to_json()

    #send it to the api
    response = requests.post(URL, data = export)

    # print the returncode
    print(response)
