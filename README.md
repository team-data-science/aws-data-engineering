# AWS Data Engineering: A Full Streaming and Batch Pipeline

This repo walks you through building a complete data engineering project on AWS, from a live API all the way to a Redshift warehouse. You send retail transactions to an API, they stream through Kinesis, and you fan them out into raw S3 storage, a DynamoDB serving layer, and Redshift, plus a separate batch pipeline with Glue. By the end you'll have every piece running and you'll understand how they connect.

It follows the LearnDataEngineering course modules 05 to 11. You can do it start to finish, or jump to the module you care about.

## What you'll build

Two data paths that both land in the same warehouse:

- **The streaming path.** A POST API takes single transactions, a Lambda drops them into a Kinesis stream called `APIData`, and three consumers read off that same stream: one Lambda writes the raw JSON to S3, one Lambda writes into DynamoDB for fast lookups, and Amazon Data Firehose loads everything into Redshift.
- **The serving API.** A GET API reads a single invoice back out of DynamoDB, so you have a read path for a dashboard or app.
- **The batch path.** CSV files sit in S3, a Glue crawler catalogs them, and a Glue ETL job bulk-loads them into a second Redshift table.

If you've heard the term "lambda architecture" (a batch layer and a speed layer feeding the same place), that's basically what this is. You don't have to care about the name. The point is you'll have built both styles of pipeline against the same data.

## Architecture at a glance

```
  insert_template.py
        │  POST
        ▼
  API Gateway  ──►  Lambda (KinesisIngest)  ──►  Kinesis "APIData"
                                                      │
                    ┌─────────────────────────────────┼─────────────────────────────────┐
                    ▼                                  ▼                                  ▼
          Lambda (KinesisToRaw)           Lambda (KinesisToDynamoDB)              Amazon Data Firehose
                    │                                  │                                  │
                    ▼                                  ▼                                  ▼
              S3 (raw JSON)                   DynamoDB                             Redshift
                                          (Customers, Invoices)              (firehosetransactions)
                                                  │
                                                  ▼
                                    Lambda (DynamoDBReadAPI)  ◄── GET  API Gateway


  Batch path (separate):

  S3 (CSV files)  ──►  Glue Crawler  ──►  Glue Data Catalog  ──►  Glue ETL Job  ──►  Redshift (bulkimport)
```

## The data set

We use the classic **Online Retail** dataset: real transactions from a UK-based online store. Each row is one line item on an invoice, with these columns:

`InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`

The raw file [`Online_Retail.csv`](Code/Module-06/Online_Retail.csv) has around 540,000 rows and some messiness in it: missing customer IDs, cancelled invoices, and old Mac-style line endings that trip up some tools. So we clean it first with [`data_preprocessing.py`](Code/Module-06/data_preprocessing.py), which drops the empty rows, keeps only numeric invoice numbers, and writes out clean versions you can actually stream.

You don't have to run the cleaning yourself, the cleaned files are already in the repo:

| File | Rows | Use it for |
| --- | --- | --- |
| [`Online_Retail_Cleaned.csv`](Code/Module-06/Online_Retail_Cleaned.csv) | ~397k | The full cleaned set |
| [`Online_Retail_Cleaned_1000rows.csv`](Code/Module-06/Online_Retail_Cleaned_1000rows.csv) | 1000 | Batch loading and bigger tests |
| [`Online_Retail_Cleaned_10rows.csv`](Code/Module-06/Online_Retail_Cleaned_10rows.csv) | 10 | Your first end-to-end test |

Start with the 10-row file. When something breaks you want to see it break in ten records, not a thousand.

## Prerequisites

- An **AWS account**. A fresh free-tier account is fine, but a few things here cost money (see the cost note below).
- A **region you'll stick to the whole way through**. This guide uses `us-east-1` (N. Virginia) everywhere. Region matters more than you'd think, because the Firehose IP and some service endpoints are region-specific. If you pick a different region, use it consistently.
- **Python 3.10+** on your machine for the local sender script, with `pandas` and `requests`:
  ```bash
  pip install pandas requests
  ```
- Basic comfort clicking around the AWS console. I do almost everything here in the UI on purpose, because seeing where each setting lives teaches you more than pasting Terraform you don't understand yet.

## Read this before you start: cost

This is super important and it's the thing that bites people. A few of these services are not free-tier-friendly if you leave them running:

- **Redshift** (module 10) costs money per hour the cluster is up.
- **Kinesis** provisioned shards cost money per hour.
- **DynamoDB** provisioned capacity and **Firehose** add small amounts.

None of it is expensive for a day of learning, but a Redshift cluster you forgot about over a weekend will surprise you. So do two things: set up a billing budget in module 05 before anything else, and go through the [Cleaning up](#cleaning-up) section at the end when you're done. Pause the Redshift cluster when you're not actively using it.

## Repo structure

All the code and config lives under `Code`, split by module:

```
Code/
├── Module-06/   ingestion: Lambda, sender script, IAM policy JSONs, the datasets
├── Module-07/   stream to S3: Lambda
├── Module-08/   stream to DynamoDB: Lambda + test event
├── Module-09/   read API: Lambda
├── Module-10/   Redshift + Firehose: table SQL, jsonpaths, copy command
└── Module-11/   batch with Glue: bulkimport table SQL
```

---

## Module 05: AWS basics, budgets and IAM

Before we build anything, two foundations: a budget so AWS can't bill you by surprise, and a clear picture of how IAM roles and policies work, because every Lambda in this project needs the right ones.

### Set a billing budget

1. In the console search bar, search for **Billing and Cost Management**. This is where you see your monthly costs.
2. Go to **Budgets and Planning → Budgets → Create Budget**.
3. Use a template, pick **Monthly cost budget**, and enter a name, an amount, and the email that should get the alert.

> **Watch out:** a budget alert only fires at 85% of forecast, at 100% of forecast, and at 100% of actual spend. After that it goes quiet. So if you want to be warned earlier or more often, create a few budgets at different thresholds as a notification ladder, or set up a Budget Report. One budget is not a safety net, it's a single heads-up.

### Understand IAM: roles vs policies

You'll create a bunch of these as we go, so get the model straight now:

- A **policy** is a set of permissions. "Allowed to put records into Kinesis." "Allowed to read from DynamoDB." That's it.
- A **role** is what you attach to a service, like a Lambda function. The role carries one or more policies.

So the pattern every single time is: write a policy that says what's allowed, then create a role for the Lambda and attach that policy plus a logging policy. If you go into the **Policies** section in IAM you'll see the AWS-managed ones marked with a little cube, and you can also write your own as JSON and scope them down to specific resources.

Here's which permissions each Lambda in this project ends up needing:

| Lambda | Needs to |
| --- | --- |
| Ingestion (module 06) | Put records into Kinesis, write logs to CloudWatch |
| Stream to raw S3 (module 07) | Read records from Kinesis, write to S3, log to CloudWatch |
| Stream to DynamoDB (module 08) | Read records from Kinesis, write to DynamoDB, log to CloudWatch |
| Read API (module 09) | Read from DynamoDB, log to CloudWatch |

We don't create all of these up front. We build each role right before the module that uses it, so it's less abstract and you can see exactly why that Lambda has those permissions. The exact JSON for the Kinesis and DynamoDB policies is in [`Code/Module-06`](Code/Module-06) if you'd rather read the permissions than click through the UI.

Logging and the Boto3 basics from this module can stay as they are in the course.

---

## Module 06: The ingestion pipeline

This is the front of the whole thing. We build: a Kinesis stream, a Lambda that writes to it, an API Gateway endpoint that triggers the Lambda, and a local script that fires transactions at the API.

### Create the Kinesis stream

1. Search for **Kinesis** and click **Data Streams → Create Data Stream**.
2. Name it **`APIData`**. This exact name matters, the Lambda code references it.
3. Capacity mode **Provisioned**, Shards **1**. Provisioned is cheaper than on-demand for a small steady test like this.
4. Scroll down and create the stream.

You should see the stream go to **Active** after a moment. One shard is plenty here. In production you'd size shards to your throughput, but a single shard handles our test traffic fine.

### Create the IAM role for the ingestion Lambda

1. Search for **IAM**, go to the **Policies** tab, **Create Policy**.
2. Choose service **Kinesis → Next**, then under **Write** access level pick **PutRecord** and **PutRecords**.
3. For **Resources → Specific**, paste the ARN of your `APIData` stream. Or pick "any in this account" if you want to keep it simple while learning. Then **Next**.
4. Name the policy **`myKinesisWrite`** and create it. (This matches [`IAM-Policy-WriteKinesis.json`](Code/Module-06/IAM-Policy-WriteKinesis.json).)
5. Go to the **Roles** tab, **Create Role**, choose **AWS service → Lambda → Next**.
6. Attach two policies: **`myKinesisWrite`** and **`AWSLambdaBasicExecutionRole`** (that second one is the CloudWatch logging permission).
7. Name the role **`LambdaStreamIngest`** and create it.

### Create the ingestion Lambda

1. Search for **Lambda → Create a function**.
2. Function name **`KinesisIngest`**, runtime **Python 3.12**, architecture default **x86**.
3. Under permissions, **Change default execution role → Use an existing role → `LambdaStreamIngest`**.
4. Create the function.
5. Scroll to the code, paste in [`PostMethod-Lambda-Code.py`](Code/Module-06/PostMethod-Lambda-Code.py), and hit **Deploy**.

The code is short: it reads the POST body, turns it back into a string, and calls `put_record` into the `APIData` stream. Nothing magic.

### Create the API Gateway endpoint

1. Search for **API Gateway → Create API → REST API → Build**.
2. API name **`Online Retail Data Transfer`**, create it.
3. **Create Resource**, name it **`main`**, create resource.
4. **Create method**: method type **POST**, integration type **Lambda function**, then search for **`KinesisIngest`** and select it. Create the method.

Now the piece people forget, and it breaks everything downstream if you skip it:

5. Click **Integration Request → Edit**, scroll to **Mapping Templates**, and **Generate template → Method request passthrough**. Set content type to **`application/json`** and **Save**.

> **This is super important.** The Lambda reads `event['context']['http-method']` and `event['body-json']`. Those fields only exist because of this passthrough mapping template. Skip it and you'll get a 200 in the test console but a `KeyError` in the logs, and you'll waste an afternoon on it. Ask me how I know.

6. **Deploy API**, create a stage called **`prod`**, and deploy. In a real project you'd have several stages through development, one is fine for us.

### Test it in the console

1. Go to the **Test** section of your POST method.
2. Paste this into the request body:
   ```json
   {"InvoiceNo":536365,"StockCode":"84029E","Description":"RED WOOLLY HOTTIE WHITE HEART.","Quantity":6,"InvoiceDate":"12/1/2010 8:26","UnitPrice":3.39,"CustomerID":17850,"Country":"United Kingdom"}
   ```
3. Run it. You should get a **200** response, because the stream and the function already exist.
4. Check the logs under **CloudWatch → Log groups → `/aws/lambda/KinesisIngest`** to see the event come through.

### Send real data from your machine

Now let's fire actual rows at it with [`insert_template.py`](Code/Module-06/insert_template.py).

1. Grab your invoke URL. Go to **API Gateway → Stages** and copy the URL, then add your resource name to the end. So it looks like `https://xxxxx.execute-api.us-east-1.amazonaws.com/prod/main`.
2. Open `insert_template.py` and paste it into the `URL` variable:
   ```python
   # replace this with your own invoke URL, and don't forget the /main resource on the end
   URL = "https://xxxxx.execute-api.us-east-1.amazonaws.com/prod/main"
   ```
3. Run it from inside `Code/Module-06` so it finds the CSV next to it:
   ```bash
   cd Code/Module-06
   python3 insert_template.py
   ```

The script reads `Online_Retail_Cleaned_10rows.csv` by default and posts each row. You should see each row's JSON print followed by `<Response [200]>`. If you want to push more, point the script at the 1000-row file instead, but do the 10-row run first.

To confirm the data actually landed in Kinesis, open the `APIData` stream and watch the **PutRecord** count climb in the Monitoring tab, or check the CloudWatch logs again.

> **Bonus:** you can test the API with Postman instead of code if you just want to fire one sample. The ready-made JSON is in [`Postman_test_string.txt`](Code/Module-06/Postman_test_string.txt).

---

## Module 07: Stream to raw S3 storage

First consumer off the stream. We store every incoming record as raw JSON in S3, which is your cheap, keep-everything landing zone.

### Create the S3 bucket

1. Search for **S3 → Create bucket**.
2. Give it a name. Bucket names have to be globally unique across all of AWS, so something generic will be taken. See the [naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html). Example: `online-retail-apidata-raw`.
3. Leave everything on default: block public access on, no ACLs. Create the bucket.

### Create the IAM role for the S3 Lambda

1. In **IAM → Policies → Create Policy**, choose **Kinesis → Next**.
2. Under access levels, select **all List actions** and **all Read actions**. This Lambda only reads the stream, it never writes to it.
3. Resources: your `APIData` ARN, or any in this account. **Next**.
4. Name it **`myKinesisRead`** and create it. (Matches [`IAM-Policy-ReadKinesis.json`](Code/Module-06/IAM-Policy-ReadKinesis.json).)
5. Go to **Roles → Create Role → AWS service → Lambda → Next**.
6. Attach three policies: **`myKinesisRead`**, **`AmazonS3FullAccess`**, and **`AWSLambdaBasicExecutionRole`**.
7. Name the role **`LambdaRawS3Pipeline`** and create it.

### Create the Lambda from a blueprint

This time we start from an AWS blueprint that already wires up the Kinesis trigger for us.

1. **Lambda → Create a function → Use a blueprint**.
2. Blueprint name: **Process records sent to a Kinesis stream** (runtime Python 3.10), architecture default x86.
3. Name it **`KinesisToRaw`** (or `KinesisToS3`, your call).
4. **Change default execution role → Use an existing role → `LambdaRawS3Pipeline`**.
5. Configure the Kinesis trigger:
   - Select the **`APIData`** stream.
   - **Batch Size 100.** That means each Lambda invocation handles up to 100 records, so you get small files of ~100 entries. Fine for learning, you'd tune it up for real volume.
   - Additional settings: **Retry attempts: 2**.
6. Create the function.

### Point it at your bucket and add the code

The Lambda reads the bucket name from an environment variable, so it isn't hardcoded.

1. Go to the function's **Configuration → Environment variables → Edit → Add**.
2. Key **`bucket_name`**, value your actual bucket name (for example `online-retail-apidata-raw`). Save.
3. Scroll to the code, paste in [`Module7-Lambda-Code.py`](Code/Module-07/Module7-Lambda-Code.py), and **Deploy**.

The code decodes each base64 Kinesis record, collects the batch, and writes it as a timestamped file like `data_2025-08-12-14-30-00.json` into your bucket.

### Test it

1. Click the dropdown next to **Test → Configure new test event**.
2. Give it a name and pick the **Kinesis** template (`kinesis-get-records`). Save.
3. Hit **Test**. You should see a success message in the output window, and a new JSON file should appear in your S3 bucket.

If you already have live data flowing from module 06, the trigger fires on its own and files start showing up without you doing anything. That's the whole point of the trigger.

---

## Module 08: Stream to DynamoDB

Second consumer off the same stream. Here we write into DynamoDB, which gives us fast key lookups for a serving API later. Same stream, different destination, and that's the nice thing about Kinesis: multiple consumers, one source.

### Create the DynamoDB tables

We need two tables. Create each one the same way under **DynamoDB → Tables → Create Table**:

**Customers**
- Table name: `Customers`
- Partition key: `CustomerID`, type **Number**
- Table settings: **Customized**
- Capacity mode **Provisioned**, autoscaling **off**, read capacity **1**, write capacity **1**
- Create table

**Invoices**
- Same steps, but table name `Invoices` and partition key `InvoiceNo`, type **Number**

Capacity 1/1 keeps cost near nothing. It's slow if you hammer it, but you won't during a tutorial.

### Create the IAM role for the DynamoDB Lambda

1. **IAM → Policies → Create Policy**, choose **DynamoDB → Next**.
2. Under **List** pick **ListTables**. Under **Write** pick **UpdateItem** and **PutItem**.
3. Resources: any in this account. Name it **`myDynamoDBWrite`** and create it.

> **Note:** `PutItem` replaces a whole row, `UpdateItem` only touches the columns you give it and leaves the rest alone. The Lambda here uses `UpdateItem` so it can keep adding stock codes to an existing invoice without wiping what's already there. Worth knowing the difference, it changes how your data builds up.

4. **Roles → Create Role → AWS service → Lambda → Next**.
5. Attach **`myDynamoDBWrite`**, **`myKinesisRead`** (this Lambda reads the stream too), and **`AWSLambdaBasicExecutionRole`**.
6. Name the role **`LambdaDynamoDBWrite`** and create it.

### Create the Lambda from the blueprint

1. **Lambda → Create a function → Use a blueprint**, same **Process records sent to a Kinesis stream** blueprint (Python 3.10), x86.
2. Name it **`KinesisToDynamoDB`**.
3. **Use an existing role → `LambdaDynamoDBWrite`**.
4. Kinesis trigger: select **`APIData`**, **Batch Size 10** this time for faster debugging, retry attempts 2.
5. Create the function, then paste in [`Module8-Lambda-Code.py`](Code/Module-08/Module8-Lambda-Code.py) and **Deploy**.

The code splits each transaction across both tables: it updates the `Customers` row keyed on `CustomerID`, and it updates the `Invoices` row keyed on `InvoiceNo`, storing the line item as a JSON blob under its stock code.

### Test it

1. Configure a new test event, but this time use the ready-made event: paste the contents of [`DynamoDBTestEvent.json`](Code/Module-08/DynamoDBTestEvent.json).
2. Run the test. You should see it process the records successfully.
3. Go to **DynamoDB → Tables → `Customers`** (or `Invoices`) **→ Explore table items** and you should see rows.

---

## Module 09: The read API

Now the serving side. We build a GET endpoint that pulls a single invoice back out of DynamoDB, so a dashboard or app has something to call.

### Create the IAM role for the read Lambda

1. **IAM → Policies → Create Policy → DynamoDB → Next**.
2. Under **Read** access level pick **GetItem**. That's all this one needs. Resources: any in this account. Name it **`myDynamoRead`** and create it. (Matches [`IAM-Policy-Get-DyanamoDB.json`](Code/Module-06/IAM-Policy-Get-DyanamoDB.json).)
3. **Roles → Create Role → AWS service → Lambda → Next**, attach **`myDynamoRead`** and **`AWSLambdaBasicExecutionRole`**, name it **`LambdaDynamoDBRead`**, create it.

### Create the read Lambda

1. **Lambda → Create a function**, name **`DynamoDBReadAPI`**, runtime **Python 3.12**, x86.
2. **Use an existing role → `LambdaDynamoDBRead`**, create the function.
3. Paste in [`GetMethod-Lambda-Code.py`](Code/Module-09/GetMethod-Lambda-Code.py) and **Deploy**.

It reads the `InvoiceNo` from the query string and does a `get_item` against the `Invoices` table.

### Add a GET method to your existing API

We reuse the API from module 06, we're just adding a second method to the same `main` resource.

1. **API Gateway → your `Online Retail Data Transfer` API → resource `main`**.
2. **Create method**: type **GET**, integration type **Lambda function**, search for **`DynamoDBReadAPI`**, create the method.
3. **Integration Request → Edit → Mapping Templates → Method request passthrough**, content `application/json`, save. Same reason as the POST method: the Lambda reads `event['params']['querystring']`, which only exists with this template.
4. **Deploy API** to the `prod` stage.
5. Test it: in the Test section, add the query string **`InvoiceNo=536365`** and run. You should get a 200 back with the invoice item as JSON.

---

## Module 10: Streaming into Redshift with Firehose

Third consumer off the stream. Amazon Data Firehose reads from Kinesis and loads into a Redshift warehouse, so you have your data in real SQL tables for analytics. This is the module with the most moving parts, so go slow.

### Create the Redshift cluster

1. Search for **Redshift → Clusters → Create Cluster**.
2. Cluster identifier: leave it `redshift-cluster-1`.
3. Size: **`dc2.large`**, number of nodes **1**.
4. Admin user name **`awsuser`**, and set an admin password you'll remember. You'll type it into Firehose and Glue later.
5. Database name defaults to **`dev`**. Leave it, or change it under Additional configuration if you want.
6. Open **Additional configuration → Network and security** and check **Publicly accessible**. Firehose needs to reach it.
7. Create the cluster.

No IAM role is needed on Redshift itself here, because we aren't having Redshift reach out to anything. Firehose pushes the data in.

> **Watch out:** once the cluster is up, create a snapshot before you try to pause it (Actions → Create snapshot). AWS won't let you pause a cluster that has never been snapshotted, and pausing is how you stop the meter running when you take a break.

### Open the security group for Firehose

Redshift sits inside a VPC, and Firehose has to be allowed in.

1. **Redshift → your cluster → Properties tab**, scroll to network and security, click the **VPC security group**.
2. Select the security group, scroll to **Inbound rules → Edit inbound rules**, and add a rule.
3. The rule allows the **Firehose IP for your region**. For `us-east-1` (N. Virginia) it's **`52.70.63.192/27`**. For any other region, get the right IP from the [Firehose access docs](https://docs.aws.amazon.com/firehose/latest/dev/controlling-access.html), because it's different per region and a wrong one here means data silently never arrives.

> **Heads up:** the security group ID will look a little different from any screenshot. That's normal, it's generated per account. Just make sure you're editing the one attached to your cluster.

### Create the Redshift table

1. **Redshift → Query editor**, connect to the database. Create a new connection, database name **`dev`**, and your admin credentials.
2. Run the create-table SQL from [`Redshift-Table-Create-Command.txt`](Code/Module-10/Redshift-Table-Create-Command.txt). It creates the `firehosetransactions` table.
3. Click the three dots next to the table to view its schema and confirm it's there.

The columns are all set to `not null` on purpose. That way a row that didn't map correctly fails loudly on import instead of quietly landing half-empty. If loads fail, that same file tells you to check `STL_LOAD_ERRORS` for the reason.

### Prepare the S3 staging bucket and jsonpaths file

Firehose stages the data in S3 before it copies it into Redshift, and it needs a map from your JSON fields to the table columns.

1. Create another S3 bucket (globally unique name), for example `firehoseredshift-yourname`. Defaults are fine.
2. Download [`jsonpaths.json`](Code/Module-10/jsonpaths.json) from this repo. It lists each field in column order.
3. Upload that file into your new bucket with the **Upload** button.

### Configure the Firehose stream

1. Search for **Amazon Data Firehose → Firehose streams → Create Firehose stream**.
2. Source: **Amazon Kinesis Data Streams**, destination: **Amazon Redshift**.
3. Source settings: browse for the **`APIData`** stream.
4. Leave the stream name on its default `KDS-RED-...`.
5. Destination settings: **Provisioned cluster → `redshift-cluster-1`**, database **`dev`**.
6. Authentication: **username and password**, user **`awsuser`** and your password.
7. Table: **`firehosetransactions`**.
8. Intermediate S3 bucket: browse for the `firehoseredshift` bucket you just made.
9. Copy command options: use the JSON copy command, which references your `jsonpaths.json`. The template is in [`Firehose-copy-command.txt`](Code/Module-10/Firehose-copy-command.txt).

> **Do NOT skip this:** you have to edit the bucket name in the copy command to your actual bucket. The template ships with a placeholder bucket, and if you leave it, the copy points at a bucket that isn't yours and nothing loads. This is the single most common reason "everything looks configured but Redshift stays empty."

10. Retry duration: **120**.
11. Open **Buffer hints, compression and encryption** and set buffer size **2 MB**, buffer interval **60 seconds**, so data flushes fast enough for you to see it during testing.
12. In Advanced settings, let it create a **Service Role** with the permissions Firehose needs.
13. Create the Firehose stream.

Now send some data through module 06's script again and give it a minute. Rows should show up in `firehosetransactions` when you query it in the Redshift editor. If they don't, check `STL_LOAD_ERRORS` first, it almost always names the exact problem.

---

## Module 11: Batch processing with AWS Glue

Completely separate path from everything above. Instead of streaming, we take CSV files sitting in S3 and bulk-load them into Redshift with Glue. This is your batch layer.

### Create a VPC endpoint for S3

Glue needs to reach S3 from inside the VPC, and a Gateway endpoint is the clean way to do that.

1. Search for **VPC → Endpoints → Create Endpoint**.
2. Name **`s3endpoint`**, category **AWS services**.
3. Search services for S3 and pick the Gateway one for your region. For `us-east-1` that's **`com.amazonaws.us-east-1.s3`**, type **Gateway**.
4. VPC: the one your Redshift cluster is in (usually the only default one).
5. Check the box for the **default route table**, set policy to **Full access**, and create it.

### Create the Glue IAM role

1. **IAM → Roles → Create Role → AWS service → Glue → Next**.
2. Attach **`AmazonRedshiftFullAccess`**, **`AWSGlueServiceRole`**, and **`AmazonS3FullAccess`**.
3. Name it **`AWSGlueServiceRole-bulkimport`** and create it.

### Upload the CSVs to a new bucket

1. Create an S3 bucket, for example `aws-bulkimport-glue` (unique name, defaults fine).
2. Upload [`Online_Retail_Cleaned_1000rows.csv`](Code/Module-06/Online_Retail_Cleaned_1000rows.csv) with the Upload button. This is the batch we'll load.

### Set up the Glue crawlers

Make sure Redshift is running first: **Redshift → your cluster → Actions → Resume** if you paused it.

Create a Glue database to catalog into:

1. **Glue → Data Catalog → Databases → Create Database**, name **`glue-transactionsdb`**, create it.

Now a crawler for the CSV files in S3:

2. **Crawlers → Create Crawler**, name it something unique like `S3LearnDataEngineeringCrawler`, **Next**.
3. Add a data source: **S3**, browse for your `aws-bulkimport-glue` bucket, add it as a source, **Next**.
4. Choose the existing IAM role **`AWSGlueServiceRole-bulkimport`**, **Next**.
5. Target database **`glue-transactionsdb`**, schedule **on demand**, **Next**, create the crawler.
6. **Run** it to test. It takes a few minutes.

Then a connection and a second crawler for Redshift:

7. **Connections → Create connection → Amazon Redshift → Next**. Pick your cluster, database `dev`, and your credentials. Name it something like `Redshift connection` and create it. Then open it, **Actions → Test connection**, pick the IAM role, and test.
8. Create a second crawler (`RedshiftLearnDataEngineeringCrawler`), data source **JDBC**, include path pointing at the Redshift table you want to catalog (for example `dev/public/bulkimport`). Same IAM role, same target database, on demand, create and run it.
9. Check **Tables** in the left pane to see the cataloged tables show up.

### Build and run the Glue ETL job

Before this job can write to Redshift, the target table has to exist. Run the create-table SQL from [`Redshift-Table-Create-Command.txt`](Code/Module-11/Redshift-Table-Create-Command.txt) in the Redshift query editor first. It creates the `bulkimport` table.

1. **Glue → ETL Jobs → Create a job with Visual ETL**.
2. **Source:** add an **S3** source, point it at your bucket and the CSV format, and set the IAM role. Click the **+** to add the next node.
3. **Transform:** add a **Change Schema** node and set the column data types so they match the target table.
4. **Target:** add a **Redshift** target, pick your connection, database, and the `bulkimport` table, and set the write mode to **APPEND** (insert).
5. Go to the **Job details** tab, name it something like `Redshift Bulkimport`, set the IAM role, and limit workers to **2** so it stays cheap.
6. **Save**, then **Run** the job.

When it finishes, query `bulkimport` in Redshift and you should see your 1000 rows. That's your batch pipeline done, sitting right next to the streaming one, both feeding the same warehouse.

---

## Troubleshooting

The stuff that actually goes wrong when you build this. Most of it is small and maddening, which is exactly why it's worth writing down.

**You get a 403 from the API when running `insert_template.py`**
You almost certainly left the resource name off the URL. The URL you copy from the stage in the console does not include your `main` resource, so you have to add it yourself. It should end in `/prod/main`, not just `/prod`.

**200 in the console test, but the CloudWatch log shows `KeyError: 'context'`**
Two causes. Either you skipped the "Method request passthrough" mapping template on the Integration Request (that's what creates the `context` and `body-json` fields), or you sent an empty request with no payload. Add the mapping template, and make sure you're actually sending data.

**`json-body not found` error**
Same root cause: the `application/json` mapping template isn't configured on the method. Go back to Integration Request and add it.

**Data flows through the API but nothing lands in S3 or DynamoDB**
Check that the consumer Lambda's Kinesis trigger points at the `APIData` stream and is enabled, and that its role has `myKinesisRead`. Also check the Lambda's own CloudWatch logs, a permissions error shows up there clearly.

**Firehose is configured but Redshift stays empty**
Work through these in order. Did you edit the bucket name in the copy command to your real bucket? Is the Firehose IP for your region added to the Redshift security group inbound rules? Is the cluster publicly accessible? Then query `STL_LOAD_ERRORS` in Redshift, which usually names the exact column or row that failed.

**S3 "bucket already exists" error**
Bucket names are globally unique across all of AWS, not just your account. Someone has that name. Add something specific like your username or a random suffix.

**Can't pause the Redshift cluster**
You need to create a snapshot first (Actions → Create snapshot). AWS blocks pausing a cluster that's never been snapshotted.

**The raw `Online_Retail.csv` looks like one giant line**
It uses old Mac carriage-return line endings, so some tools show it as a single line. You don't need to touch the raw file, use the cleaned versions in the repo, or run [`data_preprocessing.py`](Code/Module-06/data_preprocessing.py) yourself.

---

## Cleaning up

Don't skip this, it's the difference between a free learning project and a surprise bill.

- **Pause or delete the Redshift cluster.** This is the big one. Snapshot it, then pause it, or delete it entirely if you're done.
- **Delete the Kinesis stream** `APIData` so you stop paying for the provisioned shard.
- **Disable or delete the Lambda triggers and functions** if you're finished.
- **Empty and delete the S3 buckets** you created (raw, firehose staging, bulk import).
- **Delete the DynamoDB tables** `Customers` and `Invoices`.
- **Delete the Firehose stream.**

Leave the budget from module 05 in place. It costs nothing and it'll warn you if you missed something.

## Where to get help

If a step didn't work or something in here is out of date because AWS moved a button around again, open an issue on this repo and I'll take a look.

For the full course and more data engineering material:

- LearnDataEngineering Academy: [learndataengineering.com](https://learndataengineering.com)
- YouTube: [@andreaskayy](https://www.youtube.com/@andreaskayy)

Have fun building this. Once both pipelines are running and you can see the same data arrive two different ways, you'll have a real feel for how streaming and batch fit together, and that's worth a lot more than any single service on your resume.

Andreas
