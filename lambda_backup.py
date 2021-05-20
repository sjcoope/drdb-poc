import json
import urllib.parse
import boto3
import re
from os import error
from boto3.dynamodb.conditions import Key
import uuid
from datetime import datetime

table_name = "drdb-state"
date_format = "%Y-%m-%d-T%H:%M:%SZ"

glue_client = boto3.client('glue')
ddb_client = boto3.client('dynamodb')
ddb_resource = boto3.resource('dynamodb', region_name="ap-southeast-2")
lambda_client = boto3.client('lambda')
table = ddb_resource.Table(table_name)

s3 = boto3.client('s3')
s3_resource = boto3.resource("s3") #Not ideal using both??

def get_instance_id(drive_id):
    resp = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("drive#"+drive_id))
    results = resp["Items"]
    if(results is None or len(results) != 1):
        raise RuntimeError("Error - cannot get instance_id")
    
    return results[0]["pk"][9:]

def get_database_id(instance_id, db_name):
    resp = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("instance#"+instance_id) & Key("data").begins_with("database#"+db_name))
    results = resp["Items"]
    if(results is None or len(results) == 0):
        raise RuntimeError("Error - cannot get database_id")
    
    return results[0]["pk"][9:]

def handle_response(response, message):
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if(status_code != 200):
        raise RuntimeError("Error: {status_code}".format(status_code=str(status_code)))
    else:
        print(message)

def create_table(db_name, name, folder_path, drive_id):

    # Get database id
    instance_id = get_instance_id(drive_id)
    database_id = get_database_id(instance_id, db_name)

    resp_ddb = ddb_client.put_item(
        TableName=table_name,
        Item={
            "pk": {"S": "table#" + str(uuid.uuid4()) },
            "sk": {"S": "database#" + database_id },
            "data": {"S": "database#" + name},
            "createdDate": {"S": datetime.today().strftime(date_format)},
            "name": {"S": name},
            "status": {"S": "ACTIVE"}
        }
    )

    handle_response(resp_ddb, "dynamodb data created for table: {name}".format(name=name))

    resp_glue = glue_client.create_table(
        DatabaseName=db_name,
        TableInput={
            'Name': name,
            "StorageDescriptor": {
                "Location": folder_path
            }
        }
    )

    handle_response(resp_glue, "glue data created for table: {name}".format(name=name))

def table_exists(drive_id, database_name, table_name):
    prefix = "databases/" + database_name + "/" + table_name + "/"
    bucket = s3_resource.Bucket(drive_id)
    matches = list(bucket.objects.filter(Prefix=prefix, MaxKeys=1))
    if(any([w.key == prefix for w in matches])):
       return True
    else:
        return False

def test_s3_access(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("S3 access check: OK")
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    test_s3_access(bucket, key)
    
    # Check for type of file import (supports 2 methods: 1 - file in root of database folder; 2 - file in table import folder)
    # Sample string: databases/db-main-1/table-1-1/import/test-data-2.csv
    db_name = ""
    table_name = ""
    import_mode = "" # DATABASE OR TABLE
    if("import" in key):
        results = re.search("^databases/([^/]+)/([^/]+)/import/(.*?([^/]+)/?)$", key)
        db_name = results.group(1)
        table_name = results.group(2)
        import_mode = "TABLE"
    else:
        results = re.search("^databases/([^/]+)/(.*?([^/]+)/?)$", key)
        db_name = results.group(1)
        import_mode = "DATABASE"
    
    # Get database and table ids (temp measure - could be done in spark job)
    dbquery = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("instance#"+instance_id) & Key("data").begins_with("database#"+database_name))
    
    if(import_mode == "TABLE")
    
    print("db_name=" + db_name)
    print("table_name=" + table_name)
    print("import_mode=" + import_mode)
    
    # TODO - Can call create table here if the import mode is database (so we create a table for a file)
    
    base_prefix = "{bucket}/databases/{db_name}/{table_name}/".format(bucket=bucket, db_name=db_name, table_name=table_name)
    
    payload = {
                "bucketName":bucket,
                "keyPrefix": base_prefix + "import/",
                "sourceFormat":"csv",
                "writeFormats":"parquet",
                "writeMode":"overwrite",
                "readerOptions":"header=true,inferSchema=true,delimiter=\t",
                "targetLocation":base_prefix + "processed/",
                "databaseName": db_name,
                "databaseId": "",
                "tableName": table_name,
                "tableId": "",
                "dynamoDBTable": "drdb-state",
                "importType": import_mode
            }
    
    print(payload)
    
    response = lambda_client.invoke(
        FunctionName="arn:aws:lambda:ap-southeast-2:037453421035:function:dr_data_converter_job",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )
    
    print(response)
    