
from cli.commands.instance import instance
from os import error
import boto3
from boto3.dynamodb.conditions import Key
import uuid
from datetime import datetime
from cli.utils import utils
from cli.entities.database import Database
from cli.entities.table import Table

gdc_catalog_id = "037453421035"
table_name = "drdb-state"
date_format = "%Y-%m-%d-T%H:%M:%SZ"

glue_client = boto3.client('glue')
ddb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
ddb_resource = boto3.resource('dynamodb', region_name="ap-southeast-2")
table = ddb_resource.Table(table_name)

def get_instance_id(drive_id):
    resp = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("instance#"+drive_id))
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

def table_exists(drive_id, database_name, table_name):
    result = s3_client.list_objects(Bucket=drive_id, Prefix="databases/" + database_name + "/" + table_name)
    if(result): return True
    else: return False

# Improved implementation of test for table exists.
def table_exists_2(drive_id, database_name, table_name):
    prefix = "databases/" + database_name + "/" + table_name + "/"
    bucket = s3_resource.Bucket(drive_id)
    matches = list(bucket.objects.filter(Prefix=prefix, MaxKeys=1))
    if(any([w.key == prefix for w in matches])):
       return True
    else:
        return False

def get_instance(drive_id):
    resp = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("instance#"+drive_id))
    return resp["Items"]

def create_instance(drive_id, resource_id):
    resp = ddb_client.put_item(
        TableName=table_name,
        Item={
            "pk": {"S": "instance#" + str(uuid.uuid4()) },
            "sk": {"S": "instance#" + drive_id },
            "data": {"S": "resource#" + resource_id},
            "createdDate": {"S": datetime.today().strftime(date_format)},
            "drive_id": {"S": drive_id},
        }
    )

    utils.handle_response(resp, "dynamo table - instance created.")

def get_database(drive_id, name):
    instance_id = get_instance_id(drive_id)
    resp = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("instance#"+instance_id) & Key("data").eq("database#"+name))
    return resp["Items"]

def get_databases(drive_id):
    instance_id = get_instance_id(drive_id)
    resp = table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq("instance#"+instance_id) & Key("data").begins_with("database"))
    return resp["Items"]

def get_database_from_catalog(db_name):
    print('db_name', db_name)
    resp = glue_client.get_database(
        CatalogId=gdc_catalog_id,
        Name=db_name
    )
    utils.handle_response(resp, "")
    return resp

def create_database(name, drive_id):

    # Get instance Id from drive_id
    instance_id = get_instance_id(drive_id)

    # Create database in DynamoDB
    resp_ddb = ddb_client.put_item(
        TableName=table_name,
        Item={
            "pk": {"S": "database#" + str(uuid.uuid4()) },
            "sk": {"S": "instance#" + instance_id },
            "data": {"S": "database#" + name},
            "createdDate": {"S": datetime.today().strftime(date_format)},
            "name": {"S": name},
            "status": {"S": "ACTIVE"},
        }
    )

    utils.handle_response(resp_ddb, "dynamo database created: {name}".format(name=name))

    # Create database in glue
    resp_glue = glue_client.create_database(
        DatabaseInput={
            'Name': name
        }
    )
    utils.handle_response(resp_glue, "glue database created: {name}".format(name=name))

def get_table(drive_id, db_name, table_name):
    instance_id = get_instance_id(drive_id)
    database_id = get_database_id(instance_id, db_name)

    resp = table.query(IndexName='gsi_1', KeyConditionExpression=Key('sk').eq("database#"+database_id) & Key("data").eq("table#"+table_name))
    return resp["Items"]

def get_tables(drive_id, db_name):
    instance_id = get_instance_id(drive_id)
    database_id = get_database_id(instance_id, db_name)

    resp = table.query(IndexName='gsi_1', KeyConditionExpression=Key('sk').eq("database#"+database_id) & Key("data").begins_with("table"))
    return resp["Items"]

def get_tables_from_catalog(database_name):
    results = []

    resp = glue_client.get_tables(
        CatalogId=gdc_catalog_id,
        DatabaseName=database_name
    )
    utils.handle_response(resp, "")

    for table in resp["TableList"]:
        results.append(Table.from_cli(table))

    return results


def create_table(db_name, name, folder_path, drive_id):

    # TODO - Test that database exists in GDC and DDB

    # TODO - do we need to updte access patterns to get DB table just by name?
    # Get database id
    instance_id = get_instance_id(drive_id)
    database_id = get_database_id(instance_id, db_name)
    print(database_id)

    resp_ddb = ddb_client.put_item(
        TableName=table_name,
        Item={
            "pk": {"S": "table#" + str(uuid.uuid4()) },
            "sk": {"S": "database#" + database_id },
            "data": {"S": "table#" + name},
            "createdDate": {"S": datetime.today().strftime(date_format)},
            "name": {"S": name},
            "status": {"S": "ACTIVE"}
        }
    )

    utils.handle_response(resp_ddb, "dynamodb data created for table: {name}".format(name=name))

    resp_glue = glue_client.create_table(
        DatabaseName=db_name,
        TableInput={
            'Name': name,
            "StorageDescriptor": {
                "Location": folder_path
            }
        }
    )

    utils.handle_response(resp_glue, "glue data created for table: {name}".format(name=name))

def rename_database(database_name_from, database_name_to):
    print('todo')

def rename_table(table_name_from, table_name_to):
    print('todo')
