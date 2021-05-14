import boto3
from cli.utils import utils
from cli.entities.database import Database
from cli.entities.table import Table

glue_client = boto3.client('glue')
dynamodb = boto3.resource('dynamodb', region_name="us-east-2")
table = dynamodb.Table("drdb-state")

CATALOG_ID = "037453421035"

def create_database(name):
    resp = glue_client.create_database(
        DatabaseInput={
            'Name': name
        }
    )

    utils.handle_response(resp, "database created: {name}".format(name=name))
    

def create_table(db_name, name, folder_path):

    # TODO - Test that database exists in GDC and DDB

    resp = glue_client.create_table(
        DatabaseName=db_name,
        TableInput={
            'Name': name,
            "StorageDescriptor": {
                "Location": folder_path
            }
        }
    )

    # TODO - create table for database in DDb

    utils.handle_response(resp, "table created: {name}".format(name=name))

def get_instances(databaseId):
    # TODO - get all instances if databaseId is not passed
    print("Not Implemented")

# Will just query DDB for performance (not querying GDC)
def get_databases():
    results = []
    response = table.query(IndexName='gsi_1',KeyConditionExpression=Key('sk').eq(criteria))
    for item in response["Items"]:
        print(item)
        # results.append(Database.from_ddb(item))


    # GDC Query Option - Not selected for performance
    # -----------------------------------------------
    # resp = glue_client.get_databases(CatalogId=CATALOG_ID)
    # utils.handle_response(resp, "")

    # for db in resp["DatabaseList"]:
    #     results.append(Database.from_cli(db))
    
    return results

def get_database(db_name):
    resp = glue_client.get_database(
        CatalogId=CATALOG_ID,
        Name=db_name
    )
    utils.handle_response(resp, "")

    print(resp);

def get_tables(database_name):
    results = []

    resp = glue_client.get_tables(
        CatalogId=CATALOG_ID,
        DatabaseName=database_name
    )
    utils.handle_response(resp, "")

    for table in resp["TableList"]:
        results.append(Table.from_cli(table))

    return results

def rename_database(database_name_from, database_name_to):
    print('todo')

def rename_table(table_name_from, table_name_to):
    print('todo')
