import boto3
from cli.utils import utils

glue_client = boto3.client('glue')

def create_database(name):
    resp = glue_client.create_database(
        DatabaseInput={
            'Name': name
        }
    )

    utils.handle_response(resp, "database created: {name}".format(name=name))
    

def create_table(db_name, name, folder_path):
    resp = glue_client.create_table(
        DatabaseName=db_name,
        TableInput={
            'Name': name,
            "StorageDescriptor": {
                "Location": folder_path
            }
        }
    )

    utils.handle_response(resp, "table created: {name}".format(name=name))
