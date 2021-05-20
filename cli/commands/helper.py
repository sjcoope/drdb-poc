from click import decorators
from cli.commands.table import table
import click
from cli.proxies import drfs, drdb
from cli.utils import utils, ddb_utils
from sampledata.helper import SampleData
from tqdm import tqdm
import boto3
from botocore.exceptions import ClientError
import time

# Example Commands:
# CREATE-TABLE: python app.py helper add-table-data -n table1 -db database-simon-1 -dr drfs-drive-org-simonorg1 -fc 10 -rc 50
# CREATE-DDB-TABLE: python app.py helper create-ddb-table -n table1

try:
    s3_resource = boto3.resource('s3')
    dynamodb = boto3.resource('dynamodb', region_name="ap-southeast-2")
except ClientError as err:
    raise RuntimeError("Failed to create boto3 client.\n" + str(err))

@click.group()
def helper():
    """
    DRDB Util CLI commands
    """
    pass

@helper.command("add-table-csv-data")
@click.option("-n", "--table_name")
@click.option("-db", "--db_name")
@click.option("-dr", "--drive_id")
@click.option("-fc", "--file_count")
@click.option("-rc", "--record_count")
def cmd_add_table_csv_data(table_name, db_name, drive_id, file_count, record_count):
    sd = SampleData(seed=123)

    folder_path = "databases/{db_name}/{table_name}/raw/".format(db_name=db_name,table_name=table_name)
    filename = "test-data-file"
    filebody = "{id},{name},{email},{dob},{is_active}\n"
    bodytext = ""
    for x in tqdm(range(int(file_count))):
        s3_object = s3_resource.Object(drive_id, folder_path + filename + str(x) + '.csv')

        bodytext += "id, name, email, dob, is_active\n"
        for y in range(int(record_count)):
            bodytext += filebody.format(
                id=sd.number(5),
                name=sd.fullname('us'),
                email=sd.email(),
                dob=sd.past_date(),
                is_active=sd.boolean()
            )

        s3_object.put(Body=bodytext)

@helper.command("create-ddb-table")
@click.option("-n", "--table_name")
def cmd_create_ddb_table(table_name):
    dynamodb.create_table(TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'pk',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'sk',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'pk',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'sk',
                'AttributeType': 'S'
            },
            {
            'AttributeName': 'data',
            'AttributeType': 'S'
            }
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'gsi_1',
                'KeySchema': [
                            {
                                'AttributeName': 'sk',
                                'KeyType': 'HASH'
                            },
                            {
                                'AttributeName': 'data',
                                'KeyType': 'RANGE'
                            },
                        ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    print("Please confirm that the table has created before attempting operations against it...")

@helper.command("create-ddb-table-data")
@click.option("-n", "--table_name")
def create_ddb_table_data(table_name):
    print('Creating sample data...')
    instances, databases, tables, jobs, operations = ddb_utils.create_sample_data()
 
    print('Creating node lists...')
    table_data = ddb_utils.build_adjacency_lists(instances, databases, tables, jobs, operations)
    filename = time.strftime("%Y%m%d-%H%M%S") + "_table_data.json"
    utils.save_to_file(filename, utils.to_json(table_data))

    print('Writing to DynamoDb...')
    ddb_utils.apply_table_data(table_name, table_data)

    print('Operation complete!')
