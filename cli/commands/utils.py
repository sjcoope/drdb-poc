from click import decorators
from cli.commands.table import table
import click
from cli.proxies import drfs, drdb
from sampledata.helper import SampleData
import boto3
from tqdm import tqdm
from botocore.exceptions import ClientError

# CREATE-TABLE: python app.py utils add-table-data -n table1 -db database-simon-1 -dr drfs-drive-org-simonorg1 -rc 10

try:
    s3_resource = boto3.resource('s3')
except ClientError as err:
    raise RuntimeError("Failed to create boto3 client.\n" + str(err))

@click.group()
def utils():
    """
    DRDB Util CLI commands
    """
    pass

@utils.command("add-table-data")
@click.option("-n", "--table_name")
@click.option("-db", "--db_name")
@click.option("-dr", "--drive_id")
@click.option("-fc", "--file_count")
@click.option("-rc", "--record_count")
def cmd_add_table_data(table_name, db_name, drive_id, file_count, record_count):
    sd = SampleData(seed=123)

    folder_path = "databases/{db_name}/{table_name}/raw/".format(db_name=db_name,table_name=table_name)
    print(folder_path)
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
