import click
from cli.proxies import drfs, drdb
from cli.utils import utils

# CREATE-TABLE: python app.py table create -n table1 -db database-simon-1 -dr drfs-drive-org-simonorg1
# GET-TABLES: python app.py table get-all -db database-simon-1

@click.group()
def table():
    """
    DRDB Table CLI commands
    """
    pass

@table.command("create")
@click.option("-n", "--name")
@click.option("-db", "--db_name")
@click.option("-dr", "--drive_id")
def cmd_create_table(name, db_name, drive_id):

    # Create table folder in databases folder
    folder_name = "databases/{db_name}/{tbl_name}/".format(db_name=db_name, tbl_name=name)
    drfs.create_folder(drive_id, folder_name)

    # Create raw and processed folders
    drfs.create_folder(drive_id, "{path}raw/".format(path=folder_name))
    processed_folder = "{path}processed/".format(path=folder_name)
    drfs.create_folder(drive_id, processed_folder)

    # Create the table in drdb
    folder_path = "s3://{bucket}/{location}".format(bucket=drive_id, location=processed_folder)
    drdb.create_table(db_name, name, folder_path)

@table.command("get")
@click.option("-db", "--db_name")
@click.option("-t", "--table_name")
def cmd_get_table(db_name, table_name):
    print('todo')

@table.command("get-all")
@click.option("-db", "--db_name")
def cmd_get_tables(db_name):
    results = drdb.get_tables(db_name)
    print(utils.to_json(results))

