import click
from cli.proxies import drfs, drdb
from cli.utils import utils
import re

# EXAMPLE COMMANDS
# ----------------------
# CREATE-TABLE: python app.py table create -n table-2-1 -db db-main-2 -dr drfs-drive-main-2
# GET-TABLE: python app.py table get -n table-2-1 -db db-main-2 -dr drfs-drive-main-2 
# GET-TABLES: python app.py table get -db db-main-2 -dr drfs-drive-main-2 
# GET-TABLES-FROM-CATALOG: python app.py table get-dc -db db-main-2

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
    drfs.create_folder(drive_id, "{path}import/".format(path=folder_name))
    drfs.create_folder(drive_id, "{path}error/".format(path=folder_name))
    processed_folder = "{path}processed/".format(path=folder_name)
    drfs.create_folder(drive_id, processed_folder)

    # Create the table in drdb
    folder_path = "s3://{bucket}/{location}".format(bucket=drive_id, location=processed_folder)
    drdb.create_table(db_name, name, folder_path, drive_id)

@table.command("get")
@click.option("-dr", "--drive_id")
@click.option("-db", "--db_name")
@click.option("-n", "--table_name")
def cmd_get_table(drive_id, db_name, table_name):
    if(table_name is not None):
        results = drdb.get_table(drive_id, db_name, table_name)
    else:
        results = drdb.get_tables_from_catalog(drive_id, db_name)

    print(utils.to_json(results))

@table.command("get-dc")
@click.option("-db", "--db_name")
def cmd_get_tables(db_name):
    results = drdb.get_tables_from_catalog(db_name)
    print(utils.to_json(results))
