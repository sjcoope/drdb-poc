import click
from cli.proxies import drfs, drdb
from cli.utils import utils

# CREATE-DATABASE: python app.py database create -n DB_NAME -d drfs-drive-org-simonorg1
# GET-DATABASES: python app.py database get-all
# GET-DATABASE: python app.py database get --db database-simon-1

@click.group()
def database():
    """
    DRDB Database CLI commands
    """
    pass

@database.command("create")
@click.option("-n", "--name")
@click.option("-d", "--drive_id")
def cmd_create_database(name, drive_id):

    # Create folder in databases folder
    folder_name = "databases/{db_name}/".format(db_name=name)
    drfs.create_folder(drive_id, folder_name)

    # Create database in glue data catalog
    drdb.create_database(name)

@database.command("get-all")
@click.option("-d", "--drive_id")
def cmd_get_databases(drive_id):
    results = drdb.get_databases()
    print(utils.to_json(results))

@database.command("get")
@click.option("-d", "--drive_id")
@click.option("-db", "--db_name")
def cmd_get_database(drive_id, db_name):
    results = drdb.get_database(drive_id, db_name)
    print(utils.to_json(results))
