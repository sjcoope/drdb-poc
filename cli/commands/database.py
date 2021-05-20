import click
from cli.proxies import drfs, drdb
from cli.utils import utils

# EXAMPLE COMMANDS
# ----------------------
# CREATE-DATABASE: python app.py database create -n db-main-2 -dr drfs-drive-main-2
# GET-DATABASE: python app.py database get -n db-main-2 -dr drfs-drive-main-2 
# GET-DATABASES: python app.py database get -dr drfs-drive-main-2
# GET-DATABASE-FROM-CATALOG: python app.py database get-dc -n db-main-2


@click.group()
def database():
    """
    DRDB Database CLI commands
    """
    pass

@database.command("create")
@click.option("-n", "--name")
@click.option("-dr", "--drive_id")
def cmd_create_database(name, drive_id):

    # Create folder in databases folder
    folder_name = "databases/{db_name}/".format(db_name=name)
    drfs.create_folder(drive_id, folder_name)

    # Create database in glue data catalog
    drdb.create_database(name, drive_id)

@database.command("get")
@click.option("-n", "--db_name")
@click.option("-dr", "--drive_id")
def cmd_get_database(db_name, drive_id):
    if(db_name is None):
        results = drdb.get_databases(drive_id)
    else:
        results = drdb.get_database(drive_id, db_name)
    
    print(utils.to_json(results))

@database.command("get-dc")
@click.option("-n", "--name")
def cmd_get_databases(name):
    results = drdb.get_database_from_catalog(name)
    print(utils.to_json(results))


