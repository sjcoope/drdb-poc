import click
from cli.proxies import drfs, drdb
from cli.utils import utils

# EXAMPLE COMMANDS
# ----------------------
# CREATE-INSTANCE: python app.py instance create -dr drfs-drive-main-2 -r Org123
# GET-INSTNACE: python app.py instance get -dr drfs-drive-main-2

@click.group()
def instance():
    """
    DRDB Instance CLI commands
    """
    pass

@instance.command("create")
@click.option("-dr", "--drive_id")
@click.option("-r", "--resource_id")
def cmd_create_instance(drive_id, resource_id):
    # Create database in glue data catalog
    drdb.create_instance(drive_id, resource_id)

@instance.command("get")
@click.option("-dr", "--drive_id")
def cmd_get_instance(drive_id):
    results = drdb.get_instance(drive_id)
    print(utils.to_json(results))