
import click
from cli.commands import database, table, utils

@click.group()
def main():
    """
    Simple CLI for AWS S3/Glue integration.
    """
    pass

main.add_command(database.database)
main.add_command(table.table)
main.add_command(utils.utils)