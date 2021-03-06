
import click
from cli.commands import database, table, helper, ddbquery, instance, job

@click.group()
def main():
    """
    Simple CLI for AWS S3/Glue integration.
    """
    pass

main.add_command(instance.instance)
main.add_command(database.database)
main.add_command(table.table)
main.add_command(helper.helper)
main.add_command(ddbquery.ddbquery)
main.add_command(job.job)