import boto3
from boto3.dynamodb.conditions import Key
from cli.utils import utils
import argparse
import click

tablename = "drdb-state-example"
dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
table = dynamodb.Table(tablename)

@click.group()
def ddbquery():
    """
    CLI commands for example DDB queries.
    """
    pass

def log_output(message):
    print(utils.to_json(message))

@ddbquery.command("get-instances")
@click.option("-r", "--resource_id")
def cmd_get_instances(resource_id):
    response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("resource#"+resource_id) & Key("data").begins_with("instance"))
    log_output(response["Items"])

@ddbquery.command("get-instance")
@click.option("-i", "--instance_id")
def cmd_get_instance_data(instance_id):
    response = table.query(KeyConditionExpression=Key("pk").eq("instance#"+instance_id))
    log_output(response["Items"])

@ddbquery.command("get-databases")
@click.option("-i", "--instance-id")
@click.option("-n", "--database_name")
def cmd_get_databases(instance_id, database_name):
    if(database_name is None):
        response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("instance#"+instance_id))
    else:
        response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("instance#"+instance_id) & Key("data").begins_with("database#"+database_name))
    
    log_output(response["Items"])

@ddbquery.command("get-database")
@click.option("-d", "--database-id")
def cmd_get_database(database_id):
    response = table.query(KeyConditionExpression=Key("pk").eq("database#"+database_id))
    log_output(response["Items"])

@ddbquery.command("get-database-jobs")
@click.option("-d", "--database_id")
@click.option("-dt", "--date")
def cmd_get_database_jobs(database_id, date):
    if(date is None):
        response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("database#"+database_id) & Key("data").begins_with("job"))
    else:
        response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("database#"+database_id) & Key("data").begins_with("job#"+date))

    log_output(response["Items"])
    
@ddbquery.command("get-tables")
@click.option("-d", "--database-id")
def cmd_get_tables(database_id, date):
    response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("database#"+database_id))
    log_output(response["Items"])

@ddbquery.command("get-table")
@click.option("-t", "--table-id")
def cmd_get_tables(table_id):
    response = table.query(KeyConditionExpression=Key("pk").eq("table#"+table_id))
    log_output(response["Items"])

@ddbquery.command("get-table-jobs")
@click.option("-t", "--table_id")
@click.option("-dt", "--date")
def cmd_get_table_jobs(table_id, date):
    if(date is None):
        response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("table#"+table_id) & Key("data").begins_with("job"))
    else:
        response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("table#"+table_id) & Key("data").begins_with("job#"+date))

    log_output(response["Items"])

@ddbquery.command("get-events_by_job")
@click.option("-j", "--job-id")
def cmd_get_events_by_job(job_id):
    response = table.query(KeyConditionExpression=Key("pk").eq("job#"+job_id))
    log_output(response["Items"])