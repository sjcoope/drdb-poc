import boto3
from boto3.dynamodb.conditions import Key
import click
from cli.utils import utils

tablename = "drdb-state-example"
dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
table = dynamodb.Table(tablename)

# EXAMPLE COMMANDS
# ----------------------
# GET-JOBS-FOR-TABLE: python app.py job get -tb 52082-74bd-410e-9479-e468901503a8
# GET-JOBS-FOR-DATABASE: python app.py job get -db [DB-ID]
# GET-EVENTS: python app.py job get-events -j 456789-456789-456789

@click.group()
def job():
    """
    DRDB Job CLI commands
    """
    pass

@job.command("get")
@click.option("-db", "--database_id")
@click.option("-tb", "--table_id")
def cmd_get_jobs(database_id, table_id):
    if(table_id is None):
        response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("database#"+database_id) & Key("data").begins_with("job"))
    else:
        response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("table#"+table_id) & Key("data").begins_with("job"))
        print(response)

    print(utils.to_json(response["Items"]))

@job.command("get-events")
@click.option("-j", "--job-id")
def cmd_get_events(job_id):
    response = table.query(IndexName="gsi_1", KeyConditionExpression=Key("sk").eq("job#"+job_id))
    print(utils.to_json(response["Items"]))