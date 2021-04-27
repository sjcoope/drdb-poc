import click
import boto3
from botocore.exceptions import ClientError, WaiterError

# Create boto3 client.
try:
    s3_resource = boto3.resource('s3')
except ClientError as err:
    log("Failed to create boto3 client.\n" + str(err))

@click.group()
def main():
    """
    Simple CLI of Utilities for DR File System (DRFS)
    """
    pass

@main.command("create-test-data")
@click.option("-d", "--drive")
@click.option("-c", "--count")
@click.option("-f", "--folder")
def create_test_data(drive, count, folder):
    #TODO - Check drive is a DRFS drive (i.e. by tag name)
    #TODO - Validate parameters

    fileName = 'test-file'
    fileBody = b'Here is some test text'

    states = ['active','activehidden','quarantined']
    statecount = 0

    for x in range(int(count)):
        s3Object = s3_resource.Object(drive, folder + "/" + fileName + str(x) + ".txt")

        if(statecount >= 3): statecount = 0
        tagValue = "State=" + states[statecount]
        statecount = statecount + 1

        s3Object.put(Body=fileBody, Tagging=tagValue)    
