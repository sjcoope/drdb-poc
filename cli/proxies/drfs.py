import boto3
from cli.utils import utils

s3_client = boto3.client('s3')

def create_folder(drive_id, folder_path):
    resp = s3_client.put_object(
        Bucket=drive_id, 
        Key=folder_path
    )

    utils.handle_response(resp, "folder created: {folder_path}".format(folder_path=folder_path))
