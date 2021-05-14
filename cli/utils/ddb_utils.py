import datetime
import boto3
import datetime
import copy
from botocore.exceptions import ClientError
from sampledata.helper import SampleData

date_format_string = "%Y-%m-%d-T%H:%M:%SZ"

try:
    s3_resource = boto3.resource('s3')
    dynamodb = boto3.resource('dynamodb', region_name="ap-southeast-2")
except ClientError as err:
    raise RuntimeError("Failed to create boto3 client.\n" + str(err))

def create_sample_data():
    min_datetime = datetime.datetime.now() - datetime.timedelta(days=365)
    max_datetime = datetime.datetime.now()
    entity_statuses = ['QUARANTINED', 'ACTIVE', 'JOB_RUNNING', 'SUSPENDED', 'ERRORS', 'WARNINGS']
    job_statuses = ['ACTIVE', 'IGNORE']
    job_status_count = 2
    event_types = ['ERROR', 'WARNING', 'INFO']
    event_type_count = 3
    entity_status_count = 5
    instance_count = 50
    database_count = 500
    table_count = 10000
    job_count = 10000
    event_count = 20000

    sd = SampleData(seed=123)
    instances = []
    databases = []
    tables = []
    jobs = []
    events = []

    for i in range(1, instance_count + 1):
        instances.append({
            "id": str(i),
            "name": sd.words(1, 3),
            "createdDate": sd.datetime_between(min_datetime, max_datetime),
            "resourceId": str(sd.number(5))
        })

    for d in range(1, database_count + 1):
        instance = instances[sd.int(0, instance_count-1)]
        created_date = sd.datetime_between(min_datetime, max_datetime)
        databases.append({
            "id": str(d),
            "name": sd.word(),
            "createdDate": created_date,
            "lastUpdatedDate": sd.datetime_between(instance["createdDate"], max_datetime),
            "status": entity_statuses[sd.int(0,entity_status_count)],
            "instanceId": instance["id"]
        })

    for t in range(table_count):
        database = databases[sd.int(0, database_count-1)]
        created_date = sd.datetime_between(min_datetime, max_datetime)
        tables.append({
            "id": str(t),
            "name": sd.word(),
            "createdDate": created_date,
            "lastUpdatedDate": sd.datetime_between(database["createdDate"], max_datetime),
            "status": entity_statuses[sd.int(0,entity_status_count)],
            "databaseId": database["id"]
        })
    
    for j in range(job_count):
        entity_type = ["database", "table"][sd.int(0,1)]
        entity_id = 0
        if(entity_type == "database"):
            entity_id = databases[sd.int(0, database_count-1)]["id"]
        if(entity_type == "table"):
            entity_id = tables[sd.int(0, database_count-1)]["id"]

        jobs.append({
            "id": str(j),
            "entityType": entity_type,
            "entityId": entity_id,
            "status": job_statuses[sd.int(0, job_status_count-1)],
            "createdDate": sd.datetime_between(min_datetime, max_datetime)
        })

    for e in range(event_count):
        job = jobs[sd.int(0, job_count-1)]
        events.append({
            "id": str(e),
            "description": sd.sentence(),
            "createdDate": sd.datetime_between(job["createdDate"], max_datetime),
            "type": event_types[sd.int(0, event_type_count-1)],
            "jobId": job["id"]
        })

    # Convert all data types to string (post processing)
    return clean_types(instances, databases, tables, jobs, events)

def clean_types(instances, databases, tables, jobs, events):
    lists = [instances, databases, tables, jobs, events]
    date_types = ["createdDate", "lastUpdatedDate"]
    for list in lists:
        for item in list:
            for type in date_types:
                if(type in item):
                    item[type] = item[type].strftime(date_format_string)

    return instances, databases, tables, jobs, events

def build_adjacency_lists(instances, databases, tables, jobs, events):
    lists = []
    lists += build_node_list(instances, "id", "instance", "resourceId", "resource", "instance#createdDate")
    lists += build_node_list(databases, "id", "database", "instanceId", "instance", "database#name")
    lists += build_node_list(tables, "id", "table", "databaseId", "database", "table#name")
    lists += build_node_list(jobs, "id", "job", "entityId", "entityType", "job#createdDate")
    lists += build_node_list(events, "id", "event", "jobId", "job", "event#createdDate")
    return lists

def format_node_value(value, prefix):
    return value if (not prefix) else "{prefix}#{value}".format(prefix=prefix, value=value)

def build_node_list(node_rows, pk, pk_prefix, sk, sk_prefix, gs1_sk):
    partition = []
    for row in node_rows:
        node_row = copy.deepcopy(row)
        node_row["pk"] = format_node_value(get_value(node_row, pk, False), get_value(node_row, pk_prefix, False))
        node_row["sk"] = format_node_value(get_value(node_row, sk, False), get_value(node_row, sk_prefix, False))
        node_row["data"] = format_node_value(build_composite_sort_key(node_row, gs1_sk), None)
        partition.append({'PutRequest': {'Item': node_row}})
    return partition

def get_value(array, key, keep_value):
    if(keep_value):
        return array[key] if key in array else key
    else:
        return array.pop(key, key)

def build_composite_sort_key(row, keyname):
    elements = keyname.split("#")
    keys = [get_value(row, element, True) for element in elements]
    
    # Convert required types
    for x in range(len(keys)):
        val = keys[x]
        if(type(val) == datetime.datetime):
            keys[x] = val.strftime(date_format_string)

    return "#".join(keys)

def apply_table_data(table_name, data):
    while len(data) > 25:
        batch_write(table_name, data[:24])
        data = data[24:]         
    if data:
        batch_write(table_name, data)

def batch_write(table_name, data):
    dynamodb.batch_write_item(
        RequestItems={table_name : data}
    )