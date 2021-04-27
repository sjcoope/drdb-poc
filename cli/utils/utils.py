def handle_response(response, message):
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if(status_code != 200):
        raise RuntimeError("Error: {status_code}".format(status_code=str(status_code)))
    else:
        print(message)