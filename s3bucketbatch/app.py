from json import dumps, loads
from random import choice as random_choice
from string import ascii_lowercase, digits

import cfnresponse
from boto3 import session as boto3_session


def random_string(size=8, chars=ascii_lowercase + digits):
    return "".join(random_choice(chars) for x in range(size))


def lambda_handler(event, context):
    response = cfnresponse.FAILED
    physicalResourceId = ""
    physicalResourceIdList = []
    response_data = {}

    session = boto3_session.Session()
    s3 = session.resource("s3")

    print(f"Event data: {dumps(event)}")

    if event["RequestType"] in ["Create", "Update"]:
        random_suffix = random_string()
        for targetnumber in range(1, 1 + int(event["ResourceProperties"]["Count"])):
            bucket_name = f"{event['ResourceProperties']['BucketName']}{targetnumber:03d}-{random_suffix}"
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": session.region_name},
            )
            physicalResourceIdList.append(bucket_name)
        response_data = {"NumberOfBucketsCreated": event["ResourceProperties"]["Count"]}
        physicalResourceId = dumps(physicalResourceIdList)
        response = cfnresponse.SUCCESS

    elif event["RequestType"] == "Delete":
        for resource in loads(event["PhysicalResourceId"]):
            bucket = s3.Bucket(resource)
            bucket.delete()

        response_data = {"NumberOfBucketsCreated": "0"}
        physicalResourceId = ""
        response = cfnresponse.SUCCESS

    cfnresponse.send(
        event, context, response, response_data, physicalResourceId=physicalResourceId
    )
