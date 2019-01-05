from json import dumps

import cfnresponse
from boto3 import client as boto3_client
from moto import mock_s3
import pytest
import mock

from s3bucketbatch import app

# from s3bucketbatch.app import random_string

BUCKET_NAME = "cr-test-exempligratia"
NUMBER_OF_BUCKETS = 10
RANDOM_STRING = "111"


@pytest.fixture()
def cfn_event(resources_string, request):
    """
        CloudFormation event. By default a Create event.
        Adjust for Update and Delete accordingly
    """
    event_data = {
        "RequestType": request.param,
        "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:MyLambda-HC5AK3EGCVLX",
        "ResponseURL": "http://pre-signed-S3-url-for-response",
        "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/MyStack/guid",
        "RequestId": "unique id for this create request",
        "ResourceType": "Custom::TestResource",
        "LogicalResourceId": "MyTestResource",
        "ResourceProperties": {
            "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:MyLambda-HC5AK3EGCVLX",
            "Count": str(NUMBER_OF_BUCKETS),
            "BucketName": BUCKET_NAME,
        },
        "PhysicalResourceId": "",
    }

    if request.param == "Update":
        event_data["OldResourceProperties"] = {
            "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:MyLambda-HC5AK3EGCVLX",
            "BucketName": "one-of-many-buckets",
            "Count": "10",
        }

    if request.param in ["Update", "Delete"]:
        event_data["PhysicalResourceId"] = resources_string

    return event_data


@pytest.fixture()
def cfn_context():
    """ Context object, blank for now """
    return ""


@pytest.fixture()
def cfn_response_data(request):
    """ Response data, containing the 'read attributes' on the custom resource """
    return {"NumberOfBucketsCreated": str(request.param)}


@pytest.fixture()
def resources_string():
    """ The Phusical ID string containing the bucket names """
    buckets = []
    for bucket_number in range(1, 1 + int(NUMBER_OF_BUCKETS)):
        buckets.append(f"{BUCKET_NAME}{bucket_number:03d}-{RANDOM_STRING}")
    return dumps(buckets)


@pytest.fixture()
def buckets():
    """ Create the buckets for testing of removal """

    @mock_s3
    def bucket_list():
        s3_client = boto3_client("s3")
        for targetnumber in range(1, 1 + int(NUMBER_OF_BUCKETS)):
            bucket_name = f"{BUCKET_NAME}{targetnumber:03d}-{RANDOM_STRING}"
            s3_client.create_bucket(Bucket=bucket_name)
        return s3_client

    return bucket_list


@pytest.mark.parametrize(
    "cfn_event, cfn_response_data",
    [("Create", NUMBER_OF_BUCKETS), ("Update", NUMBER_OF_BUCKETS)],
    indirect=True,
)
@mock.patch("s3bucketbatch.app.random_string", return_value=RANDOM_STRING)
@mock.patch("cfnresponse.send")
@mock_s3
def test_create_update(
    mocked_send, cfn_context, cfn_event, cfn_response_data, resources_string
):
    # Mock S3
    s3 = boto3_client("s3")

    # Call the Lambda function
    app.lambda_handler(cfn_event, cfn_context)

    # Test 3 buckets have been "created"
    assert (
        len(s3.list_buckets()["Buckets"]) == NUMBER_OF_BUCKETS
    ), f"Expected {NUMBER_OF_BUCKETS} S3 Buckets; {len(s3.list_buckets()['Buckets'])} created"
    # Test cfnresponse called with expected parameters
    mocked_send.assert_called_with(
        cfn_event,
        cfn_context,
        cfnresponse.SUCCESS,
        cfn_response_data,
        physicalResourceId=resources_string,
    )


@pytest.mark.parametrize("cfn_event, cfn_response_data", [("Delete", 0)], indirect=True)
@mock.patch("cfnresponse.send")
@mock_s3
def test_delete(
    mocked_send, cfn_context, cfn_event, cfn_response_data, resources_string, buckets
):
    s3 = buckets()

    # Call the Lambda function
    app.lambda_handler(cfn_event, cfn_context)

    # Test all/3 buckets have been "deleted"
    assert (
        len(s3.list_buckets()["Buckets"]) == 0
    ), f"Expected {0} S3 Buckets; {len(s3.list_buckets()['Buckets'])} left after deletion"
    # Test cfnresponse called with expected parameters
    mocked_send.assert_called_with(
        cfn_event,
        cfn_context,
        cfnresponse.SUCCESS,
        cfn_response_data,
        physicalResourceId="",
    )
