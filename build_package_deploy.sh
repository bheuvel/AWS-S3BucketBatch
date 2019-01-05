#!/usr/bin/env bash
set -e
# Configure your S3 bucket here:
S3_DEPLOYMENT_BUCKET=my-deployment-bucket-rand2ut79hlcns6qy

if aws s3 ls "s3://${S3_DEPLOYMENT_BUCKET}" 2>&1 | grep -q 'NoSuchBucket'
then
    echo "Make sure bucket \"${S3_DEPLOYMENT_BUCKET}\" exists,"
    echo "and 'aws s3 ls \"s3://${S3_DEPLOYMENT_BUCKET}\"' is able to list it"
else
    echo "Using \"s3://${S3_DEPLOYMENT_BUCKET}\" as deployment bucket"


    # Actual build, package and deploy commands:
    sam build
    sam package --s3-bucket ${S3_DEPLOYMENT_BUCKET} --output-template-file packaged.yaml
    sam deploy --template-file packaged.yaml --stack-name s3bucketbatch --capabilities CAPABILITY_IAM


fi