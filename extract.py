import json
import boto3

def extract():
    # name of the S3 bucket that contains the flat file that contains data
    BUCKET_NAME = "uber-analytics-archies"

    # name of the flat file
    FILE_NAME = "yellow_tripdata_2024-05.parquet"

    # config.json contains the access key and the access key id for AWS. The credentials are not included in the github repo.
    with open("config.json", "r", encoding='utf-8') as jsonfile:
        data = json.load(jsonfile)
        # create access object with correct credentials
        s3_access_object = boto3.resource(
            # the data is stored in a S3 bucket, so we need to access the S3 service to get data
            service_name='s3',
            region_name='ca-central-1',
            # main object in json file is 'credentials'
            aws_access_key_id=data["credentials"]["aws_access_key_id"],
            aws_secret_access_key=data["credentials"]["aws_secret_access_key"]
        )

    # using the access object, access the S3 bucket and download the file
    s3_access_object.Bucket(BUCKET_NAME).download_file(FILE_NAME, FILE_NAME)
