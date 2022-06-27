# Library imports
import os
import json
import boto3
import uvicorn
from mangum import Mangum
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI
from typing import Dict
from typing_extensions import TypedDict


# create a class to define the value types of input dictionary
# total = true -> must have all defined keys
class ReportValues(TypedDict, total=True):
    # value of group key = str
    group: str
    # value of gamma_shape key = int
    gamma_shape: int
    # value of gamma_scale key = int
    gamma_scale: int
    # value of poisson_lambda key = int
    poisson_lambda: int
    # value of date_interval key = str
    date_interval: str
    # value of region key is a dict whose keys are str and values are float
    region: Dict[str, float]
    # value of gender key is a dict whose keys are str and values are float
    gender: Dict[str, float]
    # value of device key is a dict whose keys are str and values are float
    device: Dict[str, float]


# Create the FastAPI object
app = FastAPI()


# simple endpoint to check if API is running on the cloud
@app.get("/")
def api_status():
    """Endpoint to check if API is working"""

    return "API in ON!"


@app.post("/sampling_report")  # define endpoint
def save_report(data: ReportValues):
    """Take the data sent on post request, validate input types and
    send report to a S3 bucket.

    Args
        data: a dictionary that follows ReportValues typing

    Return
        response_msg: a string with the status of report input trial on AWS S3 bucket"""

    # timestamp (in UTC) that API received the data
    timestamp = datetime.utcnow()
    # define date of timestamp
    timestamp_date = timestamp.date()
    # define datetime of timestamp
    # replacing empty space between date and time with "Z" (UTC time)
    timestamp_datetime = str(timestamp).replace(" ", "Z")

    # try to load variables and send data to S3
    try:
        # take environment variables from .env
        load_dotenv()
        # define bucket name
        bucket_name = os.environ["AWS_S3_BUCKET"]
        # define file name
        file_name = f"data_lake_raw/lambda_api/sampling_report/extracted_at={timestamp_date}/api_request_at={timestamp_datetime}.json"
        # content to be sent
        content = bytes(json.dumps(data).encode("UTF-8"))

        # create a resource service client for AWS S3
        s3 = boto3.client("s3")

        # put content on the required S3 bucket
        s3_put = s3.put_object(Bucket=bucket_name, Key=file_name, Body=content)

    # in case of errors
    except Exception as e:

        return f"The following error was raised on API: {e}"

    # data was successfully input in s3 bucket
    else:

        # get S3 input request status
        r_status = s3_put["ResponseMetadata"]["HTTPStatusCode"]
        # prepare reponse message
        response_msg = f"HTTP status of report input to S3 bucket --> {r_status}"

        return response_msg


# use Mangum adapter to run FastAPI in AWS Lambda
lambda_handler = Mangum(app)


# check if api.py if being called directly
if __name__ == "__main__":
    # Run the API on http://127.0.0.1:8000
    uvicorn.run(app, host="127.0.0.1", port=8000)
