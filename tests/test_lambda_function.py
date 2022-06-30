# load the required libraries
import os
import json
import boto3
import pytest
from moto import mock_s3
from dotenv import load_dotenv
from unittest.mock import patch
from fastapi.testclient import TestClient
from api.lambda_function import app


# instanciate TestClient to test FastAPI
client = TestClient(app)


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def fake_s3_bucket(aws_credentials, *args, **kwargs):
    """Pytest fixture that creates an fake s3 bucket on a
    fake AWS account.
    """
    # open moto mock with fake aws credentials
    with mock_s3(aws_credentials):
        # create a fake client
        s3 = boto3.client("s3")
        # create the required s3 bucket on fake account
        s3.create_bucket(Bucket=os.environ["AWS_S3_BUCKET"])
        # yield the fake bucket on fake client account
        yield s3


class TestFastAPI:
    def test_api_status_ok(self):
        """Check a successful GET request to / endpoint"""
        # make a GET request to / endpoint
        response = client.get("/")

        assert (response.status_code == 200) and (response.json() == "API in ON!")

    def test_save_report_wrong_input(self):
        """Check if api gives expected response in case
        POST request has wrong values"""

        # request header
        headers = {"Content-type": "application/json"}

        # request data
        data = {
            "group": "TREATMENT",
            "gamma_shape": "WRONG",
            "gamma_scale": 10,
            "poisson_lambda": 3,
            "date_interval": "[2022-06-28,2022-07-04] [extremes included]",
            "region": {"LAM": 0.2, "NAM": 0.2, "EUR": 0.2, "AFR": 0.2, "ASA": 0.2},
            "gender": {"MALE": 0.5, "FEMALE": 0.5},
            "device": {"MOBILE": 0.5, "COMPUTER": 0.5},
        }

        # convert dict input to json input
        data_json = json.dumps(data)

        # make a POST request to /sampling_report endpoint
        response = client.post("/sampling_report", data=data_json, headers=headers)

        assert response.status_code == 422  # Unprocessable Entity

    # patch os.environ so as to raise exception
    @patch(target="os.environ", side_effect=Exception)
    def test_save_report_wrong_venv(self, monkeypatch):
        """Check if api gives expected response in case
        its environmental variables are not available"""

        # request header
        headers = {"Content-type": "application/json"}

        # request data
        data = {
            "group": "TREATMENT",
            "gamma_shape": 20,
            "gamma_scale": 10,
            "poisson_lambda": 3,
            "date_interval": "[2022-06-28,2022-07-04] [extremes included]",
            "region": {"LAM": 0.2, "NAM": 0.2, "EUR": 0.2, "AFR": 0.2, "ASA": 0.2},
            "gender": {"MALE": 0.5, "FEMALE": 0.5},
            "device": {"MOBILE": 0.5, "COMPUTER": 0.5},
        }

        # convert dict input to json input
        data_json = json.dumps(data)

        # make a POST request to /sampling_report endpoint
        response = client.post("/sampling_report", data=data_json, headers=headers)

        assert response.json().startswith("The following error was raised on API: ")

    def test_save_report_ok(self, fake_s3_bucket):
        """Check a successful POST request to /sampling_report endpoint"""

        # request header
        headers = {"Content-type": "application/json"}

        # request data
        data = {
            "group": "TREATMENT",
            "gamma_shape": 20,
            "gamma_scale": 10,
            "poisson_lambda": 3,
            "date_interval": "[2022-06-28,2022-07-04] [extremes included]",
            "region": {"LAM": 0.2, "NAM": 0.2, "EUR": 0.2, "AFR": 0.2, "ASA": 0.2},
            "gender": {"MALE": 0.5, "FEMALE": 0.5},
            "device": {"MOBILE": 0.5, "COMPUTER": 0.5},
        }

        # convert dict input to json input
        data_json = json.dumps(data)

        # make a POST request to /sampling_report endpoint
        response = client.post("/sampling_report", data=data_json, headers=headers)

        assert response.json() == "HTTP status of report input to S3 bucket --> 200"
