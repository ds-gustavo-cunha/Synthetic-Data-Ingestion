# import required libraries
import pytest
from api.lambda_function import app
from fastapi.testclient import TestClient


# creates a request session with the api object
client = TestClient(app)

# define invalid input
invalid_input = {
    "group": "TREATMENT",
    "gamma_shape": 1,
    "gamma_scale": "INVALID_TYPE",
    "poisson_lambda": 3,
    "date_interval": "[2022-06-26,2022-07-02] [extremes included]",
    "region": {"LAM": 0.2, "NAM": 0.2, "EUR": 0.2, "AFR": 0.2, "ASA": 0.2},
    "gender": "INVALID_TYPE",
    "device": {"MOBILE": 0.5, "COMPUTER": 0.5},
}


# define valid input
valid_input = {
    "group": "TREATMENT",
    "gamma_shape": 1,
    "gamma_scale": 10,
    "poisson_lambda": 3,
    "date_interval": "[2022-06-26,2022-07-02] [extremes included]",
    "region": {"LAM": 0.2, "NAM": 0.2, "EUR": 0.2, "AFR": 0.2, "ASA": 0.2},
    "gender": {"MALE": 0.5, "FEMALE": 0.5},
    "device": {"MOBILE": 0.5, "COMPUTER": 0.5},
}


def test_root_ok():
    """Test endpoint that tells if API is working"""

    # set the HTTP metho to GET on endpoint /
    response = client.get("/")

    assert response.status_code == 200
    assert response.json() == "API in ON!"


def test_sampling_report_wrong_input_format():
    """Test if api gives bad response in case input has invalid type"""

    # set the HTTP metho to POST on endpoint /sampling_report
    response = client.post("/sampling_report", json=invalid_input)

    assert response.status_code == 422


def test_sampling_report_venv_variable_missing(monkeypatch):
    """Test if api gives bad response in case of
    s3 bucket name is not available in .env file"""

    # make sure a KeyError is raised
    # once the AWS_S3_BUCKET venv variable won't be available
    with pytest.raises(KeyError):
        # remove required one environmental variable
        with monkeypatch.delenv("AWS_S3_BUCKET"):
            # set the HTTP method to POST
            # on endpoint /sampling_report
            # with valid input
            response = client.post("/sampling_report", json=valid_input)
