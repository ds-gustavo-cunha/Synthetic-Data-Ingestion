# import required libraries
import os
import pytest
import boto3
from moto import mock_dynamodb
from unittest.mock import patch, mock_open
from synthetic_data_ingestion.dynamodb_ingestion import DynamodbIngestor


class TestDynamodbIngestor:

    # mock "with open" clause and "os.listdir" function
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    def test_constructor_invalid_folder(
        self, mock_list_dir, mock_open  # self / lower mock / upper mock
    ):
        """test correct construction of DynamodbIngestor object
        given some mocked files"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # if a dynamodb object is successfuly instanciated,
        # then it must have the following attributes
        required_attrs = [
            "logger",
            "logs_folder",
            "all_logs",
            "_table_created_flag",
            "_logs_sent",
        ]

        assert set(dynamodb_ingestor.__dict__.keys()) == set(required_attrs)

    # mock "with open" clause, "os.listdir" and "boto3.resouce" functions
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    @patch(target="boto3.resource", side_effect=Exception)
    def test__create_client(
        self,
        boto_exception,
        mock_list_dir,
        mock_open,  # self / lower mock / interm mock / upper mock
    ):
        """test if _create_client method raises an error in case
        venvs (for boto3) are missing"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # make sure _create_conn_engine will raise and error
        with pytest.raises(Exception):
            # call _create_conn_engine method
            dynamodb_ingestor._create_client()
