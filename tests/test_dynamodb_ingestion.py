# import required libraries
from asyncio import format_helpers
import os
import pytest
import boto3
from moto import mock_dynamodb
from unittest.mock import patch, mock_open
from synthetic_data_ingestion.dynamodb_ingestion import DynamodbIngestor


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def fake_dynamo_table(aws_credentials, *args, **kwargs):
    """Pytest fixture that creates an fake s3 bucket on a
    fake AWS account.
    """
    # open moto mock with fake aws credentials
    with mock_dynamodb(aws_credentials):
        # create a fake resource
        dynamodb = boto3.resource("dynamodb")
        # create a fake DynamoDB Table
        dynamodb_table = dynamodb.Table(os.environ["AWS_DYNAMODB_TABLE"])
        
        # yield the fake table on the fake client account
        yield dynamodb_table



class TestDynamodbIngestor:

    # mock "with open" clause and "os.listdir" function
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    def test_constructor_invalid_folder(
        self, mock_list_dir, mock_open 
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
    def test__create_client_boto_exception(
        self,
        boto_exception,
        mock_list_dir,
        mock_open, 
    ):
        """test if _create_client method raises an error in case
        venvs (for boto3) are missing"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # make sure _create_conn_engine will raise and error
        with pytest.raises(Exception):
            # call _create_conn_engine method
            dynamodb_ingestor._create_client()


    # mock "with open" clause, "os.listdir" 
    # and use "fake_dynamo_table" fixture
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    def test__create_client_okay(
        self,
        mock_open,
        mock_list_dir,
        fake_dynamo_table,  # self / lower mock / interm mock / fixture
    ):
        """test if _create_client method return the expected value
        in case there is no errors"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # call _create_client method
        assert dynamodb_ingestor._create_client() == None


    # mock "with open" clause, "os.listdir" 
    # and use "fake_dynamo_table" fixture
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    def test__create_table_table_created(
        self,
        mock_open,
        mock_list_dir,
        fake_dynamo_table, 
    ):
        """test if _create_client method return the expected value
        in case the table was already created"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # call _create_table method
        assert dynamodb_ingestor._create_table() == "Nothing was done once table is already created"

    # mock "with open" clause, "os.listdir" and "boto3.resouce" functions
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    @patch(target="boto3.resource", side_effect=Exception("Boto3 exception"))
    def test__create_table_boto_exception(
        self,
        boto_exception,
        mock_list_dir,
        mock_open, 
    ):
        """test if _create_client method raises an error in case
        venvs (for boto3) are missing"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # set creted flag to false so as to check create_table method
        dynamodb_ingestor._table_created_flag = False

        # make sure _create_conn_engine will raise and error
        with pytest.raises(Exception):
            # call _create_conn_engine method
            dynamodb_ingestor._create_table()


    # mock "with open" clause, "os.listdir" 
    # and use "fake_dynamo_table" fixture
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    def test__create_table_okay(
        self,
        mock_open,
        mock_list_dir,
        fake_dynamo_table, 
    ):
        """test if _create_client method return the expected value
        in case there is no error in table creation"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # set _table_created flag to false so as to try to create the table
        dynamodb_ingestor._table_created_flag = False

        # call _create_table method
        assert dynamodb_ingestor._create_table() == "_create_table method successfully called"


    # mock "with open" clause, "os.listdir" 
    # and use "fake_dynamo_table" fixture
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    def test__parse_logs_okay(
        self,
        mock_open,
        mock_list_dir,
        fake_dynamo_table, 
    ):
        """test if _parse_logs method return the expected value
        in case there is no error in table creation"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # call _create_table method
        assert dynamodb_ingestor._parse_logs() == "parse_logs method successfully called"


    # mock "with open" clause, "os.listdir" 
    # and use "fake_dynamo_table" fixture
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1 - msg2\n\nmsg3\n")
    def test__parse_logs_exception(
        self,
        mock_open,
        fake_dynamo_table, 
    ):
        """test if _parse_logs method return the expected value
        in case there is no error in table creation"""

        # patch os.listdir method so as to create DynamoIngestor object
        with patch(target="os.listdir", return_value=["log1", "log2"]):
            # instanciate dynamodb ingestor
            dynamodb_ingestor = DynamodbIngestor("mocking")

            # patch os.listdir method so as to raise error and test _parse_logs exception
            with patch(target="os.listdir", side_effect=Exception):
                # make sure _parse_logs will raise and error
                with pytest.raises(Exception):                
                    # call _parse_logs method
                    dynamodb_ingestor._parse_logs()


    # mock "with open" clause, "os.listdir" 
    # and use "fake_dynamo_table" fixture
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    def test_send_logs_okay(
        self,
        mock_open,
        mock_list_dir,
        fake_dynamo_table, 
    ):
        """test if send_logs method return the expected value
        in case there is no error in table creation"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # call _create_table method
        assert dynamodb_ingestor.send_logs() == "send_logs method successfully called"


    # mock "with open" clause, "os.listdir" 
    # and use "fake_dynamo_table" fixture
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    def test_send_logs_okay(
        self,
        mock_open,
        mock_list_dir,
        fake_dynamo_table, 
    ):
        """test if send_logs method return the expected value
        in case there is log were already sent"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # set _table_created flag to false so as to try to create the table
        dynamodb_ingestor._logs_sent = True

        # call _create_table method
        assert dynamodb_ingestor.send_logs() == "Nothing was done once log were already sent"


    # mock "with open" clause, "os.listdir" 
    # and use "fake_dynamo_table" fixture
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    @patch(target="os.remove", return_value="deleted")
    def test__delete_logs_okay(
        self,
        mock_open,
        mock_list_dir,
        mock_remove,
    ):
        """test if _delete_logs method return the expected value
        in case there is no error in table creation"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # call _create_table method
        assert dynamodb_ingestor._delete_logs() == "_delete_logs method successfully called: not log was deleted once none was already sent to DynamoDB"


    # mock "with open" clause, "os.listdir" 
    # and use "fake_dynamo_table" fixture
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    @patch(target="os.listdir", return_value=["log1", "log2"])
    @patch(target="os.remove", return_value="deleted")
    def test__delete_logs_okay(
        self,
        mock_open,
        mock_list_dir,
        mock_remove,
    ):
        """test if _delete_logs method return the expected value
        in case there is no error in table creation"""

        # instanciate dynamodb ingestor
        dynamodb_ingestor = DynamodbIngestor("mocking")

        # set _logs_sent flag
        dynamodb_ingestor._logs_sent = True

        # call _create_table method
        assert dynamodb_ingestor._delete_logs() == "_delete_logs method successfully called"


    # mock "with open" clause, "os.listdir" 
    # and use "fake_dynamo_table" fixture
    @patch(target="builtins.open", new_callable=mock_open, read_data="msg1\n\nmsg2\n")
    def test__delete_logs_exception(
        self,
        mock_open,
    ):
        """test if _delete_logs method return the expected value
        in case there is no error in table creation"""

        # patch os.listdir method so as to create DynamoIngestor object
        with patch(target="os.listdir", return_value=["log1", "log2"]):
            # instanciate dynamodb ingestor
            dynamodb_ingestor = DynamodbIngestor("mocking")

            # set _logs_sent flag
            dynamodb_ingestor._logs_sent = True

            # patch os.listdir method so as to raise error and test _parse_logs exception
            with patch(target="os.listdir", side_effect=Exception("list.dir exception")):
                # make sure _parse_logs will raise and error
                with pytest.raises(Exception):                
                    # call _parse_logs method
                    dynamodb_ingestor._delete_logs()
