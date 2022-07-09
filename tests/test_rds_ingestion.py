# import required libraries
import os
import random
import pytest
import numpy as np
from datetime import datetime
from unittest.mock import patch
from synthetic_data_ingestion.rds_ingestion import RdsIngestor
from synthetic_data_ingestion.sample_creator import SynthCustomers


# define samples to parameterize SynthCustomers class
samples = [(10, "CONTROL"), (100, "TREATMENT"), (1000, "CONTROL")]

# parameterize class
@pytest.mark.parametrize("num_samples,group", samples)
class TestRdsIngestor:
    def test_constructor_valid(self, num_samples, group):
        """test correct construction of RdsIngestor object
        given different values of num_samples and group params"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate RdsIngestor
        rds_ingestor = RdsIngestor(synth_customers)

        assert (rds_ingestor.synth_customers == synth_customers) and hasattr(
            rds_ingestor, "logger"
        )

    def test_constructor_invalid_no_report(self, num_samples, group):
        """test if constructor raises an error in case of
        incorrect input object (missing report data)"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()

        # make sure it will raise an error
        with pytest.raises(Exception):
            # instanciate RdsIngestor
            rds_ingestor = RdsIngestor(synth_customers)

    def test_constructor_invalid_type(self, num_samples, group):
        """test if constructor raises an error in case of
        incorrect input object type"""

        # make sure it will raise an error
        with pytest.raises(Exception):
            # instanciate RdsIngestor
            rds_ingestor = RdsIngestor("INVALID_INPUT")

    def test__create_conn_engine_venv_error(self, monkeypatch, num_samples, group):
        """Check if _create_conn_engine method raises an error in case
        venv variables are not found"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate RdsIngestor object
        rds_ingestor = RdsIngestor(synth_customers)

        # make sure it will raise and error
        with pytest.raises(Exception):
            # remove required one environmental variable
            with monkeypatch.delenv("AWS_RDB_ENDPOINT"):
                # call _create_conn_engine method
                rds_ingestor._create_conn_engine()

    def test__create_conn_engine_boto3_error(self, monkeypatch, num_samples, group):
        """Check if _create_conn_engine method raises an error in case
        of boto3 exceptions"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate RdsIngestor object
        rds_ingestor = RdsIngestor(synth_customers)

        # set a monkey patch so that when boto3.Session() is called
        # it raises an exception
        monkeypatch.setattr("boto3.Session", lambda: Exception)

        # call _create_conn_engine method
        assert rds_ingestor._create_conn_engine().startswith(
            "_create_conn_engine method NOT successful: raised error ---> "
        )

    def test__create_conn_success(self, monkeypatch, num_samples, group):
        """Check if _create_conn_engine method successfully create
        engine in case of correct input"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate RdsIngestor object
        rds_ingestor = RdsIngestor(synth_customers)

        # define a class for mock object
        class Mocker:
            def __init__(self, *args, **kwargs):
                # class constructor
                pass

            def client(self, *args, **kwargs):
                # client method  of Mocker object
                return "Okay"

        # define mocking function
        def mocking(*args, **kwargs):
            return Mocker()  # Mocker object

        # set a monkey patch so that when boto3.Session() is called
        # it call our mocker
        monkeypatch.setattr("boto3.Session", mocking)

        # call _create_conn_engine method
        assert rds_ingestor._create_conn_engine() is None

    def test__create_ingestion_schema_success(self, monkeypatch, num_samples, group):
        """Check if _create_ingestion_schema method is successfully called
        for different RdsIngestor() instances"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate RdsIngestor object
        rds_ingestor = RdsIngestor(synth_customers)

        # call _create_ingestion_schema method
        # if return is None -> successfully called
        assert rds_ingestor._create_ingestion_schema() is None

    def test_ingest_samples_error(self, monkeypatch, num_samples, group):
        """Check if _create_conn_engine method raises an error in case
        of engine connections problems"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate RdsIngestor object
        rds_ingestor = RdsIngestor(synth_customers)

        # set a monkey patch so that when sqlalchemy.engine.base.Engine is called
        # it raises an exception
        monkeypatch.setattr("sqlalchemy.engine.base.Engine", lambda: Exception)

        # call _create_conn_engine method
        assert rds_ingestor._create_conn_engine().startswith(
            "_create_conn_engine method NOT successful: raised error ---> "
        )

    # decorator to mock sql engine connect method -> raise exception
    @patch("sqlalchemy.engine.base.Engine.connect", side_effect=Exception)
    def test_ingest_samples_conn_error(self, mock_sqlalchemy, num_samples, group):
        """Check if ingest_samples method raises an error in case
        of engine connections problems"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate RdsIngestor object
        rds_ingestor = RdsIngestor(synth_customers)

        # call ingest_samples method
        assert rds_ingestor.ingest_samples().startswith(
            "ingest_samples method NOT successfully called: raised error --->"
        )

    # decorator to raise error when calling dataframe.to_sql -> raise exception
    @patch("pandas.core.frame.DataFrame.to_sql", side_effect=Exception)
    def test_ingest_samples_pandas_error(self, mock_pd_sql, num_samples, group):
        """Check if ingest_samples method raises an error in case
        of pd.to_sql errors"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate RdsIngestor object
        rds_ingestor = RdsIngestor(synth_customers)

        # mock sql engine connect method
        with patch("sqlalchemy.engine.base.Engine.connect") as mock_sql:
            # call ingest_samples method
            assert rds_ingestor.ingest_samples().startswith(
                "ingest_samples method NOT successfully called: raised error --->"
            )

    # mock callable for dataframe.to_sql
    def mock_pd_to_sql(*args, **kwargs):
        return "Okay"

    # decorator to call mock_to_sql when calling dataframe.to_sql
    @patch("pandas.core.frame.DataFrame.to_sql", mock_pd_to_sql)
    def test_ingest_samples_pandas_ok(self, num_samples, group):
        """Check if ingest_samples method returns the expected message
        in case no error is raised"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate RdsIngestor object
        rds_ingestor = RdsIngestor(synth_customers)

        # mock sql engine connect method
        with patch("sqlalchemy.engine.base.Engine.connect") as mock_sql:
            # call ingest_samples method
            assert (
                rds_ingestor.ingest_samples()
                == "ingest_samples method successfully called"
            )
