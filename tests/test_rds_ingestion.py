# import required libraries
import os
import random
import pytest
import numpy as np
from datetime import datetime
from synthetic_data_ingestion.rds_ingestion import RdsIngestor
from synthetic_data_ingestion.sample_creator import SynthCustomers


# define samples to parameterize TestSynthCustomers class
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

        assert RdsIngestor(synth_customers) is not None

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

        # make sure _create_conn_engine will raise and error
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

        # make sure _create_conn_engine will raise and error
        with pytest.raises(Exception):
            # call _create_conn_engine method
            rds_ingestor._create_conn_engine()

    def test__create_ingestion_schema_success_call(
        self, monkeypatch, num_samples, group
    ):
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
        """Check if ingest_samples method raises an error in case
        of engine connections problems"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate RdsIngestor object
        rds_ingestor = RdsIngestor(synth_customers)

        # set a monkey patch so that when boto3.Session() is called
        # it raises an exception
        monkeypatch.setattr("sqlalchemy.engine.base.Engine", lambda: Exception)

        # make sure _create_conn_engine will raise and error
        with pytest.raises(Exception):
            # call _create_conn_engine method
            rds_ingestor._create_conn_engine()
