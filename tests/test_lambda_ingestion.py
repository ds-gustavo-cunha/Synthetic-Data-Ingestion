# import required libraries
import os
import pytest
import requests
import numpy as np
from unittest.mock import patch
from synthetic_data_ingestion.sample_creator import SynthCustomers
from synthetic_data_ingestion.lambda_ingestion import LambdaIngestor, NpEncoder


# define samples to parameterize TestSynthCustomers class
samples = [(10, "CONTROL"), (100, "TREATMENT"), (1000, "CONTROL")]

# parameterize class
@pytest.mark.parametrize("num_samples,group", samples)
class TestLambdaIngestor:
    def test_constructor_valid(self, num_samples, group):
        """test correct construction of LambdaIngestor object
        given different values of num_samples and group params"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate LambdaIngestor
        lambda_ingestor = LambdaIngestor(synth_customers)

        assert hasattr(lambda_ingestor, "raw_report")

    def test_constructor_invalid_type(self, num_samples, group):
        """test if constructor raises an error in case of
        incorrect input object type"""

        # make sure it will raise an error
        with pytest.raises(Exception):
            # instanciate RdsIngestor
            LambdaIngestor("INVALID_INPUT")

    def test_constructor_invalid_no_report(self, num_samples, group):
        """test if constructor raises an error in case of
        incorrect input object (missing report data)"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()

        # make sure it raises and error
        with pytest.raises(Exception):
            # instanciate RdsIngestor
            LambdaIngestor(synth_customers)

    def test__jsonify_report_okay(self, num_samples, group):
        """test if expect json result is correct"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate LambdaIngestor
        lambda_ingestor = LambdaIngestor(synth_customers)

        # overwrite raw_report attribute to test it
        lambda_ingestor.raw_report = {"X": 1, "Y": 1.5, "Z": np.array([])}

        # conver _raw_report attribute to json
        lambda_ingestor._jsonify_report()

        assert (
            lambda_ingestor.json_report == '{"X": 1, "Y": 1.5, "Z": []}'
        ) and isinstance(lambda_ingestor.json_report, str)

    def test_send_report_to_lambda_okay(self, num_samples, group, monkeypatch):
        """check if send_report_to_lambda method
        returns the expected value in case of no errors"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate LambdaIngestor
        lambda_ingestor = LambdaIngestor(synth_customers)

        # define a class to mock requests.post
        class Monkey:
            def __init__(self, *args, **kwargs):
                # class constructor
                self.r = "Okay"

            def json(self):
                # return r atribute
                return self.r

        # set a monkey patch so that when requests.post is called
        monkeypatch.setattr("requests.post", Monkey)

        assert lambda_ingestor.send_report_to_lambda() == "Okay"

    @patch(target="requests.post", side_effect=Exception("post error"))
    def test_send_report_to_lambda_request_raise_error(
        self, mock_post_error, num_samples, group
    ):
        """check if send_report_to_lambda method raise error
        in case requests.post not successful"""

        # instanciate SynthCustomers object given the num_samples and group params
        # and generate samples and report
        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        # instanciate LambdaIngestor
        lambda_ingestor = LambdaIngestor(synth_customers)

        assert (
            lambda_ingestor.send_report_to_lambda()
            == "send_report_to_lambda method raised the following error ---> post error"
        )


class TestNpEncoder:
    def test_default_int(self):
        """test default encoder"""
        assert NpEncoder().default(np.array([1])[0]) == 1

    def test_default_float(self):
        """test default jsonifiing"""
        assert NpEncoder().default(np.array([1.5])[0]) == 1.5

    def test_default_array(self):
        """test default jsonifiing"""
        assert NpEncoder().default(np.array([1, 1.5])) == [1, 1.5]
