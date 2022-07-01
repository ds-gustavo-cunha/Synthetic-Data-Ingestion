# import required libraries
import os
import boto3
import logging
import time
import json
import numpy as np
import requests
from datetime import datetime
from dotenv import load_dotenv
from synthetic_data_ingestion.sample_creator import SynthCustomers


class NpEncoder(json.JSONEncoder):
    """Custom encoder for json.dumps so as to avoid errors similar to:
    'Encoder Object of type int64 is not JSON serializable'"""

    def default(self, obj):
        # if item is numpy integer
        if isinstance(obj, np.integer):
            return int(obj)
        # if item is numpy float
        if isinstance(obj, np.floating):
            return float(obj)
        # if item is numpy array
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


class LambdaIngestor:
    def __init__(self, synth_customer_object: SynthCustomers) -> None:
        """Save the synth_customer_object input in the LambdaIngestor object

        Args
            synth_customer_object: a synthetic_data_ingestion.sample_creator.SynthCustomers object"""

        # instanciate logger
        self.logger = logging.getLogger("lambda_ingestion.py")

        # define log date in utc
        logging.Formatter.converter = time.gmtime

        # define logging configuration
        logging.basicConfig(
            filename=f"../logs/data_ingestion-{datetime.utcnow().date()}.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y:%m:%d %H:%M:%S",
        )

        # check if synth_customer_object param is
        # a synthetic_data_ingestion.sample_creator.SynthCustomers object
        if type(synth_customer_object) != SynthCustomers:

            # log a warning
            self.logger.critical(
                "LambdaIngestor object NOT instanciated: synth_customer_object param is NOT a synthetic_data_ingestion.sample_creator.SynthCustomers object"
            )

            raise Exception(
                "LambdaIngestor object NOT instanciated: synth_customer_object param must be a synthetic_data_ingestion.sample_creator.SynthCustomers object"
            )

        # define attributes of a SynthCustomers object
        # after generate_samples and generate_report methods
        # have been called
        required_attributes = {
            "gamma_shape",
            "gamma_scale",
            "poisson_lambda",
            "region",
            "region_weights",
            "gender",
            "gender_weights",
            "device",
            "device_weights",
            "groups",
            "logger",
            "group",
            "num_samples",
            "sampling_dict",
            "sampling_created",
            "random_shape",
            "random_scale",
            "random_lam",
            "gen_date_utc",
            "creation_report",
        }

        # check if synth_customer_object param has all attributes that
        # a SynthCustomers object has after generate_samples and generate_report methods
        # have been called
        if set(synth_customer_object.__dict__.keys()) != required_attributes:

            # log a warning
            self.logger.critical(
                "LambdaIngestor object NOT instanciated: synth_customer_object param doesn't have all required attributes"
            )

            raise Exception(
                "LambdaIngestor object NOT instanciated: synth_customer_object param doesn't have all required attributes"
            )

        # instanciate SynthCustomers object with the input params
        self.raw_report = synth_customer_object.creation_report

        # log an information
        self.logger.info(f"LambdaIngestor object successfully instanciated")

    def send_report_to_lambda(self) -> str:
        """Get the raw report, convert to json and send to AWS Lambda API"""

        # convert raw_report to a json report
        self._jsonify_report()

        # take environment variables from .env.
        load_dotenv()

        # try to send data to api
        try:

            # load environmental variables -> raise error if not found
            LAMBDA_URL = os.environ["AWS_LAMBDA_API"]

            # request header
            header = {"Content-type": "application/json"}

            # make request
            r = requests.post(
                url=f"{LAMBDA_URL}/sampling_report",
                data=self.json_report,
                headers=header,
            )

        # in case of errors when sending
        except Exception as e:
            # log an crictical
            self.logger.critical(
                f"send_report_to_lambda method raised the following error ---> {e}"
            )

            # message
            return f"send_report_to_lambda method raised the following error ---> {e}"

        # NO error when sending
        else:

            # log an information
            self.logger.info(
                f"send_report_to_lambda method successfully called: {r.json()}"
            )

            # response from lambda api
            return r.json()

    def _jsonify_report(self) -> None:
        """Get the raw report and convert it to a json"""

        # use NpEncoder to convert raw_report to a json report
        self.json_report = json.dumps(self.raw_report, cls=NpEncoder)

        # log an information
        self.logger.info(f"jsonify_report method successfully called")