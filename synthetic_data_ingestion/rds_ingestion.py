# import required libraries
import sys
import os
import boto3
import logging
import time
import pandas as pd
from datetime import datetime
from os.path import basename
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import Float, SmallInteger, String
from synthetic_data_ingestion.sample_creator import SynthCustomers


class RdsIngestor:
    def __init__(self, synth_customer_object, log_folder: str = None):
        """Class constructor. It will instanciate a SynthCustomers object.

        Args
            synth_customer_object: a synthetic_data_ingestion.sample_creator.SynthCustomers object
            log_folder: a string with the path to store logs"""

        # instanciate logger
        self.logger = logging.getLogger("rds_ingestion.py")

        # define log date in utc
        logging.Formatter.converter = time.gmtime

        # check if user input a folder to store logs
        if log_folder is None:
            # set a default folder
            log_folder = "../logs"

        # define logging configuration
        logging.basicConfig(
            filename=f"{log_folder}/data_ingestion-{datetime.utcnow().date()}.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y:%m:%d %H:%M:%S",
        )

        # check if synth_customer_object param is
        # a synthetic_data_ingestion.sample_creator.SynthCustomers object
        if type(synth_customer_object) != SynthCustomers:

            # log a warning
            self.logger.critical(
                "RdsIngestor object NOT instanciated: synth_customer_object param is NOT a synthetic_data_ingestion.sample_creator.SynthCustomers object"
            )

            raise Exception(
                "synth_customer_object param must be a synthetic_data_ingestion.sample_creator.SynthCustomers object"
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

        # check if synth_customer_object param is has all attributes that
        # a SynthCustomers object has after generate_samples and generate_report methods
        # have been called
        if set(synth_customer_object.__dict__.keys()) != required_attributes:

            # log a warning
            self.logger.critical(
                "RdsIngestor object NOT instanciated: synth_customer_object param doesn't have all required attributes"
            )

            raise Exception(
                "RdsIngestor object NOT instanciated: synth_customer_object param doesn't have all required attributes"
            )

        # instanciate SynthCustomers object with the input params
        self.synth_customers = synth_customer_object

        # log an information
        self.logger.info(
            f"RdsIngestor object successfully instanciated: group = {synth_customer_object.group}, num_samples = {synth_customer_object.num_samples}"
        )

    def ingest_samples(self) -> str:
        """Send the generated samples to AWS RDS"""

        # create a dataframe based on
        # sampling information (on sampling_dict)
        # from SynthCustomers object
        self.df_ingestion = pd.DataFrame(data=self.synth_customers.sampling_dict)

        # create engine to connect with AWS RDS
        self._create_conn_engine()

        # define schema to input data on table
        self._create_ingestion_schema()

        # try to input data on AWS RDS
        try:
            # open connection with context manager
            with self.engine.connect() as connection:
                # insert data from df_insertion into database
                self.df_ingestion.to_sql(
                    name="SyntheticCustomers",  # Name of SQL table
                    con=connection,  # sqlalchemy.engine (Engine or Connection)
                    schema="public",  # specify the schema
                    if_exists="append",  # if the table already exists.
                    index=False,  # don't write df index as a column
                    dtype=self.dtype_schema,  # schame to input data on table
                )

        # input not valid
        except Exception as e:
            # log a warning
            self.logger.critical(
                f"ingest_samples method NOT successfully called: raised error ---> {e}"
            )

            return (
                f"ingest_samples method NOT successfully called: raised error ---> {e}"
            )

        # input validated
        else:
            # log an information
            self.logger.info(f"ingest_samples method successfully called")

            return "ingest_samples method successfully called"

    def _create_conn_engine(self) -> None:
        """Create a engine to connect with AWS RDS database"""

        # take environment variables from .env.
        load_dotenv()

        # try to load venvs and connect to AWS RDS
        try:

            # load environmental variables -> raise error if not found
            ENDPOINT = os.environ["AWS_RDB_ENDPOINT"]
            PORT = os.environ["AWS_RDB_PORT"]
            USER = os.environ["AWS_RDB_USER"]
            PASSWORD = os.environ["AWS_RDB_PASSWORD"]

            # instanciate a session with AWS credentials (in .aws/credentials)
            session = boto3.Session()
            # create a service client for AWS RDS
            client = session.client("rds")

            # create a engine for database connection
            # 'postgresql+psycopg2://user:password\@hostname:port/database_name'
            self.engine = create_engine(
                f"postgresql+psycopg2://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/postgres"
            )

        # input not valid
        except Exception as e:
            # log a warning
            self.logger.critical(
                f"_create_conn_engine method NOT successful: raised error ---> {e}"
            )

            return f"_create_conn_engine method NOT successful: raised error ---> {e}"

        # input validated
        else:

            # log an information
            self.logger.info(f"_create_conn_engine method successfully called")

    def _create_ingestion_schema(self) -> None:
        """Define the schema that data must follow in order to be input on AWS RDS"""

        # define schema for data ingestion
        self.dtype_schema = {
            "total_purchase_price": Float(precision=2),
            "num_diff_items": SmallInteger,
            "purchase_date": String(length=10),
            "region": String(length=3),
            "gender": String(length=6),
            "group": String(length=9),
            "device": String(length=8),
        }

        # log an information
        self.logger.info(f"_create_ingestion_schema method successfully called")
