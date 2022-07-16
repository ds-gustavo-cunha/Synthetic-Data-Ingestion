######################################
##############   VENV   ##############

# import required libraries
import os
from dotenv import load_dotenv


###################
### Project library

# define path to project package
root_path = os.environ["LOGS_FOLDER_PATH"]

# import required libraries
import sys

# append root_path to the list of directories
# where the Python interpreter searches for modules
sys.path.append(root_path)


#################################
### Virtual environment variables

# take environment variables from .env.
load_dotenv(f"{root_path}/.env")


############
### Constant

# import required libraries
import os

# define path to logs folder
log_folder = os.path.join(root_path, "logs")


#######################################
############## LIBRARIES ##############

from datetime import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator


# import required libraries -> project library
from synthetic_data_ingestion.rds_ingestion import RdsIngestor
from synthetic_data_ingestion.sample_creator import SynthCustomers
from synthetic_data_ingestion.dynamodb_ingestion import DynamodbIngestor
from synthetic_data_ingestion.lambda_ingestion import LambdaIngestor


#########################################
############## FUNCTIONS ################


def rds_ingestion(synth_customer_object: SynthCustomers, log_folder: str) -> None:
    """Send synthetic data to AWS RDS.

    Args
        synth_customer_object: a synthetic_data_ingestion.sample_creator.SynthCustomers object.
        log_folder: a string with the path to store logs."""

    # instantiate a RdsIngestor object
    rds_ingestor = RdsIngestor(synth_customers, log_folder)
    # send synthetic samples to AWS RDS
    rds_ingestor.ingest_samples()


def lambda_ingestion(synth_customer_object: SynthCustomers, log_folder: str) -> None:
    """Send synthetic data to AWS Lambda (FastAPI).

    Args
        synth_customer_object: a synthetic_data_ingestion.sample_creator.SynthCustomers object.
        log_folder: a string with the path to store logs."""

    # instanciate LambdaIngestor object
    lam_ingestion = LambdaIngestor(synth_customers)
    # send report to FastAPI on AWS Lambda
    lam_ingestion.send_report_to_lambda()


def dynamo_ingestion(logs_folder: str) -> None:
    """send log to AWS DynamoDB

    Args
        log_folder: a string with the path to store logs"""

    # instanciate a DynamodbIngestor object
    dynamo_ingestor = DynamodbIngestor(logs_folder)
    # send log to AWS DynamoDB
    dynamo_ingestor.send_logs()


#########################################
############## AIRFLOW DAG ##############

# create a dag with context manager
with DAG(
    dag_id="synthetic_customers_workflow",
    description="Ingest synthetic data on AWS architecture",
    start_date=datetime(2022, 6, 30),
    end_date=None,
    schedule_interval=timedelta(minutes=5),
    catchup=False,  # don't wait schedule_interval after start date
) as dag:

    # iterate over groups to be created
    for group in ["CONTROL", "TREATMENT"]:
        # instanciate SynthCustomers object
        synth_customers = SynthCustomers(
            num_samples=5, group=group, log_folder=log_folder
        )
        # generate synthetic samples
        synth_customers.generate_samples()
        # generate report
        synth_customers.generate_report()

        # task to ingest data on AWS RDS Database
        rds_ingestion_task = PythonOperator(
            task_id=f"rds_ingestion_{group.lower()}",
            python_callable=rds_ingestion,  # call python function
            op_kwargs={
                "synth_customer_object": synth_customers,
                "log_folder": log_folder,
            },  # callable args
            show_return_value_in_logs=True,
        )

        # task to ingest data on AWS Lambda (FastAPI)
        lambda_ingestion_task = PythonOperator(
            task_id=f"lambda_ingestion_{group.lower()}",
            python_callable=lambda_ingestion,  # call python function
            op_kwargs={
                "synth_customer_object": synth_customers,
                "log_folder": log_folder,
            },  # callable args
            show_return_value_in_logs=True,
        )

        # task to send data to AWS DynamoDB
        dynamo_ingestion_task = PythonOperator(
            task_id=f"dynamo_ingestion_{group.lower()}",
            python_callable=dynamo_ingestion,  # call python function
            op_kwargs={"logs_folder": log_folder},  # callable args
            trigger_rule="all_done",  # regardless of upstream success/fail
            show_return_value_in_logs=True,
        )

        # define workflow
        rds_ingestion_task >> lambda_ingestion_task >> dynamo_ingestion_task
