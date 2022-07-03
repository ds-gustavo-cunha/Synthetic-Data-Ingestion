#######################################
############## LIBRARIES ##############

from   airflow.models             import DAG
from   airflow.operators.python   import PythonOperator
from   datetime                   import datetime

# import required libraries -> project library
from synthetic_data_ingestion.rds_ingestion import RdsIngestor
from synthetic_data_ingestion.sample_creator import SynthCustomers
from synthetic_data_ingestion.dynamodb_ingestion import DynamodbIngestor
from synthetic_data_ingestion.lambda_ingestion import LambdaIngestor


#######################################
############## FUNCTIONS ##############


def data_ingestion(num_samples, group):
    """Function that following the ingestion pipeline 
    according to data_architecture.png
    
    Args
        num_samples: an integer with the number of synthetic users to create
        group: a string with CONTROL or TREATMENT customers
    """

    # instanciate SynthCustomers object
    synth_customers = SynthCustomers(num_samples, group)
    # generate synthetic samples
    synth_customers.generate_samples()
    # generate report
    synth_customers.generate_report()

    # instantiate a RdsIngestor object
    rds_ingestor = RdsIngestor(synth_customers)
    # send synthetic samples to AWS RDS
    rds_ingestor.ingest_samples()

    # instanciate LambdaIngestor object
    lam_ingestion = LambdaIngestor(synth_customers)
    # send report to FastAPI on AWS Lambda
    lam_ingestion.send_report_to_lambda()

    # instanciate a DynamodbIngestor object
    dynamo_ingestor = DynamodbIngestor("../logs")
    # send log to AWS DynamoDB
    dynamo_ingestor.send_logs()


#########################################
############## AIRFLOW DAG ##############

# create a dag with context manager
with DAG(dag_id = "synthetic_data_ingestion", 
        start_date = datetime(2022,7, 2),
        schedule_interval = "* * * * *", # At 09:00 on Monday.
        catchup = False # don't wait schedule_interval after start date
        ) as dag:
        

    # create CONTROL customers and follow ingestion workflow
    control_ingestion =  PythonOperator(task_id = "control_ingestion",
        python_callable = data_ingestion, # call python function
        op_kwargs = {"num_samples": 50, "group": "CONTROL"}, # callable args
        trigger_rule = "all_done" # regardless of upstream success/fail
        )

    # create TREATMENT customers and follow ingestion workflow
    treatment_ingestion =  PythonOperator(task_id = "treatment_ingestion",
        python_callable = data_ingestion, # call python function
        op_kwargs = {"num_samples": 50, "group": "TREATMENT"}, # callable args
        trigger_rule = "all_done" # regardless of upstream success/fail
        )


    # define workflow
    control_ingestion >> treatment_ingestion