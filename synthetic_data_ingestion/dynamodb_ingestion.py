# import required libraries
import os
import re
import boto3
import logging
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv


class DynamodbIngestor:
    def __init__(self, logs_folder: str) -> None:
        """Instanciate class with folder whose
        logs need to be sent to Dynamo DB

        Args
            logs_folder: a string with the path to logs folder"""

        # instanciate logger
        self.logger = logging.getLogger("dynamodb_ingestion.py")

        # define log date in utc
        logging.Formatter.converter = time.gmtime

        # define logging configuration
        logging.basicConfig(
            filename=f"../logs/data_ingestion-{datetime.utcnow().date()}.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y:%m:%d %H:%M:%S",
        )

        # define a attribute with logs_folder input
        self.logs_folder = logs_folder

        # parse logs folder
        self._parse_logs()

        # set a flag to indicate that table was already created
        self._table_created_flag = True

        # set a flag to indicate that if logs were successfully sent to DynamoDB
        self._logs_sent = False

        # log an information
        self.logger.info(
            f"DynamodbIngestor object successfully instanciated: logs_folder = {logs_folder}"
        )


    def send_logs(self) -> str:
        """Send logs (in batch) to AWS DynamoDB"""

        # check if logs were not sent yet
        if not self._logs_sent:

            # try to send logs in batch
            try:

                # create a DynamoDB client
                self._create_client()

                # open dynamodb client with context manaer
                with self.table.batch_writer() as writer:
                    # iterate over all logs
                    for log in self.all_logs:
                        # send log
                        writer.put_item(
                            Item={
                                "timestamp": log[0],  # log level
                                "log_file_sequence": log[1],  # timestamp (in UTC)
                                "level": log[2],  # log level
                                "name": log[3],  # log name
                                "msg": log[4],  # log message
                            }
                        )

            # exception on batch sending
            except Exception as e:
                # log a warning
                self.logger.critical(
                    f"send_logs method NOT successful: raised error ---> {e}"
                )

                return f"send_logs method NOT successful: raised error ---> {e}"

            # log sent
            else:

                # log an information
                self.logger.info(f"send_logs method successfully called")

                # set a flag to indicate that if logs were successfully sent to DynamoDB
                self._logs_sent = True

                # delete logs to save space
                # self._delete_logs()

                return "send_logs method successfully called"

        # logs were already sent
        else:
            # log an information
            self.logger.info(
                f"send_logs method successfully called: nothing was done once log were already sent"
            )

            # message to user
            return "Nothing was done once log were already sent"


    def _create_client(self) -> None:
        """Create a client to connect with AWS DynamoDB"""

        # take environment variables from .env.
        load_dotenv()

        # try to load venvs and connect to AWS DynamoDB
        try:

            # load environmental variables -> raise error if not found
            DYNAMO_TABLE = os.environ["AWS_DYNAMODB_TABLE"]

            # create a resource service client
            dynamodb = boto3.resource("dynamodb")

            # creates a Table resource
            self.table = dynamodb.Table(DYNAMO_TABLE)

        # input not valid
        except Exception as e:
            # log a warning
            self.logger.critical(
                f"_create_client method NOT successful: raised error ---> {e}"
            )

            return f"_create_client method NOT successful: raised error ---> {e}"

        # input validated
        else:

            # log an information
            self.logger.info(f"_create_client method successfully called")

    def _create_table(self) -> str:
        """Create a table on AWS DynamoDB"""

        # if table was not created yet
        if not self._table_created_flag:

            # try to create a table on AWS DynamoDB
            try:
                # create a resource service client
                dynamodb = boto3.resource("dynamodb")

                # take environment variables from .env.
                load_dotenv()

                # load environmental variables -> raise error if not found
                DYNAMO_TABLE = os.environ["AWS_DYNAMODB_TABLE"]

                # create dynamodb table
                table = dynamodb.create_table(  # define table structure
                    TableName=DYNAMO_TABLE,  # table name in DynamoDB
                    KeySchema=[  # primary key structure for the table
                        {
                            "AttributeName": "timestamp",
                            "KeyType": "HASH",
                        },  # Partition key
                        {
                            "AttributeName": "log_file_sequence",
                            "KeyType": "RANGE",
                        },  # Sort key
                    ],
                    AttributeDefinitions=[  # describing the key schema for the table and indexes
                        {"AttributeName": "timestamp", "AttributeType": "S"},  # String
                        {
                            "AttributeName": "log_file_sequence",
                            "AttributeType": "N",
                        },  # Number
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )

            # exception on table creation
            except Exception as e:
                # log a warning
                self.logger.critical(
                    f"_create_table method NOT successful: raised error ---> {e}"
                )

                return f"_create_table method NOT successful: raised error ---> {e}"

            # table created
            else:

                # log an information
                self.logger.info(f"_create_table method successfully called")

                # set a flag to indicate that table was successfully created
                self._table_created_flag = True

                return "_create_table method successfully called"

        # if table was already created yet
        else:

            # log an information
            self.logger.info(
                f"_create_table method successfully called: nothing was done once table is already created"
            )

            # message to user
            return "Nothing was done once table is already created"

    def _parse_logs(self) -> str:
        """Given the logs_folder param, parse all files inside this folder
        and create a list with prepared log to DynamoDB Input"""

        # try to find the given folder
        try:

            # get name of files inside logs folder
            log_files = os.listdir(self.logs_folder)

        # input not valid
        except Exception as e:
            # log a warning
            self.logger.critical(
                f"parse_logs method NOT successful: raised error ---> {e}"
            )

            return f"parse_logs method NOT successful: raised error ---> {e}"

        # folder was found
        else:

            # create a regex mask to parse log messages
            timestamp_maks = "^(\d{4}:\d{2}:\d{2} \d{2}:\d{2}:\d{2})"

            # instanciate final logs list
            all_logs = []

            # iterate of log files
            for log in log_files:
                # create path to the given log
                log_path = os.path.join(self.logs_folder, log)

                # open log file with context manager
                with open(log_path, "r") as log_file:
                    # read log file content as a list (one item per line)
                    list_logs = log_file.readlines()

                    # iterate over log lines
                    for idx, log in enumerate(list_logs):
                        # check if log message starts with a timestamp
                        # (avoid confusing logs)
                        if re.search(timestamp_maks, log) is not None:
                            # split log message on " - "
                            log_split = log.split(" - ")

                            # append the parsed log information to all_logs list
                            all_logs.append(
                                (
                                    log_split[0].replace(
                                        " ", "Z"
                                    ),  # timestamp (in UTC)
                                    idx + 1,  # log sequence for the given file
                                    log_split[1],  # log level
                                    log_split[2],  # log name
                                    log_split[3][:-1],
                                )  # log message
                            )

            # save all_logs list as an attribute of the instance
            self.all_logs = all_logs

            # log an info
            self.logger.info(f"parse_logs method successfully called")

            return "parse_logs method successfully called"


    def _delete_logs(self) -> str:
        """Given the logs_folder param, parse all files inside this folder
        and delete them"""

        # check if logs were succesfully sent
        if self._logs_sent:

            # try to find the given folder
            try:

                # get name of files inside logs folder
                log_files = os.listdir(self.logs_folder)

            # input not valid
            except Exception as e:
                # log a warning
                self.logger.warning(
                    f"_delete_logs method NOT successful: raised error ---> {e}"
                )

                return f"_delete_logs method NOT successful: raised error ---> {e}"

            # folder was found
            else:

                # iterate of log files
                for log in log_files:
                    # create path to the given log
                    log_path = os.path.join(self.logs_folder, log)

                    # delete log file
                    os.remove(log_path)

                # log an info
                self.logger.info("_delete_logs method successfully called")

                return "_delete_logs method successfully called"

        # logs were not successfully sent
        else:
            # log an info
            self.logger.warning(
                "_delete_logs method successfully called: not log was deleted once none was already sent to DynamoDB"
            )

            return "_delete_logs method successfully called: not log was deleted once none was already sent to DynamoDB"
