# # import required libraries
# from synthetic_data_ingestion.rds_ingestion import RdsIngestor
# from synthetic_data_ingestion.sample_creator import SynthCustomers
# from synthetic_data_ingestion.dynamodb_ingestion import DynamodbIngestor
# synth_customers = SynthCustomers(50, "TREATMENT")
# synth_customers.generate_samples()
# synth_customers.generate_report()

# # rds_ingestor = RdsIngestor(7, "CONTROL")

# # d = DynamodbIngestor("../logs/")

# import json
# import numpy as np

# class NpEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, np.integer):
#             return int(obj)
#         if isinstance(obj, np.floating):
#             return float(obj)
#         if isinstance(obj, np.ndarray):
#             return obj.tolist()
#         return super(NpEncoder, self).default(obj)

# report_dict = synth_customers.creation_report


# # Your codes ....
# data = json.dumps(report_dict, cls=NpEncoder)
# data


# #====================
# # make request to api
# #====================

# # required libraries
# import requests

# # API url
# # local api test
# # url = 'http://0.0.0.0:8000' # FastAPI
# # test containerized api
# #URL = 'http://127.0.0.1:8000'
# URL = "https://nvlzovmsyjyq5jqa6gnlif3fra0xpyaw.lambda-url.us-east-1.on.aws/"

# # request header
# header = {'Content-type': 'application/json' }

# # make request
# r = requests.post( url = f"{URL}/sampling_report", data = data, headers = header )

# # print response status code and response in json format
# print( f'Status Code {r.status_code}' )
# r.json()
