# import required libraries
from setuptools import find_packages
from setuptools import setup

# open requirements.txt with context manager
with open('requirements.txt') as f:
    # load requirements.txt context as a list
    content = f.readlines()

# set requirements as rows that don't have git+
requirements = [x.strip() for x in content if 'git+' not in x]

# difine project setup
setup(name='synthetic_data_ingestion', # project name
      version="1.0", # project version
      description="Synthetic data injection project", # project description
      packages=find_packages(), # find package from source folder
      install_requires=requirements, # install packages in requirements list
      test_suite='tests', # folder with tests
      # include_package_data: to install data from MANIFEST.in
      include_package_data=True, # include data package inside package
      scripts=['scripts/synthetic_data_ingestion-run'], # available scripts
      zip_safe=False) # project canNOT be installed and run from a zip file
