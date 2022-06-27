#!/bin/bash

# print user info
echo "---> Checking pipenv! <---"
# check if pipenv is running
pipenv install

# define path to current folder
DIR=$(pwd)

# folder that stores venv
VENV_FOLDER=$(pipenv --venv)
# get python version inside lib folder
PYTHON_VERSION=`ls $VENV_FOLDER/lib/`

echo "---> $PYTHON_VERSION <---"

# define folder that holds all venv library code
SITE_PACKAGES=$VENV_FOLDER/lib/$PYTHON_VERSION/site-packages

# print step report
echo "---> Venv found! <---"

# print report
echo "---> Zipping files... <---"

# change to venv directory
cd $SITE_PACKAGES
# zip files
# -r = recurse into directories
# -9 = compress better
# zip every code of venv library
# into package.zip of project folder
# ignoring boto3 and botocore (Lambda has them)
# -q = quiet
zip -r9 -q --exclude=*boto3* --exclude=*botocore* $DIR/lambda_venv.zip *

# change back to project directory
cd $DIR

# add the lambda_function to the zip
zip -g -q lambda_venv.zip .env lambda_function.py

echo "---> Adding lambda_function.py and .env files to zip <---"

# print report
echo "---> Venv successfully zipped! <---"

# get statistics for the given .zip
STATS=`du -sh lambda_venv.zip`

# print final report
echo "---> Zipped file:"
echo "--->     $STATS"