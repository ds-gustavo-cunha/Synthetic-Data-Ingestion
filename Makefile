install_requirements:
	@pip install -r requirements.txt

black:
	black api/*.py tests/*.py synthetic_data_ingestion/*.py scripts/

run_api:
	uvicorn api.api:app --reload

test:
	@coverage run -m pytest tests/*.py
	@coverage report -m --omit="tests/*.py"

clean:
	@rm -f */version.txt
	@rm -f .coverage
	@rm -fr */__pycache__ */*.pyc __pycache__
	@rm -fr build dist
	@rm -fr synthetic_data_ingestion-*.dist-info
	@rm -fr synthetic_data_ingestion.egg-info

install_project_lib:
	pip install -e .