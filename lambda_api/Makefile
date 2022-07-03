black:
	@black *.py

run_api:
	uvicorn lambda_function:app --reload

lambda_venv:
	@bash lambda_venv_builder.sh

remove_lambda_venv:
	@rm -i lambda_venv.zip