build:
	poetry build

publish:
	poetry publish --dry-run

install:
	poetry install

lint:
	poetry run flake8 cre_xml_parser
