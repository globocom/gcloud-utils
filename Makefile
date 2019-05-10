.PHONY: install clean test coverage lint sonar

ROOT_PATH=$(shell pwd)

install:
	@pip install -r requirements.txt
	@pip install -r requirements_dev.txt

clean:
	-@rm -rf $(ROOT_PATH)/*.egg-info
	-@rm -rf $(ROOT_PATH)/dist
	-@rm -rf $(ROOT_PATH)/build

release: clean
ifndef VERSION
	@echo "USAGE:\n make VERSION=XXX release"
	$(error you must pass a VERSION)
endif
	@echo $(VERSION) > version
	@git add version
	@git commit -m "Bump $(VERSION)"
	@git tag $(VERSION)
	@git push --tags
	@git push origin HEAD
	@echo "Pushing lib"
	@python setup.py sdist
	@twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
	

test:
	@pytest

coverage:
	@coverage run --source=./gcloud_utils/ -m pytest
	@coverage report -m
	@coverage xml

lint:
	@pylint ./gcloud_utils/

sonar: coverage
	sonar-scanner
