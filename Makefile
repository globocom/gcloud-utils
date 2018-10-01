.PHONY: install clean test coverage lint sonar

ROOT_PATH=$(shell pwd)

install:
	@pip install -r requirements.txt --index-url=https://artifactory.globoi.com/artifactory/api/pypi/pypi-all/simple/
	@pip install -r requirements_test.txt --index-url=https://artifactory.globoi.com/artifactory/api/pypi/pypi-all/simple/

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
	@git tag $(VERSION)
	@git commit -m "Bump $(VERSION)"
	@git push --tags
	@git push origin HEAD
	@echo "Push to artifactory"
	@python setup.py sdist upload -r pypi-local 
	@python setup.py sdist upload -r ipypi-local


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
