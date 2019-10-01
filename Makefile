.PHONY: install clean test coverage lint ci

ROOT_PATH=$(shell pwd)
PYTHON_VERSION := $(word 2,$(subst ., ,$(shell python --version 2>&1)))

install:
	@pip install -r requirements.txt
	@pip install -r requirements_dev.txt
ifeq ($(PYTHON_VERSION),3)
	@pip install -r requirements_dev_python3.txt
else
	@pip install -r requirements_dev_python2.txt
endif

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
ifeq ($(PYTHON_VERSION),3)
	@pylint ./gcloud_utils/ --rcfile=python3.pylintrc
else
	@pylint ./gcloud_utils/
endif

ci: coverage lint