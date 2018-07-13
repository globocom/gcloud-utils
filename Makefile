ROOT_PATH=$(shell pwd)

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
	git add version
	git tag $(VERSION)
	git commit -m "Bump $(VERSION)"
	git push --tags
	git push origin HEAD
	@echo "Push to artifactory"
	python setup.py sdist upload -r pypi-local 
	python setup.py sdist upload -r ipypi-local


test:
	python -m unittest -v tests.test_compute tests.test_storage tests.test_ml_engine tests.test_dataproc

coverage_test:
	coverage run --source=./gcloud_utils/ -m unittest -v tests.test_compute tests.test_storage tests.test_ml_engine tests.test_dataproc