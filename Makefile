release:
ifndef VERSION
	@echo "USAGE:\n make VERSION=XXX release"
	$(error you must pass a VERSION)
endif
	@echo $(VERSION) > version
	git tag $(VERSION)
	git commit -m "Bump $(VERSION)"
	git push --tags
	git push origin HEAD
	@echo "Push to artifactory"
	python setup.py sdist upload -r pypi-local
	python setup.py sdist upload -r ipypi-local
	rm -rf *.egg-info dist

test:
	python -m unittest -v tests.test_compute tests.test_storage tests.test_ml_engine