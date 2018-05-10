upload:
	python ./setup.py sdist upload -r ipypi-local

test:
	python -m unittest -v tests.test_compute tests.test_storage