from setuptools import setup, find_packages

with open("version") as version_file:
    version = version_file.read().strip()

with open("requirements.txt") as reqs:
    requirements = reqs.read().split("\n")

setup(
    name="gcloud_utils",
    version=version,
    description="handle gcloud services",
    author="Big Data",
    author_email="bigdata@corp.globo.com",
    scripts=['cli/query_to_table', 'cli/table_to_gcs', 'cli/gcs_to_table'],
    license='MIT',
    url = 'https://github.com/globocom/gcloud-utils/',
    download_url = 'https://github.com/globocom/gcloud-utils/archive/master.zip',
    install_requires=requirements,
    packages=find_packages()
)
