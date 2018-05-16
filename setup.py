from setuptools import setup, find_packages

with open("version") as version_file:
    version = version_file.read().strip()

setup(
    name="gcloud_utils",
    version=version,
    description="handle gcloud services",
    author="Big Data",
    author_email="bigdata@corp.globo.com",
    license='MIT',
    url = 'https://gitlab.globoi.com/bigdata/gcloud-utils',
    download_url = 'https://gitlab.globoi.com/bigdata/gcloud-utils/repository/master/archive.tar.gz',
    install_requires=["google-api-python-client", "gcloud"],
    packages=find_packages()
)
