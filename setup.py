from distutils.core import setup

with open("version") as version_file:
    version = version_file.read().strip()

setup(
  name = 'gcloud_utils',
  version=version,
  packages = ['gcloud_utils'], # this must be the same as the name above
  description = 'handle gcloud services',
  author = 'Susana Bouchardet',
  author_email = 'susana.bouchardet@corp.globo.com',
  url = 'https://gitlab.globoi.com/bigdata/gcloud-utils', # use the URL to the github repo
  download_url = 'https://gitlab.globoi.com/bigdata/gcloud-utils/repository/master/archive.tar.gz', # I'll explain this in a second
  keywords = ['gcloud'], # arbitrary keywords
  classifiers = [],
)