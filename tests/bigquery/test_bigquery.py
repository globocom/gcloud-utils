"""Test Bigquery Module"""
import unittest
import os
from googleapiclient.http import HttpMockSequence
from gcloud_utils.bigquery.bigquery import Bigquery
from gcloud_utils.bigquery.query_builder import QueryBuilder
try:
    import mock
except ImportError:
    import unittest.mock as mock

class TestBigquery(unittest.TestCase):
    "Test Bigquery module"

    def test_make_query(self):
        query = "select * from test"
        client = mock.Mock()
        bigquery = Bigquery(client)
        bigquery.query(query)
        client.query.assert_called_once_with(query=query)
   
    def test_make_query_with_object(self):
        query = QueryBuilder("select * from test")
        job_mock = mock.Mock()
        client_mock = mock.Mock(**{'query.return_value': job_mock})

        bigquery = Bigquery(client_mock)
        bigquery.query(query)
        client_mock.query.assert_called_once_with(query=query.query)
        job_mock.result.assert_called_once()
    
    def test_make_query_to_table(self):
        pass