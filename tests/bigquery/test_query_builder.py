"""Test QueryBuilder Module"""
import unittest
import os
from gcloud_utils.bigquery.query_builder import QueryBuilder

class TestQueryBuilder(unittest.TestCase):
    "Test QueryBuilder module"

    def test_make_query(self):
        query = "select * from test"
        builder = QueryBuilder(query)
        self.assertEqual(query, builder.query)

    def test_make_query_with_args(self):
        query = "select * from test where col = '${my_date}'"
        expected = "select * from test where col = '20181011'"
        builder = QueryBuilder(query)
        builder.with_vars(my_date="20181011")
        self.assertEqual(expected, builder.query)

    def test_make_query_with_wrong_args(self):
        query = "select * from test where col = '${my_date}'"
        builder = QueryBuilder(query)
        builder.with_vars(xuxu="20181011")
        self.assertEqual(query, builder.query)

    def test_make_query_with_wrong_and_right_args(self):
        query = "select * from test where col = '${my_date}'"
        expected = "select * from test where col = '20181011'"
        builder = QueryBuilder(query)
        builder.with_vars(xuxu="20181011", my_date="20181011")
        self.assertEqual(expected, builder.query)

    def test_make_query_with_file_path(self):
        builder = QueryBuilder("tests/fixtures/bigquery/query.sql")
        expected = "select * from test where col = '20181011'"
        builder.with_vars(my_date="20181011")
        self.assertEqual(expected, builder.query)
