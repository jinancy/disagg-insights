import unittest
from pyspark.sql import SparkSession


class TransformerTests(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()


    def test_something(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
