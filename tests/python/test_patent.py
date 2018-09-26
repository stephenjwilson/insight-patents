import os
import unittest

import boto3

from src.python.parse_patents import PatentParser
from src.python.patent import Patent


class PatentTests(unittest.TestCase):
    def setUp(self):
        self.patent = Patent('test')

    def test_something(self):
        self.assertEqual(True, True)

    def test_dates(self):
        """
        Tests version 1 patent format and parsing
        :return:
        """
        fields = ['file_date', 'grant_date']
        # Test years
        for year in [1900, 3000, 1800, 2017]:
            self.patent.file_date = '{}0101'.format(year)
            self.patent.to_csv(fields)

        # Test months
        for month in ['00', '01', '13', '12']:
            self.patent.file_date = '2000{}01'.format(month)
            self.patent.to_csv(fields)
        # Test days
        for day in ['00', '01', '20', '31']:
            self.patent.file_date = '200001{}'.format(day)
            self.patent.to_csv(fields)

    def test_1(self):
        """
        Tests version 1 patent format and parsing
        :return:
        """
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('patent-xml')
        my_bucket.download_file('pftaps19760106_wk01.txt', 'testfl')
        PatentParser('testfl')
        os.remove('testfl')

    def test_2(self):
        """
        Tests version 2 patent format and parsing
        :return:
        """
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('patent-xml')
        my_bucket.download_file('ipg050104.xml', 'testfl')
        PatentParser('testfl')
        os.remove('testfl')

    def test_3(self):
        """
        Tests version 3 patent format and parsing
        :return:
        """
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('patent-xml')
        my_bucket.download_file('ipg180227.xml', 'testfl')
        PatentParser('testfl')
        os.remove('testfl')


if __name__ == '__main__':
    unittest.main()
