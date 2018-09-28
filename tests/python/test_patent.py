import os
import unittest
import zipfile

import boto3

from src.python.parse_patents import PatentParser
from src.python.patent import Patent


class PatentTests(unittest.TestCase):
    def setUp(self):
        self.patent = Patent('test')

    @staticmethod
    def decompress(file_name):
        """
        Decompresses the file.  TODO: consider extracting to memory instead of disk
        :return: Returns the name of the decompressed file
        """
        zip_ref = zipfile.ZipFile(file_name, 'r')
        out_name = zip_ref.filelist[0].filename
        zip_ref.extractall('/'.join(file_name.split('/')[:-1]))
        zip_ref.close()
        return out_name

    def test_dates(self):
        """
        Tests version 1 patent format and parsing
        :return:
        """
        fields = ['file_date']
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

    def parsing(self, fl):
        if not os.path.exists(fl.replace('/', '_')):
            s3 = boto3.resource('s3')
            my_bucket = s3.Bucket('patent-xml-zipped')
            my_bucket.download_file(fl, fl.replace('/', '_'))

        out_file = self.decompress(fl.replace('/', '_'))
        parsed = PatentParser(out_file)
        assert len(parsed.patents) > 0
        os.remove(fl.replace('/', '_'))
        os.remove(out_file)

    def test_version1(self):
        """
        Tests version 1 patent format and parsing
        :return:
        """
        self.parsing('1976/19760601')

    def test_version2(self):
        """
        Tests version 2 patent format and parsing
        :return:
        """
        self.parsing('2004/040127')

    def test_version3(self):
        """
        Tests version 3 patent format and parsing
        :return:
        """
        self.parsing('2015/150331')


if __name__ == '__main__':
    unittest.main()
