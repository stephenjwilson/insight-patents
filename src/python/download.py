import os
import re
import sys
import urllib.request

import boto3

# TODO: Add Logging
BASE_URL = "https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/{}/"


def get_urls(year):
    """
    Formats the appropriate url to retrieve data from USTPO.
    :param year: the year to obtain all zips for
    :return: returns a list of urls
    """

    zip_regex = re.compile("href=\"([a-z0-9]*?\.zip)\"")

    with urllib.request.urlopen(BASE_URL.format(year)) as response:
        html = response.read().decode('utf-8')
        urls = zip_regex.findall(html)

    return urls


def download(start_year, end_year, storage_location='patent_xml_zipped'):
    """
    Locally downloads the patent data
    :param start_year: the first year that will be retrieved
    :param end_year: the last year that will be retrieved
    :param storage_location: the folder where everything should be stored
    :return:
    """
    # Make the storage folder if needed
    try:
        os.mkdir(storage_location)
    except FileExistsError:
        pass

    # retrieve each week for a particular year
    for year in range(start_year, end_year + 1):
        folder_path = os.path.join(storage_location, str(year))
        # Make folder for year
        try:
            os.mkdir(folder_path)
        except FileExistsError:
            pass
        # Get appropriate urls
        urls = get_urls(year)
        # Download each zip
        for url in urls:
            local_path = os.path.join(folder_path, url)
            # Get and download zip
            resp = requests.get(BASE_URL.format(year) + url)
            f = open(local_path, 'wb')
            f.write(resp.content)
            f.close()
            # Upload to S3
            push_to_s3(local_path, year, url)
            # break  # tmp test TODO: remove break, test full download.
    return


def push_to_s3(file, year, name, bucket='patent-xml-zipped'):
    """
    Uploads a patent to the S3 bucket
    :param file: the local file to upload
    :param year: the year of the patent
    :param name: a standard name for the patent file TODO: not standard yet
    :param bucket: the name fo the bucket. Currently defaults to patent-xml-zipped
    :return: None
    """
    # Use boto to put s3 files
    s3 = boto3.resource('s3')  # TODO: ensure cred in ~/.aws/credentials via aws configure for ec2s
    s3.create_bucket(Bucket=bucket)

    # List files downloaded
    s3.meta.client.upload_file(file, bucket, os.path.join(str(year), name))
    return


if __name__ == '__main__':
    download(int(sys.argv[1]), int(sys.argv[2]))
