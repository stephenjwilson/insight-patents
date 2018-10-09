import datetime
import logging
import os
import re
import sys
import urllib.request

import boto3
import requests
from IPython import embed

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("download-log")
BASE_URL = "https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/{}/"


def get_urls(year):
    """
    Formats the appropriate url to retrieve data from USTPO.
    :param year: the year to obtain all zips for
    :return: returns a list of urls
    """

    zip_regex = re.compile("href=\"([a-z0-9_]*?\.zip)\"")

    with urllib.request.urlopen(BASE_URL.format(year)) as response:
        html = response.read().decode('utf-8')
        urls = zip_regex.findall(html)
    log.info("Retrieved %s URLS", '{} {}'.format(year, len(urls)))
    return urls


def download(start_year, end_year, current_date=None, storage_location='patent_xml_zipped', bucket='patent-xml-zipped'):
    """
    Locally downloads the patent data
    :param start_year: the first year that will be retrieved
    :param end_year: the last year that will be retrieved
    :param current_date: Specifies if it should only look for a single file at the current date, must be a Tuesday
    :param storage_location: the folder where everything should be stored
    :param bucket: the S3 Bucket
    :return:
    """
    # Make the storage folder if needed
    try:
        os.mkdir(storage_location)
    except FileExistsError:
        pass

    # Get already downloaded files
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    current_keys = []
    for my_object in my_bucket.objects.all():
        if ".json" in my_object.key:
            continue
        current_keys.append(my_object.key)

    # Special mode for only downloading a single week
    if current_date is not None:
        date = datetime.datetime.strptime(current_date, "%Y%m%d")
        year = str(date.year)
        folder_path = os.path.join(storage_location, str(year))
        try:
            os.mkdir(folder_path)
        except FileExistsError:
            pass

        # Get appropriate urls
        urls = get_urls(year)
        # Download each zip
        for url in urls:
            name = re.sub("\D", "", url.split('_')[0])
            if '{}/{}'.format(year, name) in current_keys:
                continue
            if str(date.month).zfill(2) == name[-4:-2]:
                # if '02' == name[-2:]:
                if str(date.day).zfill(2) == name[-2:]:
                    log.info('Working on {}'.format(name))
                    local_path = os.path.join(folder_path, url)
                    # Get and download zip
                    resp = requests.get(BASE_URL.format(year) + url)
                    
                    f = open(local_path, 'wb')
                    f.write(resp.content)
                    f.close()
                    # Upload to S3
                    push_to_s3(local_path, year, name)
                    log.info("Pushed %s", "{}_{}".format(year, name))

                    # Remove file
                    os.remove(local_path)
                    return
        log.info('No Date matched')
        return

    # retrieve each week for a particular year that hasn't been downloaded
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
            name = re.sub("\D", "", url.split('_')[0])
            if '{}/{}'.format(year, name) in current_keys:
                continue
            log.info('Working on {}'.format(name))

            local_path = os.path.join(folder_path, url)
            # Get and download zip
            resp = requests.get(BASE_URL.format(year) + url)
            f = open(local_path, 'wb')
            f.write(resp.content)
            f.close()
            # Upload to S3
            push_to_s3(local_path, year, name)
            log.info("Pushed %s", "{}_{}".format(year, name))

            # Remove file
            os.remove(local_path)

    return


def push_to_s3(file, year, name, bucket='patent-xml-zipped'):
    """
    Uploads a patent to the S3 bucket
    :param file: the local file to upload
    :param year: the year of the patent
    :param name: a standard name for the patent file
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
    if len(sys.argv) == 4:
        download(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3])
    else:
        download(int(sys.argv[1]), int(sys.argv[2]))
