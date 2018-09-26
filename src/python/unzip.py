import logging
import os
import zipfile

import boto3
import botocore

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("unzip-log")
BUCKET_NAME = 'patent-xml-zipped'


def download_from_s3(key, output_name=None, bucket=None):
    """
    Downloads a particular file from an S3 bucket
    :param key: The file key in the S3 bucket
    :param output_name: What to save the file as locally
    :param bucket: The name of the S3 bucket. Can be retrieved from the variable BUCKET_NAME
    :return:
    """
    if bucket is None:
        bucket = BUCKET_NAME
    if output_name is None:
        output_name = key.replace('/', '_')

    # Only download if needed
    if os.path.exists(output_name):
        return output_name

    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    if os.path.exists(output_name):
        return output_name
    try:
        my_bucket.download_file(key, output_name)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logging.log("{} does not exist.".format(key))
        else:
            raise
    return output_name


def push_to_s3(file, name, bucket='patent-xml-zipped'):
    """
    Uploads a patent to the S3 bucket
    :param file: the local file to upload
    :param name: a standard name for the patent file
    :param bucket: the name fo the bucket. Currently defaults to patent-xml-zipped
    :return: None
    """
    # Use boto to put s3 files
    s3 = boto3.resource('s3')  # TODO: ensure cred in ~/.aws/credentials via aws configure for ec2s
    s3.create_bucket(Bucket=bucket)

    # List files downloaded
    s3.meta.client.upload_file(file, bucket, name)
    return


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


def main():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('patent-xml-zipped')
    s3.create_bucket(Bucket='patent-xml')
    keys_to_process = []
    for my_object in my_bucket.objects.all():
        if ".json" in my_object.key:
            continue
        if my_object.key[:4] != '2018':
            continue
        fl_name = download_from_s3(my_object.key)
        fl_name = decompress(fl_name)
        push_to_s3(fl_name, fl_name, 'patent-xml')


if __name__ == '__main__':
    main()
