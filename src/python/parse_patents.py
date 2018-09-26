"""
The main run script of the pipeline. It includes a main function that is used in spark submit.
"""

import glob
import io
import logging
import os
import shutil
import zipfile

import boto3
import botocore
import psycopg2
from dotenv import load_dotenv, find_dotenv
from pyspark import SparkContext

from src.python.parsers import PatentParser

#from parsers import PatentParser

BUCKET_NAME = 'patent-xml-zipped'


def to_csv(list_of_patents, sep=','):
    """
    Takes a list_of_patent_dictionaries and outputs a csv that can be uploaded into postgres
    :param list_of_patents: A list of Patent objects
    :param output: The file name used to output the files
    :param sep: The separator for the csv file
    :return: nodes string, edges string
    """
    fields = ['patent_number', 'file_date', 'grant_date', 'title', 'abstract', 'owner', 'country', 'ipcs']
    nodes = sep.join(fields) + '\n'
    edges = "patent_number{}citation\n".format(sep)

    for patent in list_of_patents:
        s = patent.to_csv(fields, sep=sep)
        nodes += s

        # Deal with relationships
        edges += patent.to_neo4j_relationships()

    return nodes, edges


def ensure_postgres():
    """
    Ensures the appropriate postgres table exists
    :return:
    """
    # Load environment variables
    load_dotenv(find_dotenv())
    # Connect to postgres
    HOST = os.getenv("POSTGRES_HOST")
    USER = os.getenv("POSTGRES_USER")
    PASS = os.getenv("POSTGRES_PASSWORD")

    conn = psycopg2.connect(host=HOST, dbname="patent_data",
                            user=USER, password=PASS)

    # Create table
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS patents(
        patent_number text PRIMARY KEY,
        file_date date,
        grant_date date,
        title text,
        abstract text,
        owner text,
        country text,
        ipc text
    )
    """)
    conn.commit()
    conn.close()


def to_postgres(data):
    """
    Do a bulk upload of a csv file to a PostgreSQL database.
    :param csv_file: The name of the csv file to upload
    """

    # Connect to postgres
    HOST = os.getenv("POSTGRES_HOST")
    USER = os.getenv("POSTGRES_USER")
    PASS = os.getenv("POSTGRES_PASSWORD")

    conn = psycopg2.connect(host=HOST, dbname="patent_data",
                            user=USER, password=PASS)

    # Upload data
    cur = conn.cursor()
    copy_sql = """
               COPY patents FROM stdin WITH CSV HEADER
               DELIMITER as ','
               """

    cur.copy_expert(sql=copy_sql, file=io.StringIO(data))

    # cur.copy_from(io.StringIO(data), 'patents', columns=('patent_number', 'file_date', 'grant_date', 'title', 'abstract',
    #                                                      'owner', 'country', 'ipc'), sep=',')
    conn.commit()
    conn.close()


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


def process(key):
    """
    Applys an ETL pipeline for patents on a particular key from a S3 bucket
    :param key: This is the key for the S3 Bucket
    """
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    logging.info("Starting {}".format(key))
    # Load environment variables
    load_dotenv(find_dotenv())
    # Download, decompress, and determine format of data TODO: do in memory
    file_name = download_from_s3(key)
    decompress_name = decompress(file_name)

    # Parse Data and output to csv
    patent_parser = PatentParser(decompress_name)

    logging.info("Parsed {}".format(key))
    nodes, edges = to_csv(patent_parser.patents)

    # send nodes to postgres
    try:
        to_postgres(nodes)
    except psycopg2.IntegrityError:
        pass

    # Remove files
    os.remove(file_name)
    os.remove(decompress_name)

    return edges


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def main():
    """
    Main Function that downloads, decompresses, and parses the USTPO XML data before dumping it into a
    PostgreSQL and a Neo4j database.
    :return:
    """

    # Ensure that the postgres table is there
    ensure_postgres()

    # Get list of keys to process
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(BUCKET_NAME)
    keys_to_process = []
    for my_object in my_bucket.objects.all():
        if ".json" in my_object.key:
            continue
        keys_to_process.append(my_object.key)
        if len(keys_to_process) > 5:
            break

    # Create Spark job
    # keys_to_process = [key for key in keys_to_process if int(key.split('/')[0]) > 2003]

    sc = SparkContext().getOrCreate()

    c = 0
    for chunk in chunks(keys_to_process, 5):
        rdd = sc.parallelize(chunk, 24)  # Todo: read with s3a instead and union together
        edges = rdd.map(process).cache()
        edges.coalesce(1).saveAsTextFile("edges_{}".format(c))
        c += 1

    # Combine edge data
    files = glob.glob('edge*/part*')
    os.system('cat {} > edge_data.csv'.format(' '.join(files)))
    folders = glob.glob('edge*/')
    for folder in folders:
        shutil.rmtree(folder)

if __name__ == '__main__':
    main()
