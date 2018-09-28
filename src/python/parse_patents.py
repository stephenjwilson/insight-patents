"""
The main run script of the pipeline. It includes a main function that is used in spark submit.
"""

import io
import logging
import os
import zipfile

import boto3
import botocore
import psycopg2
from dotenv import load_dotenv, find_dotenv
from pyspark import SparkContext

from src.python.parsers import PatentParser

#from parsers import PatentParser

BUCKET_NAME = 'patent-xml-zipped'


def to_csv(list_of_patents, sep=',', review_only=False):
    """
    Takes a list_of_patent_dictionaries and outputs a csv that can be uploaded into postgres
    :param list_of_patents: A list of Patent objects
    :param output: The file name used to output the files
    :param sep: The separator for the csv file
    :param review_only: Dictates if the function should only return review articles
    :return: nodes string, edges string
    """
    fields = ['patent_number', 'file_date', 'grant_date', 'title', 'abstract', 'owner', 'country', 'ipcs']
    nodes = sep.join(fields) + '\n'
    edges = "patent_number{}citation\n".format(sep)

    for patent in list_of_patents:
        # Is the patent malformed?
        if review_only and patent.flagged_for_review:
            s = patent.to_csv(fields, sep=sep)
            nodes += s
            # don't make relationships
        elif not review_only and not patent.flagged_for_review:
            s = patent.to_csv(fields, sep=sep)
            nodes += s
            # Deal with relationships
            edges += patent.to_neo4j_relationships()
        else:
            pass  # ignore the patent
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
    cur.execute("""
    CREATE TABLE IF NOT EXISTS patents_to_review(
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


def to_postgres(data, table='patents'):
    """
    Do a bulk upload of a csv file to a PostgreSQL database.
    :param csv_file: The name of the csv file to upload
    :param table: The name of the table to commit to
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
               COPY {} FROM stdin WITH CSV HEADER
               DELIMITER as ','
               """.format(table)

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
    sc = SparkContext.getOrCreate()
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info("Starting {}".format(key))
    try:
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

        # send out malformed data
        nodes, edges = to_csv(patent_parser.patents, review_only=True)

        # send nodes to postgres
        try:
            to_postgres(nodes, table='patents_to_review')
        except psycopg2.IntegrityError:
            pass

        # Remove files
        os.remove(file_name)
        os.remove(decompress_name)
    except Exception as e:
        LOGGER.warn('Failed on {}, with excetption: {}'.format(key,e))

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
        # if len(keys_to_process) > 5:
        #    break

    # Create Spark job
    # keys_to_process = [key for key in keys_to_process if int(key.split('/')[0]) > 2003]
    sc = SparkContext().getOrCreate()
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    edge_bucket = 'edges-to-neo4j'
    s3.create_bucket(Bucket=edge_bucket)
    my_bucket = s3.Bucket(edge_bucket)
    edge_files = []
    for my_object in my_bucket.objects.all():
        edge_files.append(my_object.key.split('/')[0])

    c = 0
    for chunk in chunks(keys_to_process, 24):
        # Todo: read with s3a instead and union together
        # rdds = [sc.HadoopFile("s3a://{}/{}".format(BUCKET_NAME, key)) for key in chunk]
        # for i in range(1, len(rdds)):
        #     rdds[0].join(rdds[i])
        # rdd = rdds[0]
        if "edges_{}".format(c) in edge_files:
            LOGGER.info("edges_{} already exists".format(c))
            c+=1
            continue
        rdd = sc.parallelize(chunk, 24)
        edges = rdd.map(process).cache()
        edges.coalesce(1).saveAsTextFile("s3a://{}/{}".format(edge_bucket, "edges_{}".format(c)))
        c += 1
        LOGGER.info("edges_{} created".format(c))

    # # Combine edge data
    # files = glob.glob('edge*/part*')
    # os.system('cat {} > edge_data.csv'.format(' '.join(files)))
    # folders = glob.glob('edge*/')
    # for folder in folders:
    #     shutil.rmtree(folder)
    # LOGGER.info(os.path.abspath('edge_data.csv'))


if __name__ == '__main__':
    main()
