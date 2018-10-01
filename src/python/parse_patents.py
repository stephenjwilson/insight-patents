"""
The main run script of the pipeline. It includes a main function that is used in spark submit.
"""

import io
import logging
import os
import zipfile
from IPython import embed
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
    edges = ""  # ""patent_number{}citation\n".format(sep)
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

    conn.commit()
    conn.close()


def decompress(file_name):
    """
    Decompresses the file.  TODO: consider extracting to memory instead of disk
    :return: Returns the name of the decompressed file
    """
    zip_ref = zipfile.ZipFile(file_name, 'r')
    name = zip_ref.namelist()[0]
    return name, zip_ref.read(name)


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


def process(data):
    """
    Applys an ETL pipeline for patents on a particular key from a S3 bucket
    :param key: This is the key for the S3 Bucket
    """
    sc = SparkContext.getOrCreate()
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    # LOGGER.setLevel("WARN")

    try:
        # Load environment variables
        load_dotenv(find_dotenv())

        # Download, decompress, and determine format of data TODO: do in memory

        # Parse Data and output to csv
        patent_parser = PatentParser(data)

        log.warn("Parsed with {} patents seen and {} processed".format(patent_parser.totalpatents,
                                                                       len(patent_parser.patents)))
        log.warn("{} patents with citations".format(patent_parser.citationpatents))
        log.warn("{} citations\n".format(sum([len(x.citations) for x in patent_parser.patents])))

        nodes, edges = to_csv(patent_parser.patents)

        # send nodes to postgres
        try:
            to_postgres(nodes)
        except psycopg2.IntegrityError:
            pass

        # send out malformed data if needed
        if sum([x.flagged_for_review for x in patent_parser.patents]) > 0:
            nodes, _ = to_csv(patent_parser.patents, review_only=True)
            # send nodes to postgres
            try:
                to_postgres(nodes, table='patents_to_review')
            except psycopg2.IntegrityError:
                pass

    except Exception as e:
        log.warn('Failed with exception: {}'.format(e))
        edges = ''
    if edges == '':
        log.warn('Not getting edges')

    return edges


def clean_and_split(data, name):
    split_strings = {
        'aps': 'PATN',
        'ipg': '<us-patent-grant',
        'pg': '<PATDOC'
    }
    # split file as appropriate
    if 'aps' in name:
        split_string = split_strings['aps']
        chunks = [split_string + x for x in data.decode('utf-8').split(split_string)][1:]
    elif 'ipg' in name:
        split_string = split_strings['ipg']
        chunks = [split_string + x for x in data.decode('utf-8').split(split_string)][1:]
    elif 'pg' in name:
        split_string = split_strings['pg']
        chunks = [split_string + x for x in data.decode('utf-8').split(split_string)][1:]
    else:
        chunks = []
    return chunks


def chunk_list(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def main(local=True, num_of_patents_to_process=10000, partitions=2):
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
    # process(keys_to_process[0])

    # Create Spark Context
    sc = SparkContext().getOrCreate()
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    # Get list of all files already processed
    edge_bucket = 'edges-to-neo4j'
    s3.create_bucket(Bucket=edge_bucket)
    my_bucket = s3.Bucket(edge_bucket)
    edge_files = []
    for my_object in my_bucket.objects.all():
        edge_files.append(my_object.key.split('/')[0])

    # Process keys in chunks
    c = 0
    all_chunks = []
    for key in keys_to_process:
        # Todo: read with s3a instead and union together
        if "edges_{}".format(c) in edge_files:  # Skip files that already exist
            log.info("edges_{} already exists".format(c))
            c += 1
            continue
        # get file
        file_name = download_from_s3(key)
        name, data = decompress(file_name)
        # os.remove(file_name)

        chunks = clean_and_split(data, name)
        if not chunks:
            log.info("key {} failed on split".format(key))
            continue

        all_chunks += chunks
        if len(all_chunks) > num_of_patents_to_process:
            pieces = [''.join(x) for x in chunk_list(all_chunks, int(len(all_chunks)/partitions))]
            # edges = process(pieces[0])
            # embed()
            # exit()
            # Distribute data
            rdd = sc.parallelize(pieces, partitions)
            edges = rdd.map(process).collect()
            if local:
                edges.filter(lambda x: x != "\n" and x != "").coalesce(1).saveAsTextFile(
                    "{}/{}".format(edge_bucket, "edges_{}".format(c)))
            else:
                edges.filter(lambda x: x != "\n" and x != "").coalesce(1).saveAsTextFile(
                    "s3a://{}/{}".format(edge_bucket, "edges_{}".format(c)))
            c += 1
            log.info("edges_{} created".format(c))
            exit()
            all_chunks = []


if __name__ == '__main__':
    main()
