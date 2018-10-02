"""
The main run script of the pipeline. It includes a main function that is used in spark submit.
"""
import gc
import io
import logging
import os
import shutil
import zipfile
from collections import defaultdict

import boto3
import botocore
import click
import psycopg2
from dotenv import load_dotenv, find_dotenv
from neo4j.v1 import GraphDatabase
from pyspark import SparkContext

from src.python.parsers import PatentParser

# from parsers import PatentParser

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
    return name, zip_ref.read(name).decode('latin1')


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


def chunk_list(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def to_neo4j(csv_edges, table='patents'):
    """
    Code and Cyper commands needed to upload the relationship data to neo4j.

    :param csv_edges:
    :return:
    """
    # Set-Up Patents
    driver = GraphDatabase.driver(os.getenv("NEO4J_URI"), auth=("neo4j", os.getenv("NEO4J_PASSWORD")))
    session = driver.session()
    # Add Index and Constraints
    # query = '''CREATE INDEX ON :Patent(patent_number)'''
    # session.run(query)
    query = '''CREATE CONSTRAINT ON (p:Patent) ASSERT p.patent_number IS UNIQUE'''
    session.run(query)
    session.sync()

    # Get all relevant new patent numbers
    new_numbers = ["'{}'".format(x.split(',')[0]) for x in csv_edges.split('\n')]

    # TODO: get Nodes from postgres to csv
    HOST = os.getenv("POSTGRES_HOST")
    USER = os.getenv("POSTGRES_USER")
    PASS = os.getenv("POSTGRES_PASSWORD")
    conn = psycopg2.connect(host=HOST, dbname="patent_data", user=USER, password=PASS)

    # Upload data
    cur = conn.cursor()
    query = "Select patent_number, title From {} where patent_number = ANY ({})".format(table, ','.join(new_numbers))
    with open('/home/ubuntu/tmp_nodes.txt', 'w') as f:
        cur.copy_expert("copy ({}) to stdout with csv header".format(query), f)

    # load node data
    query = '''
    LOAD CSV WITH HEADERS FROM "https://s3.amazonaws.com/tmpbucketpatents/%s"
    AS csvLine
    MERGE (p: Patent {patent_number: csvLine.patent_number })
    ON CREATE SET p = {patent_number: csvLine.patent_number, title: csvLine.title, owner: csvLine.owner, 
    abstract: csvLine.abstract}
    ON MATCH SET p += {patent_number: csvLine.patent_number, title: csvLine.title, owner: csvLine.owner, 
    abstract: csvLine.abstract}''' % csv_nodes
    session.run(query)

    # Set up Patent Relationships
    # session.sync()
    f = open('/home/ubuntu/tmp_edges.txt', 'w')
    f.write(csv_edges)
    f.close()

    query = '''
    LOAD CSV WITH HEADERS FROM "file:///home/ubuntu/tmp_edges.txt"
    AS csvLine
    MERGE (f: Patent {patent_number: csvLine.patent_number})
    MERGE (t: Patent {patent_number: csvLine.citation})
    MERGE (f)-[:CITES]->(t)
    '''
    session.run(query)
    session.sync()


def nonbulk_process():
    keys = open("downloaded_files.txt").readlines()
    for key in keys:
        edges = process(key.strip())
        # Push edges to Neo4j
        to_neo4j(edges)


def process(key):
    """
    Applys an ETL pipeline for patents on a particular key from a S3 bucket
    :param key: This is the key for the S3 Bucket
    """
    sc = SparkContext.getOrCreate()
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    # LOGGER.setLevel("WARN")

    # try:
    if 1:
        # Load environment variables
        load_dotenv(find_dotenv())

        # Download, decompress, and determine format of data
        file_name = download_from_s3(key)
        name, data = decompress(file_name)
        os.remove(file_name)
        chunks = clean_and_split(data, name)
        # Parse Data and output to csv
        patent_parser = PatentParser(''.join(chunks))

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
        log.warn("init postgres")
        # send out malformed data if needed
        if sum([x.flagged_for_review for x in patent_parser.patents]) > 0:
            nodes, _ = to_csv(patent_parser.patents, review_only=True)
            # send nodes to postgres
            try:
                to_postgres(nodes, table='patents_to_review')
            except psycopg2.IntegrityError:
                pass
        log.warn("next postgres")

    # except Exception as e:
    #    log.warn('Failed with exception: {}'.format(e))
    #    edges = ''
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
        chunks = [split_string + x for x in data.split(split_string)][1:]
    elif 'ipg' in name:
        split_string = split_strings['ipg']
        chunks = [split_string + x for x in data.split(split_string)][1:]
    elif 'pg' in name:
        split_string = split_strings['pg']
        chunks = [split_string + x for x in data.split(split_string)][1:]
    else:
        chunks = []
    return chunks


@click.command()
@click.option('--local', default=False, help='Saves files locally if True, otherwise on S3 ')
@click.option('--partitions', default=10, help='The number of partitions to split the patents into for each spark job')
@click.option('--bulk', default=True,
              help='Processes the files based on all possible files on the S3 if True. If False, the pipeline will use the keys in the local file: downloaded_files.txt')
def main(local, partitions, bulk):
    """
    Main Function that downloads, decompresses, and parses the USTPO XML data before dumping it into a
    PostgreSQL and a Neo4j database.
    :return:
    """

    # Ensure that the postgres table is there
    ensure_postgres()
    # Don't create a spark job if updating a few files
    if not bulk:
        nonbulk_process()
        return
    # Get list of keys to process
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(BUCKET_NAME)
    keys_to_process = defaultdict(list)
    for my_object in my_bucket.objects.all():
        if ".json" in my_object.key:
            continue
        year, date = my_object.key.split('/')
        keys_to_process[year].append(my_object.key)

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

    for year in keys_to_process:

        # Distribute data
        output_name = year
        if output_name in edge_files:  # Skip files that already exist
            log.info("{} already exists".format(output_name))
            continue
        rdd = sc.parallelize(keys_to_process[year], partitions)
        edges = rdd.map(process)

        if local:
            try:
                edges.filter(lambda x: x != "\n" and x != "").coalesce(1).saveAsTextFile(
                    "{}/{}".format(edge_bucket, output_name))
            except:
                shutil.rmtree("{}/{}".format(edge_bucket, output_name))
                edges.filter(lambda x: x != "\n" and x != "").coalesce(1).saveAsTextFile(
                    "{}/{}".format(edge_bucket, output_name))
        else:
            edges.filter(lambda x: x != "\n" and x != "").coalesce(1).saveAsTextFile(
                "s3a://{}/{}".format(edge_bucket, output_name))

        edges.unpersist()
        rdd.unpersist()
        log.info("{} created".format(output_name))
        gc.collect()


if __name__ == '__main__':
    main()
