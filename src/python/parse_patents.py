"""
The main run script of the pipeline. It includes a main function that is used in spark submit.
"""
import datetime
import io
import json
import logging
import os
import zipfile
from IPython import embed
import boto3
import botocore
import psycopg2
from dotenv import load_dotenv, find_dotenv
from pyspark import SparkContext
import click
from neo4j.v1 import GraphDatabase

from src.python.parsers import PatentParser

# from parsers import PatentParser

BUCKET_NAME = 'patent-xml-zipped'


def to_csv(list_of_patents, sep=',', review_only=False):
    """
    Takes a list_of_patent_dictionaries and outputs a csv that can be uploaded into postgres
    :param list_of_patents: A list of Patent objects
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
    :param data: The string to send
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
    Decompresses the file.
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
    sc = SparkContext.getOrCreate()
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
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
            log.info("{} does not exist. Error: {}".format(key, e))
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
    log = log4jLogger.LogManager.getLogger(__name__)

    log.info("Starting {}".format(key))

    # Load environment variables
    load_dotenv(find_dotenv())

    # Download, decompress, and determine format of data.
    file_name = download_from_s3(key)
    # Decompresses to file. Doing in memory causes too large a footprint
    decompress_name = decompress(file_name)

    # Parse Data and output to csv
    patent_parser = PatentParser(decompress_name)

    log.warn("Parsed {}, with {} patents seen and {} processed".format(key, patent_parser.totalpatents,
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

    # Remove files
    os.remove(file_name)
    os.remove(decompress_name)

    if edges == '':
        log.warn('Not getting edges for {}!'.format(key))

    return edges


def to_neo4j(csv_edges, table='patents'):
    """
    Code and Cyper commands needed to upload the relationship data to neo4j.
    :param csv_edges: The string that contains the edges
    :param table: The string that dictates which table will be used to get node information
    :return:
    """

    #embed()
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
    new_numbers = set(["'{}'".format(x.split(',')[0]) for x in csv_edges.split('\n') if x != ''])

    # Get Nodes from postgres to csv
    HOST = os.getenv("POSTGRES_HOST")
    USER = os.getenv("POSTGRES_USER")
    PASS = os.getenv("POSTGRES_PASSWORD")
    conn = psycopg2.connect(host=HOST, dbname="patent_data", user=USER, password=PASS)

    # Upload data
    cur = conn.cursor()
    query = "Select patent_number, title, owner, abstract From %s where patent_number in (%s)" % (
        table, ', '.join(new_numbers))
    with open('/home/ubuntu/tmp_nodes.txt', 'w') as f:
        # with open('tmp_nodes.txt', 'w') as f:
        cur.copy_expert("copy ({}) to stdout with csv header".format(query), f)

    patents_from_db = []
    data = {'patents': []}
    lines = open('/home/ubuntu/tmp_nodes.txt').readlines()
    for line in lines[1:]:
        line = line.strip('\n').split(',')
        if len(line) < 4:
            print(line)
            continue
        patents_from_db.append(line[0])
        data['patents'].append({'patent_number': line[0], 'title': line[1], 'owner': line[2], 'abstract': line[3]})
    # load node data
    query = '''
    WITH {data} as q
    UNWIND q.patents AS data
    MERGE (p: Patent {patent_number: data.patent_number })
    ON CREATE SET p = {patent_number: data.patent_number, title: data.title, owner: data.owner, 
    abstract: data.abstract}
    ON MATCH SET p += {patent_number: data.patent_number, title: data.title, owner: data.owner, 
    abstract: data.abstract}'''
    session.run(query, parameters={'data': data})

    # Set up Patent Relationships
    data = {'links': []}
    lines = csv_edges.split('\n')
    for line in lines:
        line = line.strip().split('\t')
        # limit edges that do not exist in DB
        if line[0] not in patents_from_db:
            continue
        if line[1] not in patents_from_db:
            continue
        data['links'].append({'citation': line[1], 'patent_number': line[0]})

    query = '''
    WITH {data} as q
    UNWIND q.links AS data
    MERGE (f: Patent {patent_number: data.patent_number})
    MERGE (t: Patent {patent_number: data.citation})
    MERGE (f)-[:CITES]->(t)
    '''
    session.run(query, parameters={'data':data})
    session.sync()
    os.remove('/home/ubuntu/tmp_nodes.txt')


def nonbulk_process(date):
    """
    Processes files in a processive non-spark manner.
    :return:
    """
    date = datetime.datetime.strptime(date, "%Y%m%d")
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(BUCKET_NAME)
    keys_to_process = []
    for my_object in my_bucket.objects.all():
        if ".json" in my_object.key:
            continue
        # 2001 files are available in two formats. Don't use the SGML format
        if my_object.key.split('/')[1][:2] == '01':
            continue
        keys_to_process.append(my_object.key)
    #embed()
    for key in keys_to_process:
        if str(date.year)[-2:]!= key[-6:-4]:
            continue
        print(key, str(date.month).zfill(2),key[-4:-2])
        if str(date.month).zfill(2) == key[-4:-2]:
            if str(date.day).zfill(2) == key[-2:]:
                edges = process(key.strip())
                # Push edges to Neo4j
                to_neo4j(edges)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


@click.command()
@click.option('--local', default=False, help='Saves files locally if True, otherwise on S3 ')
@click.option('--bulk', default=True, type=bool,
              help='''Processes the files based on all possible files on the S3 if True. 
              If False, the pipeline will use the keys in the local file: downloaded_files.txt''')
@click.option('--date', default=None, help='This is the date to focus on in non-bulk mode.')
def main(local, bulk, date):
    """
    Main Function that downloads, decompresses, and parses the USTPO XML data before dumping it into a
    PostgreSQL and a Neo4j database.
    :return:
    """

    # Ensure that the postgres table is there
    ensure_postgres()

    # Don't create a spark job if updating a few files

    if not bulk:
        if date is not None:
            nonbulk_process(date)
        return

    # Get list of keys to process
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(BUCKET_NAME)
    keys_to_process = []
    for my_object in my_bucket.objects.all():
        if ".json" in my_object.key:
            continue
        # 2001 files are available in two formats. Don't use the SGML format
        if my_object.key.split('/')[1][:2] == '01':
            continue
        keys_to_process.append(my_object.key)

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
    for chunk in chunks(keys_to_process, 6):
        if "edges_{}".format(c) in edge_files:  # Skip files that already exist
            log.info("edges_{} already exists".format(c))
            c += 1
            continue
        rdd = sc.parallelize(chunk, 6)
        edges = rdd.map(process)
        if local:  # Option to test locally
            edges.filter(lambda x: x != "\n" and x != "").saveAsTextFile(
                "{}/{}".format(edge_bucket, "edges_{}".format(c)))
        else:
            edges.filter(lambda x: x != "").saveAsTextFile(
                "s3a://{}/{}".format(edge_bucket, "edges_{}".format(c)))
        c += 1
        log.info("edges_{} created".format(c))


if __name__ == '__main__':
    main()
