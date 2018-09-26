"""
The main run script of the pipeline. It includes a main function that is used in spark submit.
"""

import logging
import os
import zipfile

import boto3
import botocore
import psycopg2
from dotenv import load_dotenv, find_dotenv
from neo4j.v1 import GraphDatabase
from pyspark import SparkContext
from src.python.parsers import PatentParser

#from parsers import PatentParser

BUCKET_NAME = 'patent-xml-zipped'


def to_csv(list_of_patents, output, sep=','):
    """
    Takes a list_of_patent_dictionaries and outputs a csv that can be uploaded into postgres
    :param list_of_patents: A list of Patent objects
    :param output: The file name used to output the files
    :param sep: The separator for the csv file
    :return: postgres file, neo4j nodes file, neo4j edges file
    """
    fields = ['patent_number', 'file_date', 'grant_date', 'title', 'abstract', 'owner', 'country', 'ipcs']
    OUT = open(output, 'w')  # input into postgres
    OUT2 = open(output.replace('.csv', '') + '_nodes.csv', 'w')  # input into neo4j nodes
    OUT3 = open(output.replace('.csv', '') + '_edges.csv', 'w')  # input into neo4j edges
    OUT3.write("patent_number{}citation\n".format(sep))

    OUT2.write(sep.join(fields) + '{}ipcs\n'.format(sep))

    for patent in list_of_patents:
        s = patent.to_csv(fields, sep=sep)
        OUT.write(s)
        OUT2.write(s)

        # Deal with relationships
        OUT3.write(patent.to_neo4j_relationships())
    OUT.close()
    OUT2.close()
    OUT3.close()
    return output, output.replace('.csv', '') + '_nodes.csv', output.replace('.csv', '') + '_edges.csv'


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


def to_postgres(csv_file):
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
    cur.copy_from(open(csv_file), 'patents', columns=('patent_number', 'file_date', 'grant_date', 'title', 'abstract',
                                                      'owner', 'country', 'ipc'), sep=',')
    conn.commit()
    conn.close()


def to_neo4j(csv_nodes, csv_edges):
    """
    Code and Cyper commands needed to upload the relationship data to neo4j.
    :param csv_nodes: the file that contains the node information
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
    # Add Patents TODO: add other fields
    query = '''
    USING PERIODIC COMMIT 500
    LOAD CSV WITH HEADERS FROM "https://s3.amazonaws.com/tmpbucketpatents/%s"
    AS csvLine
    MERGE (p: Patent {patent_number: csvLine.patent_number })
    ON CREATE SET p = {patent_number: csvLine.patent_number, title: csvLine.title, owner: csvLine.owner, 
    abstract: csvLine.abstract}
    ON MATCH SET p += {patent_number: csvLine.patent_number, title: csvLine.title, owner: csvLine.owner, 
    abstract: csvLine.abstract}''' % csv_nodes
    session.run(query)

    # Set up Patent Relationships
    session.sync()

    query = '''
    USING PERIODIC COMMIT 500
    LOAD CSV WITH HEADERS FROM "https://s3.amazonaws.com/tmpbucketpatents/%s"
    AS csvLine
    MERGE (f: Patent {patent_number: csvLine.patent_number})
    MERGE (t: Patent {patent_number: csvLine.citation})
    MERGE (f)-[:CITES]->(t)
    ''' % csv_edges
    session.run(query)
    session.sync()


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


def push_to_s3(key, file_name, bucket=None):
    """
    Uploads a file to the s3 bucket.
    :param key: The file key in the S3 bucket
    :param file_name: The name of the local file
    :param bucket: The name of the S3 bucket. Can be retrieved from the variable BUCKET_NAME
    :return:
    """
    if bucket is None:
        bucket = BUCKET_NAME
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    my_bucket.put_object(Key=key, Body=open(file_name, 'rb'))


def process(key):
    """
    Applys an ETL pipeline for patents on a particular key from a S3 bucket
    :param key: This is the key for the S3 Bucket
    """
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    logging.info("Starting {}".format(key))
    # Load environment variables
    load_dotenv(find_dotenv())
    # Download, decompress, and determine format of data
    file_name = download_from_s3(key)
    decompress_name = decompress(file_name)

    # Parse Data and output to csv
    patent_parser = PatentParser(decompress_name)
    output_file = key.replace('/', '_') + '.csv'
    logging.info("Parsed {}".format(key))
    output_file, output_file2, output_file3 = to_csv(patent_parser.patents, output_file)

    # Dump patent data to Postgres
    try:
        to_postgres(output_file)
    except psycopg2.IntegrityError:
        pass
    # logging.info("Pushed {} to PostgreSQL".format(key))

    # Push intermediate files to s3
    fl1 = key + '_nodes.csv'
    fl2 = key + '_edges.csv'
    os.system('hdfs dfs -copyFromLocal {0} /user/{0}'.format(fl1))
    os.system('hdfs dfs -copyFromLocal {0} /user/{0}'.format(fl2))
    #logging.info("Pushed {} to HDFS".format(key))
    # push_to_s3(fl1, output_file2, bucket="tmpbucketpatents")
    # push_to_s3(fl2, output_file3, bucket="tmpbucketpatents")

    # Use intermediate files on s3 to load into neo4j
    #to_neo4j(fl1, fl2)
    # logging.info("Pushed {} to Neo4j".format(key))

    # Clean Up files
    os.remove(file_name)
    os.remove(decompress_name)
    os.remove(output_file)
    #os.remove(output_file2)
    #os.remove(output_file3)


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
        if len(keys_to_process) > 20:
            break
    # Create Spark job
    # keys_to_process = [key for key in keys_to_process if int(key.split('/')[0]) > 2003]
    sc = SparkContext().getOrCreate()
    
    for i in range(0,len(keys_to_process)):
        if i%24:
            rdd = sc.parallelize(keys_to_process[i-24:i], 24)
            rdd.foreach(process)#.collect()
    # details.saveAsTextFile("tmp4")
    # For Dev
    # for key in keys_to_process:
    #     process(key)
    #     break
    # process('1976/19760120')
    #process('2003/030107')
    #process('2018/180102')


if __name__ == '__main__':
    main()
