"""

"""

import os
import shutil
import boto3
import datetime
import logging
import zipfile
import botocore

import psycopg2
from IPython import embed
from dotenv import load_dotenv, find_dotenv
from neo4j.v1 import GraphDatabase
from pyspark import SparkContext
from parsers import determine_patent_type
BUCKET_NAME = 'patent-xml-zipped'

# TODO: Ensure it's a grant?


def to_csv(list_of_patent_dictionaries, output, sep=','):
    """
    Takes a list_of_patent_dictionaries and outputs a csv that can be uploaded into postgres
    :param list_of_patent_dictionaries:
    :param output
    :param sep
    :return: postgres file, neo4j nodes file, neo4j edges file
    """
    fields = ['patnum', 'filedate', 'title', 'grantdate', 'owner', 'city', 'state', 'country', 'class']
    OUT = open(output, 'w')  # input into postgres
    OUT2 = open(output.replace('.csv', '') + '_nodes.csv', 'w')  # input into neo4j nodes
    OUT3 = open(output.replace('.csv', '') + '_edges.csv', 'w')  # input into neo4j edges
    OUT3.write("patent_number{}citation\n".format(sep))

    OUT2.write(sep.join(fields) + '{}ipcs\n'.format(sep))
    nodes = []
    edges = []

    for patent_dictionary in list_of_patent_dictionaries:

        s = ''
        # Deal with all other fields
        for field in fields:
            if field in patent_dictionary:
                val = patent_dictionary[field].replace('"', '').replace(",", '')
                if 'date' in field:
                    val = datetime.datetime.strptime(str(val), '%Y%m%d').strftime('%Y-%m-%d')
                s += '"{}"{}'.format(val.strip(), sep)
            else:
                s += sep

        # Deal with ipcs
        ipcdata = '|'.join(["{}-{}".format(ipc[0], ipc[1]) for ipc in patent_dictionary['ipclist']])
        s += ipcdata + sep

        OUT.write(s[:-1] + "\n")
        OUT2.write(s[:-1] + "\n")

        # Deal with citations
        patent_number = patent_dictionary['patnum']
        nodes.append((patent_number, patent_dictionary['title']))
        for citation in patent_dictionary['citlist']:
            # edges.append((patent_number, citation))
            OUT3.write("{}{}{}\n".format(patent_number, sep, citation))
    OUT.close()
    OUT2.close()
    OUT3.close()
    return output, output.replace('.csv', '') + '_nodes.csv', output.replace('.csv', '') + '_edges.csv'


def to_postgres(csv_file):
    """
    Do a bulk upload to postgres
    :param csv_file:
    :return:
    """
    #logging.log("Pushed {} to postgres".format(csv_file))
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
        patnum text PRIMARY KEY,
        filedate date,
        title text,
        grantdate date,
        owner text,
        city text,
        state text,
        country text,
        class text,
        ipc text
    )
    """)

    # Upload data

    cur.copy_from(open(csv_file), 'patents', columns=('patnum', 'filedate', 'title', 'grantdate', 'owner', 'city',
                                                      'state', 'country', 'class', 'ipc'), sep=',')
    conn.commit()
    conn.close()


def new_neo4j(csv_nodes, csv_edges):
    """

    :param csv_nodes:
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
    # Add Patents
    query = '''
    LOAD CSV WITH HEADERS FROM "https://s3.amazonaws.com/tmpbucketpatents/%s"
    AS csvLine
    MERGE (p: Patent {patent_number: csvLine.patnum, title: csvLine.title})
    ON CREATE SET p = {patent_number: csvLine.patnum, title: csvLine.title}
    ON MATCH SET p += {patent_number: csvLine.patnum, title: csvLine.title}''' % csv_nodes
    session.run(query)

    # Set up Patent Relationships
    session.sync()

    query = '''
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
    :return:
    """
    zip_ref = zipfile.ZipFile(file_name, 'r')
    out_name = zip_ref.filelist[0].filename
    zip_ref.extractall('/'.join(file_name.split('/')[:-1]))
    zip_ref.close()
    return out_name


def download_from_s3(key, output_name=None):
    """

    :param key:
    :param output_name:
    :return:
    """
    if output_name is None:
        output_name = key.replace('/', '_')
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(BUCKET_NAME)
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

    :param key:
    :param file_name:
    :return:
    """
    if bucket is None:
        bucket = BUCKET_NAME
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    my_bucket.put_object(Key=key, Body=open(file_name, 'rb'))


def process(key):
    """

    :param key:
    :return:
    """
    print(key)
    # Load environment variables
    load_dotenv(find_dotenv())
    # Download, decompress, and determine format of data
    file_name = download_from_s3(key)
    decompress_name = decompress(file_name)
    gen, parser = determine_patent_type(decompress_name)

    # Parse Data and output to csv
    patents = parser(decompress_name)
    output_file = key.replace('/', '_') + '.csv'
    output_file, output_file2, output_file3 = to_csv(patents, output_file)

    # Dump patent data to Postgres
    try:
        to_postgres(output_file)
    except psycopg2.IntegrityError:
        pass

    # Push intermediate files to s3
    fl1 = key + '_nodes.csv'
    fl2 = key + '_edges.csv'
    push_to_s3(fl1, output_file2, bucket="tmpbucketpatents")
    push_to_s3(fl2, output_file3, bucket="tmpbucketpatents")

    # Use intermediate files on s3 to load into neo4j
    new_neo4j(fl1, fl2)

    # Clean Up files
    os.remove(file_name)
    os.remove(decompress_name)
    os.remove(output_file)
    os.remove(output_file2)
    os.remove(output_file3)


def main():
    """
    Main Function that downloads, decompresses, and parses the USTPO XML data
    :return:
    """
    sc = SparkContext().getOrCreate()

    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(BUCKET_NAME)
    keys_to_process = []
    for my_object in my_bucket.objects.all():
        if ".json" in my_object.key:
            continue
        keys_to_process.append(my_object.key)
        if len(keys_to_process) > 20:
            break
    keys_to_process = sc.parallelize(keys_to_process)
    keys_to_process.map(process).collect()

    # For Dev
    # for key in keys_to_process:
    #     process(key)
    # process('2003/030107')


if __name__ == '__main__':
    main()
