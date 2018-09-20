"""

"""

import os
import shutil
import boto3
import datetime
import logging
import xml.etree.cElementTree as ET
import zipfile
import botocore
from itertools import chain
import psycopg2
from IPython import embed
from dotenv import load_dotenv, find_dotenv
# from py2neo import Graph, Node, Relationship, authenticate

from pyspark import SparkContext
from neomodel import (config, StructuredNode, StringProperty, Relationship, StructuredRel, DateTimeProperty)


class PatentRel(StructuredRel):
    since = DateTimeProperty()
    met = StringProperty()


class Patent(StructuredNode):
    patent_number = StringProperty()
    title = StringProperty()
    citation = Relationship('Patent', 'OWNS', model=PatentRel)
BUCKET_NAME = 'patent-xml-zipped'


def parse_v1(file_name):
    """
    Parses v1 of the patent data.
    Credit goes to https://github.com/iamlemec/patents/blob/master/parse_grants.py#L18
    :return:
    """
    pat = None
    sec = None
    tag = None
    ipcver = None
    patents = []
    for nline in chain(open(file_name, encoding='latin1', errors='ignore'), ['PATN']):
        # peek at next line
        (ntag, nbuf) = (nline[:4].rstrip(), nline[5:-1].rstrip())
        if tag is None:
            tag = ntag
            buf = nbuf
            continue
        if ntag == '':
            buf += nbuf
            continue

        # regular tags
        if tag == 'PATN':
            if pat is not None:
                pat['ipclist'] = [(ipc, ipcver) for ipc in pat['ipclist']]
                patents.append(pat)
            pat = {}
            pat['gen'] = 1
            pat['ipclist'] = []
            pat['citlist'] = []
            sec = 'PATN'
        elif tag in ['INVT', 'ASSG', 'PRIR', 'CLAS', 'UREF', 'FREF', 'OREF', 'LREP', 'PCTA', 'ABST']:
            sec = tag
        # elif tag in ['PAL', 'PAR', 'PAC', 'PA0', 'PA1']:
        #     if sec == 'ABST':
        #         if 'abstract' not in pat:
        #             pat['abstract'] = buf
        #         else:
        #             pat['abstract'] += '\n' + buf
        elif tag == 'WKU':
            if sec == 'PATN':
                pat['patnum'] = buf
        elif tag == 'ISD':
            if sec == 'PATN':
                pat['grantdate'] = buf
        elif tag == 'APD':
            if sec == 'PATN':
                pat['filedate'] = buf
        elif tag == 'OCL':
            if sec == 'CLAS':
                pat['class'] = buf
        elif tag == 'ICL':
            if sec == 'CLAS':
                pat['ipclist'].append(buf)
        elif tag == 'EDF':
            if sec == 'CLAS':
                ipcver = buf
        elif tag == 'TTL':
            if sec == 'PATN':
                pat['title'] = buf
        elif tag == 'NCL':
            if sec == 'PATN':
                pat['claims'] = buf
        elif tag == 'NAM':
            if sec == 'ASSG':
                pat['owner'] = buf.upper()
        elif tag == 'CTY':
            if sec == 'ASSG':
                pat['city'] = buf.upper()
        elif tag == 'STA':
            if sec == 'ASSG':
                pat['state'] = buf
                pat['country'] = 'US'
        elif tag == 'CNT':
            if sec == 'ASSG':
                pat['country'] = buf[:2]
        elif tag == 'PNO':
            if sec == 'UREF':
                pat['citlist'].append(buf)

        # stage next tag and buf
        tag = ntag
        buf = nbuf

    return patents


def parse_v2():
    """
    Parses v2 of the patent data
    :return:
    """
    return


def parse_v3():
    """
    Parses v3 of the patent data
    :return:
    """
    return


# Ensure it's a grant?


def to_csv(list_of_patent_dictionaries, output):
    """
    Takes a list_of_patent_dictionaries and outputs a csv that can be uploaded into postgres
    :param patent_dictionary:
    :return:
    """
    fields = ['patnum', 'filedate', 'title', 'grantdate', 'owner', 'city', 'state', 'country', 'class']
    OUT = open(output, 'w')

    # OUT.write('\t'.join(fields) + '\tipcs\n')
    nodes = []
    edges = []

    for patent_dictionary in list_of_patent_dictionaries:

        s = ''
        # Deal with all other fields
        for field in fields:
            if field in patent_dictionary:
                val = patent_dictionary[field]
                if 'date' in field:
                    val = datetime.datetime.strptime(str(val), '%Y%m%d').strftime('%Y-%m-%d')
                s += '"{}"\t'.format(val)
            else:
                s += "\t"

        # Deal with ipcs
        ipcdata = '|'.join(["{}-{}".format(ipc[0], ipc[1]) for ipc in patent_dictionary['ipclist']])
        s += ipcdata + '\t'

        OUT.write(s[:-1] + "\n")

        # Deal with citations
        patent_number = patent_dictionary['patnum']
        nodes.append((patent_number, patent_dictionary['title']))
        for citation in patent_dictionary['citlist']:
            edges.append((patent_number, citation))
    OUT.close()
    # to_neo4j(nodes, edges)


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
                                                      'state', 'country', 'class', 'ipc'))
    conn.commit()
    conn.close()


def to_neo4j(nodes, edges):
    """
    TODO: swich to load CSV or other bulk methods
    :param file_name:
    :return:
    """

    # graph = Graph(ip_addr= os.getenv("NEO4J_IP"), username = "neo4j", password = os.getenv("NEO4J_PASSWORD"))
    # query = """
    # LOAD CSV WITH HEADERS FROM "%s" AS line
    # WITH line
    # WHERE line.citation IS NOT NULL
    # MERGE(patent: Patent{patent_number: line.patent_number, title: line.title})
    # MERGE(patent) -> (line.citation)""" % file_name
    # graph.run(query)
    # embed()
    # exit()
    # node_d ={}
    # for node in nodes:
    #     node_d[node['patent_number']] = Patent.create_or_update(node)[0].save()
    config.DATABASE_URL = os.getenv("NEO4J_HOST")
    node_d = {}
    for node in nodes:
        node_d[node[0]] = Patent.nodes.get_or_none(patent_number=node[0])
        if node_d[node[0]] is None:
            node_d[node[0]] = Patent(patent_number=node[0], title=node[1]).save()
        else:
            node_d[node[0]].title = node[1]

    for edge in edges:
        # Patent.get_or_create(edge[0]).citation.connect(Patent.get_or_create(edge[1]))
        try:
            node_d[edge[0]].citation.connect(node_d[edge[1]])
        except KeyError:
            node_d[edge[1]] = Patent(patent_num=edge[1]).save()  # TODO: ensure edge[1] gets title later
            node_d[edge[0]].citation.connect(node_d[edge[1]])


def determine_patent_type(file_name):
    """
    Determines patent version and parser
    :param file_name:
    :return:
    """

    if 'aps' in file_name:
        gen = 1
        parser = parse_v1
    elif file_name.startswith('pgb'):
        gen = 2
        parser = parse_v2
    elif file_name.startswith('ipgb'):
        gen = 3
        parser = parse_v3
    else:
        print("in format")
        embed()
        raise (Exception('Unknown format'))
    return gen, parser


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


def process(key):
    """

    :param key:
    :return:
    """
    print(key)
    load_dotenv(find_dotenv())
    file_name = download_from_s3(key)
    decompress_name = decompress(file_name)
    gen, parser = determine_patent_type(decompress_name)
    patents = parser(decompress_name)
    output_file = key.replace('/', '_') + '.csv'
    to_csv(patents, output_file)
    to_postgres(output_file)
    # TODO: clean up all files
    # Clean Up files
    # os.remove(file_name)
    # os.remove(decompress_name)
    # os.remove(output_file)


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
        if len(keys_to_process) > 2:
            break
    keys_to_process = sc.parallelize(keys_to_process)
    keys_to_process.map(process).collect()
    # for key in keys_to_process:
    #     process(key)


if __name__ == '__main__':
    main()
