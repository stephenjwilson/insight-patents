#!/usr/bin/env bash

cd /var/lib/neo4j/data
sudo mkdir neo4jEdges
sudo chown ubuntu neo4jEdges
cd neo4jEdges
aws s3 cp s3://edges-to-neo4j . --recursive
array=(edges_*/p*)

cat ${array[@]} > combined_edges.csv

psql -h test-db2.co89ijjtewjb.us-east-1.rds.amazonaws.com -p 5432 -U swilson patent_data -c "\copy (Select patent_number, file_date, title From patents) To '/var/lib/neo4j/data/neo4jEdges/tmp.csv' With CSV"

python3 ~/insight-patents/src/python/filter_edges.py tmp.csv combined_edges.csv

sed -i "1s/.*/:START_ID(Patent),:END_ID(Patent)/" trim_edges.csv
echo "patent_number:ID(Patent),file_date, title" > node_header.csv
cat node_header.csv tmp.csv > nodes.csv

# Configure neo4j?

sudo rm -rf /var/lib/neo4j/data/databases/graph2.db
sudo /usr/bin/neo4j-import --into /var/lib/neo4j/data/databases/graph2.db --id-type string --nodes:Patent nodes.csv --relationships:CITES trim_edges.csv
sudo chown -R neo4j:nogroup /var/lib/neo4j/data/databases/graph2.db
