#!/usr/bin/env bash

cd /home/ubuntu/insight-patents/src/bash/
mkdir neo4jEdges
cd neo4jEdges
aws s3 cp s3://edges-to-neo4j . --recursive
array=(edges_*/p*)

cat ${array[@]} > combined_edges.csv
sed -i "1s/.*/:START_ID(Patent),:END_ID(Patent)/" combined_edges.csv
psql -h test-db2.co89ijjtewjb.us-east-1.rds.amazonaws.com -p 5432 -U swilson patent_data -c "\copy (Select patent_number, title From patents) To '/home/ubuntu/insight-patents/src/bash/neo4jEdges/tmp.csv' With CSV"
echo "patent_number:ID(Patent),title" > node_header.csv
cat node_header.csv tmp.csv > nodes.csv

python3 ~/insight-patents/src/python/quick_nodes.py combined_edges.csv nodes.csv

# Configure neo4j?

sudo rm -rf /var/lib/neo4j/data/databases/graph1.db
sudo /usr/bin/neo4j-import --into /var/lib/neo4j/data/databases/graph1.db --id-type string --nodes:Patent nodes.csv --relationships:CITES combined_edges.csv
sudo chown -R neo4j:nogroup /var/lib/neo4j/data/databases/graph1.db