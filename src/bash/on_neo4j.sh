#!/usr/bin/env bash

mkdir neo4jEdges
cd neo4jEdges
aws s3 cp s3://edges-to-neo4j . --recursive
array=(edges_*/p*)

cat ${array[@]} > combined_edges.csv