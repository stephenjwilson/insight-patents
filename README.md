# Table of Contents
1. [Introduction](README.md#introduction)
2. [Approach](README.md#approach)
3. [Requirements](README.md#requirements)
4. [Running the code](README.md#running-the-code)
5. [Testing](README.md#testing)
6. [Author](README.md#author)

# Introduction
Patents determine the exclusive right to produce, market, and sell something. Patents
can be extraordinarily profitable, for better or worse. The field of pharmaceutical 
patents in particular produces profits in the tens of billions for a single company. 
However, there many pharmaceutical patents that are invalid because the compound was
 never developed, they still required clinical trials for F.D.A. approval, or the 
 drug was previously discussed in an academic article, see [Ronin, 2009](http://nrs.harvard.edu/urn-3:HUL.InstRepos:10611775).
For patent lawyers or for scientific researchers, it is critical to know how patents
relate to each other, how they relate to previous pharmaceutical compounds, and how patents
relate to the scientific research. Thi is a fundamental gap that many in the industry 
are addressing either by manual, intensive patent reviews or by attempts a [free text
searches](https://patents.google.com/) combined with extensive manual patent reviews.
In this project, I attempt to aid in the exploration of patents by creating a pipeline
that processes USPTO files into a front-end network of relationships between patents, 
chemicals, and scientific research.

# Approach
Current proposed pipeline:
1. USTPO XML --> AWS S3
2. Apache Spark processing of XML on EC2 Hadoop cluster
    1. Basic patent information to a Postgres database
    2. Relationship information generated from Postgres data stored in Neo4j
    3. Log spark jobs into Postgres database
3. Front End Visualization of network using react.js / neo4j
4. Luigi orchestration of pipeline and weekly updates with new patent XML


# Requirements
Languages:
* Python 3.6
* Scala

Third-Party Libraries:
* pyspark
* boto

# Running the Code:
This repository's code can be run with:
```bash
./run.sh
```

## Testing
  
Tests can be run by running with:
```bash
./run_tests.sh
``` 

# Author
Created by Stephen J. Wilson