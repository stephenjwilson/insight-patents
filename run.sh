
#!/usr/bin/env bash

# Run script to decompress and process files on ec2 spark cluster
spark-submit --master spark://ec2-35-153-14-116.compute-1.amazonaws.com:7077 --py-files ~/insight-patents/dist/insight_patents-0.0.0-py3.5.egg --num-executors 4 --total-executor-cores 4 ~/insight-patents/src/python/parse_patents.py > spark_log.txt