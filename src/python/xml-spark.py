"""

"""
from pyspark import SparkContext
# from pyspark.sql import SQLContext
# import boto3
#
#
# # def main(bucket_name='patent-xml'):
# bucket_name='patent-xml'
# s3 = boto3.resource('s3')
# my_bucket = s3.Bucket(bucket_name)
# keys_to_process = []
# for my_object in my_bucket.objects.all():
#     if ".json" in my_object.key:
#         continue
#     keys_to_process.append(my_object.key)
#
# #sc = SparkContext().getOrCreate()
# sql_context = SQLContext(sc)
# row_tag = 'us-patent-grant'
# for key in keys_to_process:
#     fl = "s3a://{}/{}".format(bucket_name, key)
#     df = sql_context.read.format('com.databricks.spark.xml').options(rowTag=row_tag).load(fl)
#     break


# if __name__ == '_main__':
#     main()