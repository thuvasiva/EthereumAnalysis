import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate() 
    
    def good_line_t(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[11])
            int(fields[8])
            return True
        except:
            return False
        
    def good_line_c(line):
        try:
            fields = line.split(',')
            if len(fields) != 6:
                return False
            return True
        except:
            return False
        
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
    
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    lines_t = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines_t = lines_t.filter(good_line_t)
    lines_c = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    clean_lines_c = lines_c.filter(good_line_c)
    t_features = clean_lines_t.map(lambda l: (l.split(',')[6], (int(l.split(',')[8]), time.strftime("%m %Y",time.gmtime(int(l.split(',')[11])))))) #(toAddress, (gas, time))
    c_features = clean_lines_c.map(lambda l: (l.split(',')[0], l.split(',')[1])) #(address, bytecode) 
    contract_transactions = t_features.join(c_features) #(address, ((gas,time),bytecode))
    gas_per_tran = contract_transactions.map(lambda l: (l[1][0][1],l[1][0][0])) #(time,gas)
    monthly_tran_counts = spark.sparkContext.broadcast(gas_per_tran.countByKey()) #number of transactions per month
    gas_used_per_month = gas_per_tran.reduceByKey(operator.add)
    avg_gas_used = gas_used_per_month.map(lambda b: (b[0], b[1]/monthly_tran_counts.value[b[0]]))
       
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum' + date_time + '/gas_used_over_time.txt')
    my_result_object.put(Body=json.dumps(avg_gas_used.take(100)))
  
    spark.stop()
    
    
    