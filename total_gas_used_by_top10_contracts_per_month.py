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
        
    def popular_contracts_filter(line):
        try:
            fields = line.split(',')
            if fields[0] == '0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444' or fields[0] == '0x7727e5113d1d161373623e5f49fd568b4f543a9e' or fields[0] == '0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef' or fields[0] == '0xbfc39b6f805a9e40e77291aff27aee3c96915bdd' or fields[0] == '0xe94b04a0fed112f3664e45adb2b8915693dd5ff3' or fields[0] == '0xabbb6bebfa05aa13e908eaa492bd7a8343760477' or fields[0] == '0x341e790174e3a4d35b65fdc067b6b5634a61caea' or fields[0] == '0x58ae42a38d6b33a1e31492b60465fa80da595755' or fields[0] == '0xc7c7f6660102e9a1fee1390df5c76ea5a5572ed3' or fields[0] == '0xe28e72fcf78647adce1f1252f240bbfaebd63bcc':
                return True
            return False
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
    popular_contracts = clean_lines_c.filter(popular_contracts_filter)  
    t_features = clean_lines_t.map(lambda l: (l.split(',')[6], (int(l.split(',')[8]), time.strftime("%m %Y",time.gmtime(int(l.split(',')[11])))))) #toAddress, (gas, time)
    c_features = popular_contracts.map(lambda l: (l.split(',')[0], l.split(',')[1])) #address, bytecode
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
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum' + date_time + '/gas_used_by_popular_contracts.txt')
    my_result_object.put(Body=json.dumps(avg_gas_used.take(1000)))
  
    spark.stop()
    
    
    