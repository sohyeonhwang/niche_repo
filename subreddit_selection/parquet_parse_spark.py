import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql import types
import argparse
import glob
import csv
from datetime import datetime
import re
from pathlib import Path
import os
import time 

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    parser.add_argument('-i', '--input', help='Tsv file of wiki edits. Supports wildcards ', required=True, type=str)
    parser.add_argument('-o', '--output-dir', help='Output directory', default='./', type=str)
    parser.add_argument('--output-format', help = "[csv, parquet] format to output",type=str,default='csv')
    parser.add_argument('--num-partitions', help = "number of partitions to output",type=int, default=1)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    start_time = time.time()
    args = parse_args()
    conf = SparkConf().setAppName("Reddit spark for the memory-limit-hitting files")
    spark = SparkSession.builder.getOrCreate()

    files = glob.glob(args.input)
    files = [os.path.abspath(p) for p in files]
    print(files[0], len(files))

    input('continue?')

    reader = spark.read

    # data
    pardf = reader.parquet(files[0])
    pardf = pardf.na.drop(subset=["subreddit"])
    df = pardf.select(pardf.id,pardf.subreddit,pardf.author,pardf.subreddit_id,pardf.Year,pardf.Month)
    df = df.filter( df.Year >= 2015 )
    df = df.withColumn('del_auth', f.when(df.author != "[deleted]", 0).otherwise(1) )
    #df.show(n=5)

    # make the variables
    out_df = df.groupBy("subreddit","Year","Month").agg(f.count("id").alias("id_count"), f.countDistinct("author").alias("author_nunique"), f.sum("del_auth").alias("del_auth_sum"))
    out_df.show(n=3)

    # key df
    key_df = df.select(df.subreddit,df.subreddit_id).dropDuplicates()
    #key_df.show(n=5)

    # export
    if "comments" in args.input:
        out_base = 'comments' #args.input[62:]
    if "submissions" in args.input:
        out_base = 'submissions' #args.input[65:]

    # coalesce(1) makes the output one file
    out_path = 'out_{}.tsv'.format(out_base)
    #out_df.coalesce(1).write.csv(out_path,sep='\t',mode='append',header=True)
    out_df.write.csv(out_path,sep='\t',mode='append',header=True)

    key_path = 'key_out_{}.tsv'.format(out_base)
    key_df.coalesce(1).write.csv(key_path,sep='\t',mode='append',header=True)

    print("{}\n{}".format(out_path,key_path))

