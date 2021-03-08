#!/bin/bash

echo 00423
python parquet_parse_spark.py -i /gscratch/comdata/output/reddit_comments_by_subreddit.parquet/part-00423-64cc16c6-ae78-43f2-a6a6-ea047f6fbdaa-c000.snappy.parquet
#sleep 5
#python parquet_parse_spark.py -i /gscratch/comdata/output/reddit_submissions_by_subreddit.parquet/part-00164-d61007de-9cbe-4970-857f-b9fd4b35b741-c000.snappy.parquet
sleep 5

echo 00023
python parquet_parse_spark.py -i /gscratch/comdata/output/reddit_submissions_by_subreddit.parquet/part-00023-d61007de-9cbe-4970-857f-b9fd4b35b741-c000.snappy.parquet
#sleep 5
#python parquet_parse_spark.py -i /gscratch/comdata/output/reddit_submissions_by_subreddit.parquet/part-00110-d61007de-9cbe-4970-857f-b9fd4b35b741-c000.snappy.parquet

