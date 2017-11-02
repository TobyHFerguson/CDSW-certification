#!/bin/bash
hdfs dfs -ls brfss/*.parquet/part-00000-* | 
awk '{ printf "/user/%s/%s\n", ENVIRON["HADOOP_USER_NAME"] , $8 }'| 
sed -n "s|\(\(.*brfss/\([0-9][0-9]*\).parquet\).*\)|drop table if exists brfss_\3;\ncreate external table brfss_\3 like parquet '\1' stored as parquet location '\2';|p"