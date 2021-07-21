#!/usr/bin/env bash
set -x

HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar

hdfs dfs -rm -r -skipTrash $2

(yarn jar $HADOOP_STREAMING_JAR \
	-D stream.num.map.output.key.fields=2 \
	-D stream.num.reduce.output.key.fields=2 \
	-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
	-D mapreduce.partition.keycomparator.options="-k1,1n -k2,2" \
	-files mapper_job_1.py,reducer_job_1.py \
	-mapper mapper_job_1.py \
	-reducer reducer_job_1.py \
	-combiner reducer_job_1.py \
	-numReduceTasks 2 \
	-input $1 \
	-output $2_tmp


yarn jar $HADOOP_STREAMING_JAR \
	-D stream.num.map.output.key.fields=2 \
        -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
        -D mapreduce.partition.keycomparator.options="-k1,1n -k2,2nr" \
	-files mapper_job_2.py,reducer_job_2.py \
	-mapper mapper_job_2.py \
	-reducer reducer_job_2.py \
	-numReduceTasks 1 \
	-input $2_tmp \
	-output $2

hdfs dfs -cat $2/* | head -n 20

) || echo "Error happens"

hdfs dfs -rm -r -skipTrash $2_tmp
