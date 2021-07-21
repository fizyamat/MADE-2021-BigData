HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar

set -x

hdfs dfs -rm -r $2

yarn jar $HADOOP_STREAMING_JAR \
        -file 'mapper.py' \
	-file 'reducer.py' \
	-reducer 'reducer.py' \
	-mapper 'mapper.py' \
	-numReduceTasks 2 \
	-input $1 \
	-output $2 \

hdfs dfs -cat $2/* | head -n 50
