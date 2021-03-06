#!/usr/bin/env bash

# copy this file to the root spark home directory, also copy our jar to root spark home
export SPARK_HOME="$(dirname "$0")"
. "${SPARK_HOME}"/bin/load-spark-env.sh

JAR=$(ls ./dmk-scala-sparkstreaming*.jar)
#JAR=$(ls ./spark-streaming-flume*.jar)",$JAR"
SPARK_CP="./spark-streaming-flume_2.10-1.6.0.jar,$JAR"
for lib in $(ls ./lib); do 
	SPARK_CP="./lib/$lib,$SPARK_CP"
done;

${SPARK_HOME}/bin/spark-submit --master local[*] --class dmk.spark.streaming.flume.FlumeStream --jars $SPARK_CP $JAR $@