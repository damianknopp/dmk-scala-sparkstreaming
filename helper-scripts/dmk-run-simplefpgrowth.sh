#!/usr/bin/env bash

# copy this file to the root spark home directory, also copy our jar to root spark home
export SPARK_HOME="$(dirname "$0")"
. "${SPARK_HOME}"/bin/load-spark-env.sh

JAR=$(ls ./dmk-scala-sparkstreaming*.jar)
SPARK_CP=$JAR",spark-mllib_2.10-1.6.0.jar"
for lib in $(ls ./lib); do 
	SPARK_CP="./lib/$lib,$SPARK_CP"
done;

${SPARK_HOME}/bin/spark-submit --master local[*] --class dmk.spark.mllib.SimpleFPGrowth --jars $SPARK_CP $JAR $@
