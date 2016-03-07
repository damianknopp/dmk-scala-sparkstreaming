#!/usr/bin/env bash

export SPARK_HOME="$(dirname "$0")"
echo $SPARK_HOME
. "${SPARK_HOME}"/bin/load-spark-env.sh

SPARK_CP="$1"
for lib in $(ls ./lib); do 
	SPARK_CP="./lib/$lib,$SPARK_CP"
done;

echo $SPARK_CP

${SPARK_HOME}/bin/spark-submit --master local[*] --class $2 --jars $SPARK_CP $1 $3 $4
