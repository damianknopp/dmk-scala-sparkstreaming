#!/bin/bash

SPARK_HOME=$HOME/spark/
CODE=$HOME/workspace-e4.4/dmk-scala-sparkstreaming
pushd $CODE
mvn clean package
cp target/*.jar $SPARK_HOME
mkdir -p tmp-data-split
cp data/*.csv tmp-data-split
pushd tmp-data-split && split -l baseball.csv && popd
cp helper-scripts/*.sh $SPARK_HOME
popd
