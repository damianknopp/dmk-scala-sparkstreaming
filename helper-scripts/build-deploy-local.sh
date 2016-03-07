#!/bin/bash

SPARK_HOME=$HOME/spark/
CODE=$HOME/workspace-e4.4/dmk-scala-sparkstreaming

DATA=tmp-data-split
mkdir -p $DATA

pushd $CODE
mvn clean package
cp target/*.jar $SPARK_HOME
cp helper-scripts/*.sh $SPARK_HOME
cp data/*.csv $SPARK_HOME/$DATA
popd
pushd $DATA && split -l 100 baseball.csv && popd
