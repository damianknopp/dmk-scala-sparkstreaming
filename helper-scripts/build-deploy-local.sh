#!/bin/bash

SPARK_HOME=$HOME/spark/
CODE=$HOME/workspace-e4.4/dmk-scala-sparkstreaming
pushd $CODE
mvn clean package
cp target/*.jar $SPARK_HOME
cp helper-scripts/*.sh $SPARK_HOME
popd
