#!/bin/bash

bin/flume-ng agent --conf ./conf/ -f conf/flume1.conf -Dflume.root.logger=DEBUG,console -n agent1 -Dflume.monitoring.type=http -Dflume.monitoring.port=9080
