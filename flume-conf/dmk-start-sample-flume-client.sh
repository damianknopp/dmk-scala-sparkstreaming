#!/bin/bash

#pass in sample file, one entry per line
bin/flume-ng avro-client --conf conf -H localhost -p 41414 -F $1 -Dflume.root.logger=DEBUG,console
#bin/flume-ng avro-client --conf conf -H localhost -p 9905 -F $1 -Dflume.root.logger=DEBUG,console
