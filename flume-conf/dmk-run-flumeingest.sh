#!/bin/bash

function list_files {
	echo "$@"
	#local files=$(ls .)
	IFS=' '
	local files=($@)
	echo $files
	local len=${#files[@]}
	echo $len
	for ((i = 0; i < len; i+=1))
	do
		local cur_file=${files[$i]}
		if [ $((i%2)) -eq 0 ]; then 
			echo "$i $cur_file"
			bin/flume-ng avro-client --conf conf -H localhost -p 41414 -F $cur_file -Dflume.root.logger=DEBUG,console
		else
			echo "$i $cur_file"
			bin/flume-ng avro-client --conf conf -H localhost -p 41415 -F $cur_file -Dflume.root.logger=DEBUG,console
		fi
		sleep 15
	done
}

list_files "$@"
