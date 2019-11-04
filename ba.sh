#!/bin/bash

if [ $# -lt 1 ]; then
	echo "parameter should be 1, respersent test name"
exit 1
fi

for i in {1..1000}; 
do 
	w=`RUST_BACKTRACE=1 cargo test $1 -p tikv 2>&1`
	result=$(echo "$w" | grep "test result: FAILED")
	if [ -n "$result" ] 
	then 
		echo "$w" 
		exit
	else
		echo "ok"
	fi 
done
