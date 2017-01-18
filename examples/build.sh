#!/bin/bash

if [ $# -ne 3 ]
then
	echo Usage: ./build.sh infile outfile compiler
	exit 1
fi

$3 $1 -o $2 -O3 -std=c++11 -pthread

exit

