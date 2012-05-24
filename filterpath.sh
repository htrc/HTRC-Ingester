#!/bin/sh
echo "inputFile=$1"
echo "outputFile=$2"
echo "depth=$3"
echo "delimiter=$4"

java -cp "build/classes:." edu.indiana.d2i.htrc.ingest.tools.PathDepthFilter $1 $2 $3 $4 $5

