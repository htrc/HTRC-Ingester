#!/bin/sh
echo "sourceFile=$1"
echo "destFile=$2"
echo "sourceRoot=$3"
echo "destRoot=$4"

if [ -z $5 ]
then
echo "no sourceAlias"
java -cp "classes:." edu.indiana.d2i.htrc.ingest.tools.SourceDestGen $1 $2 $3 $4
else
echo "sourceAlias=$5"
java -cp "classes:." edu.indiana.d2i.htrc.ingest.tools.SourceDestGen $1 $2 $3 $4 $5
fi

