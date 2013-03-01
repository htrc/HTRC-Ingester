#!/bin/sh

. ./setpropfile.sh
LOCALCLASSPATH=`/bin/sh $PWD/classpath.sh run`

java -Xms768M -Xmx1024M -cp $LOCALCLASSPATH -DPROPERTIES_LOCATION=$PROPERTIES_PATH edu.indiana.d2i.htrc.ingest.IngestService
