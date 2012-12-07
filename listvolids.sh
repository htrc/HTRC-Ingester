#!/bin/sh

. ./setpropfile.sh
LOCALCLASSPATH=`/bin/sh $PWD/classpath.sh run`


java -cp $LOCALCLASSPATH -DPROPERTIES_LOCATION=$PROPERTIES_PATH edu.indiana.d2i.htrc.ingest.tools.KeyLister $*

