#!/bin/sh

. ./setpropfile.sh
LOCALCLASSPATH=`/bin/sh $PWD/classpath.sh run`

java -cp "build/classes:." -DPROPERTIES_LOCATION=$PROPERTIES_PATH edu.indiana.d2i.htrc.ingest.tools.Reingester $*
