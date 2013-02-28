#!/bin/sh

# PROPERTIES_PATH=/home/hathitrust/htrc-ingest/HTRC-Ingester/conf/htrc-ingest.properties

. ./setpropfile.sh

java -cp "build/classes:." -DPROPERTIES_LOCATION=$PROPERTIES_PATH edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs $*
