#!/bin/sh

. ./setpropfile.sh

java -cp "build/classes:." -DPROPERTIES_LOCATION=$PROPERTIES_PATH edu.indiana.d2i.htrc.ingest.tools.ExtractVIDFromErrorLogs $*

