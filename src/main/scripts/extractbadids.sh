#!/bin/sh

. ./setpropfile.sh

java -cp "classes:." -DPROPERTIES_LOCATION=$PROPERTIES_PATH edu.indiana.d2i.htrc.ingest.tools.ExtractVIDFromErrorLogs $*

