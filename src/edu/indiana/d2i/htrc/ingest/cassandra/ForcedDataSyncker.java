/*
#
# Copyright 2007 The Trustees of Indiana University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or areed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: HTRC-Ingester
# File:  ForcedDataSyncker.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.cassandra;

import java.io.File;

import org.apache.log4j.Logger;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.PropertyReader;

/**
 * @author Yiming Sun
 *
 */
public class ForcedDataSyncker extends PairtreeToCassandraDataSyncker {
    private static Logger log = Logger.getLogger(ForcedDataSyncker.class);
    
    protected String forcedIngestListDir;
    
    public ForcedDataSyncker() {
        super();
        PropertyReader propertyReader = PropertyReader.getInstance();
        
        forcedIngestListDir = propertyReader.getProperty(Constants.PK_FORCE_INGEST_LIST_DIR);
        
    }
    
    @Override
    protected void listFiles() {
        File ingestListDir = new File(forcedIngestListDir);
        
        
        String[] ingestListFiles = ingestListDir.list(new DeltaLogFileFilter());
        
        for (String ingestListFile : ingestListFiles) {
            jobQueue.enqueue(ingestListFile);
        }
        jobQueue.markDone();
    }
    
    @Override
    public void process() {
        runJob();
        log.info("Forced Data Syncker finished");
    }
    
}

