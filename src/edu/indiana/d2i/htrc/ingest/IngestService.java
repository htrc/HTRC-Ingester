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
# File:  IngestService.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.indiana.d2i.htrc.ingest.cassandra.HectorManager;
import edu.indiana.d2i.htrc.ingest.cassandra.PairtreeToCassandraDataSyncker;
import edu.indiana.d2i.htrc.ingest.rsync.Ingester;
import edu.indiana.d2i.htrc.ingest.verify.HectorAccessor;
import edu.indiana.d2i.htrc.ingest.verify.Verificationer;

/**
 * @author Yiming Sun
 *
 */
public class IngestService {
    private static Logger log = Logger.getLogger(IngestService.class);
    
    public static void main(String[] args) {
        PropertyReader propertyReader = PropertyReader.getInstance();
        String log4PropertiesPath = propertyReader.getProperty(Constants.PK_LOG4J_PROPERTIES_PATH);
        PropertyConfigurator.configure(log4PropertiesPath);
        
//        String skipRsync = propertyReader.getProperty(Constants.PK_SKIP_RSYNC);
        
        boolean performRsync = Util.getBooleanProperty(propertyReader.getProperty(Constants.PK_PERFORM_RSYNC));
        boolean performNosqlPush = Util.getBooleanProperty(propertyReader.getProperty(Constants.PK_PERFORM_NOSQL_PUSH));
        boolean performVerification = Util.getBooleanProperty(propertyReader.getProperty(Constants.PK_PERFORM_VERIFICATION));
        
        try {
            
            log.info("Ingest Service Started");
            
            if (performRsync) {
        
                log.info("Rsyncking data from remote source to local pairtree...");
                Ingester ingester = new Ingester();
                ingester.ingest();
            
                log.info("Done rsyncking with remote source");
            } else {
                log.info ("Skip rysnc");
            }
            
            if (performNosqlPush) {
            
                log.info("Syncking data from local pairtree to cassandra...");
                
                PairtreeToCassandraDataSyncker dataSyncker = new PairtreeToCassandraDataSyncker();
                dataSyncker.process();
                
                log.info("Done syncking data from local pairtree");
            } else {
                log.info ("Skip noSQL push");
            }
            
            if (performVerification) {
                log.info("Verifying data...");
                Verificationer verificationer = new Verificationer();
                verificationer.verify();
                
                log.info("Done data verification");
            } else {
                log.info("Skip data verification");
            }
        } catch (IngestException e) {
            log.error("Ingest service terminated abnormally due to exception", e);
        } finally {
            log.info("Shutting down hector manager singleton");
            HectorManager.getInstance().shutdown();
            log.info("Shutting down hector accessor singleton");
            HectorAccessor.getInstance().shutdown();
        }
    }
}

