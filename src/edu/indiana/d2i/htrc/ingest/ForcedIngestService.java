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
# File:  ForcedIngestService.java
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

import edu.indiana.d2i.htrc.ingest.cassandra.ForcedDataSyncker;

/**
 * @author Yiming Sun
 *
 */
public class ForcedIngestService {
    private static Logger log = Logger.getLogger(ForcedIngestService.class);
    
    public static void main(String[] args) {
        PropertyReader propertyReader = PropertyReader.getInstance();
        String log4jPropertiesPath = propertyReader.getProperty(Constants.PK_LOG4J_PROPERTIES_PATH);
        PropertyConfigurator.configure(log4jPropertiesPath);
        
        log.info("Forced Ingest Service Started");
        
        ForcedDataSyncker forcedDataSyncker = new ForcedDataSyncker();
        forcedDataSyncker.process();
    }

}

