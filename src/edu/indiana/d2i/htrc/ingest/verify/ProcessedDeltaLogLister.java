/*
#
# Copyright 2012 The Trustees of Indiana University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: HTRC-Ingester
# File:  ProcessedDeltaLogLister.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.verify;

import java.io.File;
import java.io.FileFilter;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.JobQueue;
import edu.indiana.d2i.htrc.ingest.PropertyReader;

/**
 * @author Yiming Sun
 *
 */
public class ProcessedDeltaLogLister implements Runnable {

    static class DeltaLogDirFilter implements FileFilter {
        public static final String PROCESSED_DLOG_PATTERN = "_PROCESSED_dlog-\\d{8}-\\d{9}";
        public static final String UNPROCESSED_DLOG_PATTERN = "dlog-\\d{8}-\\d{9}";
        
        @Override
        public boolean accept(File file) {
            String filename = file.getName();
            return (filename.matches(PROCESSED_DLOG_PATTERN) || filename.matches(UNPROCESSED_DLOG_PATTERN));
        }
    }
    
    static class ParsedTFileFilter implements FileFilter {
        public static final String PARSED_T_PATTERN = "_PARSED_t-\\d+-\\d+.txt";
        
        @Override
        public boolean accept(File file) {
            return file.getName().matches(PARSED_T_PATTERN);
        }
        
    }

    
    protected final JobQueue<File> processedDeltaLogQueue;
    
    ProcessedDeltaLogLister(JobQueue<File> processedDeltaLogQueue) {
        
        this.processedDeltaLogQueue = processedDeltaLogQueue;
    }
    
    /**
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        PropertyReader reader = PropertyReader.getInstance();
        String deltaLogRootPath = reader.getProperty(Constants.PK_DELTA_LOG_ROOT);
        
        File deltaLogRootDir = new File(deltaLogRootPath);
        
        File[] listFiles = deltaLogRootDir.listFiles(new DeltaLogDirFilter());
        for (File deltaLogDir : listFiles) {
            File[] parsedFiles = deltaLogDir.listFiles(new ParsedTFileFilter());
            for (File parsedFile : parsedFiles) {
                processedDeltaLogQueue.enqueue(parsedFile);
            }
        }

        processedDeltaLogQueue.markDone();
    }

}

