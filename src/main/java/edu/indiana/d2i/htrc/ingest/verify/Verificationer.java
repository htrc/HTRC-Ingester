/*
#
# Copyright 2013 The Trustees of Indiana University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: HTRC-Ingester
# File:  Verificationer.java
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
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.JobQueue;
import edu.indiana.d2i.htrc.ingest.PropertyReader;
import edu.indiana.d2i.htrc.ingest.Util;
import edu.indiana.d2i.htrc.ingest.verify.Verifier.VerificationLevelEnum;

/**
 * This class initiates the verification process
 * @author Yiming Sun
 *
 */
public class Verificationer {

    /**
     * This class implements the FilenameFilter and FileFilter interfaces for picking up processed delta log directories
     * @author Yiming Sun
     *
     */
    static final class ProcessedDeltaLogDirectoryFilter implements FilenameFilter, FileFilter {

        private static final String regexp = "_PROCESSED_dlog-\\d{8}-\\d{9}";
        
        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File file) {
            String filename = file.getName();
            return (file.isDirectory() && filename.matches(regexp));
        }

        /**
         * @see java.io.FilenameFilter#accept(java.io.File, java.lang.String)
         */
        @Override
        public boolean accept(File dir, String name) {
            return (name.matches(regexp));
        }
        
    }

    private static Logger log = Logger.getLogger(Verificationer.class);
    
    /**
     * Method to carry out the verification process
     */
    public void verify() {

        JobQueue<File> processedDeltaLogQueue = new JobQueue<File>("ProcessedDeltaLogQueue");

        List<Thread> verifierThreadList = launchVerifierThreads(processedDeltaLogQueue);
        listProcessedDeltaLogs(processedDeltaLogQueue);
        
        
        
        for (Thread thread : verifierThreadList) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                log.warn("Joining thread interrupted", e);
            }
        }
        
        markProcessedDlogDirsAsDone();
    }
    
    /**
     * Method to list processed delta logs
     * @param processedDeltaLogQueue a JobQueue of File objects containing processed delta logs
     */
    protected void listProcessedDeltaLogs(JobQueue<File> processedDeltaLogQueue) {
        ProcessedDeltaLogLister lister = new ProcessedDeltaLogLister(processedDeltaLogQueue);
        Thread thread = new Thread(lister);
        thread.start();
        
    }
    
    /**
     * Method to launch verification threads
     * @param processedDeltaLogQueue a JobQueue of File objects containing processed delta logs
     * @return a List of Thread objects
     */
    protected List<Thread> launchVerifierThreads(JobQueue<File> processedDeltaLogQueue) {

        List<Thread> threads = new ArrayList<Thread>();

        PropertyReader reader = PropertyReader.getInstance();
        
        int threadCount = Integer.parseInt(reader.getProperty(Constants.PK_VERIFICATION_THREAD_COUNT));
        VerificationLevelEnum verificationLevel = VerificationLevelEnum.VERIFY_PAGE_COUNT;
        
        if (Util.getBooleanProperty(reader.getProperty(Constants.PK_VERIFICATION_LEVEL_PAGECHECKSUM))) {
            verificationLevel = VerificationLevelEnum.VERIFY_PAGE_CHECKSUM;
        } else if (Util.getBooleanProperty(reader.getProperty(Constants.PK_VERIFICATION_LEVEL_PAGESIZE))) {
            verificationLevel = VerificationLevelEnum.VERIFY_PAGE_SIZE;
        }
        
        
        for (int i = 0; i < threadCount; i++) {
            Verifier verifier = new Verifier(processedDeltaLogQueue, verificationLevel);
            Thread thread = new Thread(verifier);
            threads.add(thread);
            thread.start();
        }
        
        return threads;
        
    }
    /**
     * Method to mark processed delta log directories
     */
    protected void markProcessedDlogDirsAsDone() {
        PropertyReader propertyReader = PropertyReader.getInstance();
        
        String deltaLogRoot = propertyReader.getProperty(Constants.PK_DELTA_LOG_ROOT);
        File deltaLogRootDir = new File(deltaLogRoot);
        
        FileFilter processedDeltaLogDirectoryFilter = new ProcessedDeltaLogDirectoryFilter();
        

        File[] processedDlogDirList = deltaLogRootDir.listFiles(processedDeltaLogDirectoryFilter);

        for (File dir : processedDlogDirList) {
            String dirName = dir.getName();
            File parentFile = dir.getParentFile();
            
            File newDirName = new File(parentFile, "_VERIFIED_" + dirName.substring("_PROCESSED_".length()));
            
            boolean renameTo = dir.renameTo(newDirName);
            
            if (renameTo) {
                log.info("deltaLog directory " + dir.getPath() + " renamed to " + newDirName.getPath());
            } else {
                log.warn("Failed to rename deltaLog directory " + dir.getPath() + " to " + newDirName.getPath());
            }
        }
    }

} 

