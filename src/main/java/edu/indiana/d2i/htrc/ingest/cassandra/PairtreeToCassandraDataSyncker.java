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
# File:  DeltaLogFileLister.java
# Description: This class drives the synchronization/ingest of corpus data from the Pairtree on filesystem to Cassandra 
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.cassandra;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.JobQueue;
import edu.indiana.d2i.htrc.ingest.PropertyReader;

/**
 * This class drives the synchronization/ingest of corpus data from the Pairtree on filesystem to Cassandra 
 * @author Yiming Sun
 *
 */
public class PairtreeToCassandraDataSyncker {
    
    /**
     * This class implements FilenameFilter and FileFilter interfaces to pick delta log files
     * @author Yiming Sun
     *
     */
    static final class DeltaLogDirectoryFilter implements FilenameFilter, FileFilter {

        private static final String regexp = "dlog-\\d{8}-\\d{9}";
        
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
    
    /**
     * This class implements FilenameFilter and FileFilter interfaces to pick directories containing delta log files
     * @author Yiming Sun
     *
     */
    static final class DeltaLogFileFilter implements FilenameFilter, FileFilter {
        private static final String regexp = "t-\\d+-\\d+\\.txt";

        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File file) {
            String filename = file.getName();
            return (file.isFile() && filename.matches(regexp));
        }

        /**
         * @see java.io.FilenameFilter#accept(java.io.File, java.lang.String)
         */
        @Override
        public boolean accept(File dir, String name) {
            return (name.matches(regexp));
        }
        
    }
    
    private static Logger log = Logger.getLogger(PairtreeToCassandraDataSyncker.class);
    
    protected JobQueue<String> jobQueue;
    
    protected int threadCount;
    
    protected String deltaLogRoot;
    
    
    protected File[] dlogDirList;
    
    /**
     * Constructor
     */
    public PairtreeToCassandraDataSyncker() {
        PropertyReader propertyReader = PropertyReader.getInstance();
        
        deltaLogRoot = propertyReader.getProperty(Constants.PK_DELTA_LOG_ROOT);

        threadCount = Integer.parseInt(propertyReader.getProperty(Constants.PK_DELTA_PROCESSOR_THREAD_COUNT));

        jobQueue = new JobQueue<String>("PT2NoSQLJobQueue");

        
    }
    
    /**
     * Method to list delta log files
     */
    protected void listFiles() {
        
        File deltaLogRootDir = new File(deltaLogRoot);
        FileFilter dlogDirFileFilter = new DeltaLogDirectoryFilter();
        
        FileFilter dlogFilenameFilter = new DeltaLogFileFilter();
        
        dlogDirList = deltaLogRootDir.listFiles(dlogDirFileFilter);
        
        for (File file : dlogDirList) {
            File[] listFiles = file.listFiles(dlogFilenameFilter);
            for (File dlogFile : listFiles) {
                String dlogFilePath = dlogFile.getPath();
                if (log.isTraceEnabled()) log.trace("enqueue " + dlogFilePath);
                jobQueue.enqueue(dlogFilePath);
            }
        }
        
        jobQueue.markDone();
    }

    /**
     * Method to launch threads that process delta log files
     * @return a List of Thread objects
     */
    protected List<Thread> launchProcessorThreads() {

        List<Thread> threadList = new ArrayList<Thread>();
        
        for (int i = 0; i < threadCount; i++) {
            DeltaLogProcessor processor = new DeltaLogProcessor(i, jobQueue);
            Thread thread = new Thread(processor);
            if (log.isTraceEnabled()) log.trace("DeltaLogProcessor thread " + thread.getName() + " launched");
            threadList.add(thread);
            thread.start();
        }
        
        return threadList;
    }
    
    /**
     * Method to wait and join the threads
     * @param threadList a List of Thread objects
     */
    protected void joinProcessorThreads(List<Thread> threadList){
        for (int i = 0; i < threadCount; i++) {
            try {
                Thread thread = threadList.remove(0);
                thread.join();
                log.info("DeltaLogProcessor " + i + " finished");
            } catch (InterruptedException e) {
                log.error("Interrupted while joining thread " + i, e);
            }
        }
        
    }
    /**
     * Method to run jobs
     */
    protected void runJob() {
        List<Thread> threadList = launchProcessorThreads();
        listFiles();
        joinProcessorThreads(threadList);
    }
    
    /**
     * Method to change the delta log directories names to signify they have been processed
     */
    protected void markDlogDirsAsDone() {
        for (File dir : dlogDirList) {
            String dirName = dir.getName();
            File parentFile = dir.getParentFile();
            
            File newDirName = new File(parentFile, "_PROCESSED_" + dirName);
            
            boolean renameTo = dir.renameTo(newDirName);
            
            if (renameTo) {
                log.info("deltaLog directory " + dir.getPath() + " renamed to " + newDirName.getPath());
            } else {
                log.warn("Failed to rename deltaLog directory " + dir.getPath() + " to " + newDirName.getPath());
            }
        }
    }
    
    /**
     * Method to initiate the process
     */
    public void process() {
        runJob();
        markDlogDirsAsDone();
    }

}

