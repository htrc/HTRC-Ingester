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
# File:  VerificationIDGenerator.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.verify;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.exceptions.HTimedOutException;

import org.apache.log4j.Logger;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.JobQueue;
import edu.indiana.d2i.htrc.ingest.PropertyReader;
import gov.loc.repository.pairtree.Pairtree;

/**
 * @author Yiming Sun
 *
 */
@Deprecated
public class VerificationSpaceIDGenerator {
    
    static enum VerificationSpaceEnum {
        NEW,
        ALL;
    }
    
    static interface IDGenerator {
        public void generate(JobQueue<String> jobQueue);
    }
    
    static class IDFromCassandraGenerator implements IDGenerator {

        /**
         * @see edu.indiana.d2i.htrc.ingest.verify.VerificationSpaceIDGenerator.IDGenerator#generate(edu.indiana.d2i.htrc.ingest.JobQueue)
         */
        @Override
        public void generate(JobQueue<String> jobQueue) {
            HectorAccessor accessor = HectorAccessor.getInstance();
            try {
                List<String> idList = accessor.getAllVolumeIDs();
                for (String id : idList) {
                    jobQueue.enqueue(id);
                }
            } catch (HTimedOutException e) {
                log.error(e.toString(), e);
            }
            jobQueue.markDone();
        }
        
    }
    
    static class IDFromDeltaLogGenerator implements IDGenerator {

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
        
        static class DeltaLogVolumeIDExtractor implements Runnable {
            private static Logger log = Logger.getLogger(DeltaLogVolumeIDExtractor.class);
            private JobQueue<File> deltaLogQueue;
            private JobQueue<String> volumeIDQueue;
            private Pairtree pairtree;
            
            DeltaLogVolumeIDExtractor(JobQueue<File> deltaLogQueue, JobQueue<String> volumeIDQueue) {
                this.deltaLogQueue = deltaLogQueue;
                this.volumeIDQueue = volumeIDQueue;
                pairtree = new Pairtree();
            }
            
            public void run() {
                while (!deltaLogQueue.isDone()) {
                    File file = deltaLogQueue.dequeue();
                    if (log.isTraceEnabled()) log.trace("extracting volumeID from " + file.getPath());
                    BufferedReader bufferedReader = null;
                    String prefix = null;
                    try {
                        bufferedReader = new BufferedReader(new FileReader(file));
                        String line = null;
                        do {
                            line = bufferedReader.readLine();
                            if (line != null) {
                                if (line.endsWith(".zip")) {
                                    int index = line.lastIndexOf('/');
                                    String zipFilename = line.substring(index + 1);
                                    String cleanedPartialVolumeID = zipFilename.substring(0, zipFilename.length() - ".zip".length());
                                    String uncleanedPartialVolumeID = pairtree.uncleanId(cleanedPartialVolumeID);
                                    String volumeID = prefix + uncleanedPartialVolumeID;
                                    if (log.isTraceEnabled()) log.trace("extracted volumeID: " + volumeID);
                                    volumeIDQueue.enqueue(volumeID);
                                } else if (line.startsWith("PREFIX=")) {
                                    prefix = line.substring("PREFIX=".length());
                                }
                            }
                        } while (line != null);
                    } catch (FileNotFoundException e) {
                        log.error("Delta log file not found " + file.getPath(), e);
                    } catch (IOException e) {
                        log.error("Error reading delta log file " + file.getPath(), e);
                    } finally {
                        if (bufferedReader != null) {
                            try {
                                bufferedReader.close();
                            } catch (IOException e) {
                                log.error("Failed to close BufferedReader on file " + file.getPath(), e);
                            }
                        }
                    }
                    
                }
            }
        }
        
        private static Logger log = Logger.getLogger(IDFromDeltaLogGenerator.class);
        /**
         * @see edu.indiana.d2i.htrc.ingest.verify.VerificationSpaceIDGenerator.IDGenerator#generate(edu.indiana.d2i.htrc.ingest.JobQueue)
         */
        @Override
        public void generate(JobQueue<String> jobQueue) {
            PropertyReader reader = PropertyReader.getInstance();
            String deltaLogRootPath = reader.getProperty(Constants.PK_DELTA_LOG_ROOT);
            
            int extracterCount = Integer.parseInt(reader.getProperty(Constants.PK_DELTALOG_VOLUMEID_EXTRACTOR_THREAD_COUNT));
            
            JobQueue<File> deltaLogQueue = new JobQueue<File>("deltaLogQueue");
            List<Thread> threads = launchExtractorThreads(extracterCount, deltaLogQueue, jobQueue);

            File deltaLogRootDir = new File(deltaLogRootPath);
            File[] listFiles = deltaLogRootDir.listFiles(new DeltaLogDirFilter());
            for (File file : listFiles) {
                File[] parsedTFiles = file.listFiles(new ParsedTFileFilter());
                for (File parsedTFile : parsedTFiles) {
                    deltaLogQueue.enqueue(parsedTFile);
                }
            }
            deltaLogQueue.markDone();
            
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    log.warn("Joining thread interrupted", e);
                }
            }
            
            
        }
        
        
        private List<Thread> launchExtractorThreads(int threadCount, JobQueue<File> deltaLogQueue, JobQueue<String> volumeIDQueue) {
            List<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < threadCount; i++) {
                DeltaLogVolumeIDExtractor extractor = new DeltaLogVolumeIDExtractor(deltaLogQueue, volumeIDQueue);
                Thread thread = new Thread(extractor);
                threads.add(thread);
                thread.start();

            }
            return threads;
        }
        
    }
    
    private static Logger log = Logger.getLogger(VerificationSpaceIDGenerator.class);
    private JobQueue<String> jobQueue;
    private VerificationSpaceEnum verificationSpace;
    
    public VerificationSpaceIDGenerator(JobQueue<String> jobQueue, VerificationSpaceEnum verificationSpace) {
        this.jobQueue = jobQueue;
        this.verificationSpace = verificationSpace;
    }
    
    public void generateIDs() {
        IDGenerator idGenerator = null;
        switch (verificationSpace) {
        case NEW:
            idGenerator = new IDFromDeltaLogGenerator();
            if (log.isTraceEnabled()) log.trace("verificationSpace is NEW");
            break;
        case ALL:
            idGenerator = new IDFromCassandraGenerator();
            if (log.isTraceEnabled()) log.trace("verificationSpace is ALL");
            break;
        }
        
        idGenerator.generate(jobQueue);
        jobQueue.markDone();
    }

}

