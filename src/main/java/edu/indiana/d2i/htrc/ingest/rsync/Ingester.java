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
# File:  Ingester.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.rsync;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.Constants.CopyrightEnum;
import edu.indiana.d2i.htrc.ingest.IngestException;
import edu.indiana.d2i.htrc.ingest.JobQueue;
import edu.indiana.d2i.htrc.ingest.PropertyReader;
import edu.indiana.d2i.htrc.ingest.Util;

/**
 * @author Yiming Sun
 *
 */
public class Ingester {
    private static Logger log = Logger.getLogger(Ingester.class);
    static final String DELTA_LOG_FILENAME_PATTERN = "yyyyMMdd-HHmmssSSS";

    private String username;
    private String password;
    private String separator;
    private String sourceHost;
    private String deltaLogRoot;
    private String deltaLogDir;
    
    private String treeSourcePath;
    private String destRoot;
    
    private String sourceCleanseTarget;
    private String sourceCleanseReplacement;
    
    private boolean dryRun;

    private int sourceCount;
    private int threadCount;
    
    private BigInteger hashSpace = null;
    private int myHashKey = 0;
    
    private List<String> sourcePaths;
    private List<String> destPaths;
    
    private List<String> extraFilePaths;
   
    private JobQueue<RsyncJobDescriptor> jobQueue;
    
    public Ingester() {
        jobQueue = new JobQueue<RsyncJobDescriptor>("RsyncJobQueue");
        
        readConfiguration();
    }
    
    private void readConfiguration() {
        sourcePaths = new ArrayList<String>();
        destPaths = new ArrayList<String>();
        extraFilePaths = new ArrayList<String>();
        
        PropertyReader reader = PropertyReader.getInstance();

        username = reader.getProperty(Constants.PK_RSYNC_USERNAME);
        password = reader.getProperty(Constants.PK_RSYNC_PASSWORD);
        sourceHost = reader.getProperty(Constants.PK_RSYNC_SOURCE_HOST);
        separator = reader.getProperty(Constants.PK_RSYNC_SEPARATOR_TOKEN);

        if (reader.getProperty(Constants.PK_RSYNC_DRY_RUN) != null) {
            dryRun = true;
        } else {
            dryRun = false;
        }
        
        deltaLogRoot = reader.getProperty(Constants.PK_DELTA_LOG_ROOT);
        
        sourceCleanseTarget = reader.getProperty(Constants.PK_SOURCE_CLEANSE_TARGET);
        sourceCleanseReplacement = reader.getProperty(Constants.PK_SOURCE_CLEANSE_REPLACEMENT);
        
        if (sourceCleanseTarget == null) {
            sourceCleanseTarget = "";
        } else {
            sourceCleanseTarget = sourceCleanseTarget.trim();
        }
        
        if (sourceCleanseReplacement == null) {
            sourceCleanseReplacement = "";
        } else {
            sourceCleanseReplacement = sourceCleanseReplacement.trim();
        }
        
        treeSourcePath = Util.cleanseSource(reader.getProperty(Constants.PK_RSYNC_TREE_SOURCE_PATH), sourceCleanseTarget, sourceCleanseReplacement);
        
        destRoot = Util.addTrailingSlash(reader.getProperty(Constants.PK_RSYNC_DEST_ROOT));

        threadCount = Integer.parseInt(reader.getProperty(Constants.PK_RSYNC_THREAD_COUNT));
        
        int extraFilesCount = Integer.parseInt(reader.getProperty(Constants.PK_RSYNC_EXTRA_FILES_COUNT));
        
        for (int i = 0; i < extraFilesCount; i++) {
            String extraFilePath = Util.cleanseSource(reader.getProperty(Constants.PKN_RSYNC_EXTRA_FILE_PATH + (i + 1)), sourceCleanseTarget, sourceCleanseReplacement);
            extraFilePaths.add(extraFilePath);
        }
        
        
        int hashSpaceInt = Integer.parseInt(reader.getProperty(Constants.PK_INGEST_HASH_SPACE));
        hashSpaceInt = hashSpaceInt > 0 ? hashSpaceInt : 1;

        hashSpace = BigInteger.valueOf(hashSpaceInt);

        myHashKey = Integer.parseInt(reader.getProperty(Constants.PK_INGEST_MY_HASH_KEY));
        myHashKey = (myHashKey >= 0 && myHashKey < hashSpaceInt) ? myHashKey : 0;
        
        

//        sourceCount = Integer.parseInt(reader.getProperty(Constants.PK_RSYNC_SOURCE_COUNT));

//        for (int i = 0; i < sourceCount; i++) {
//            String sourceRoot = reader.getProperty(Constants.PKN_RSYNC_SOURCE_ROOT + (i + 1));
//            sourceRoot = Util.addTrailingSlash(sourceRoot);
//            sourceRoots.add(sourceRoot);
//            
//            String destRoot = reader.getProperty(Constants.PKN_RSYNC_DEST_ROOT + (i + 1));
//            destRoot = Util.addTrailingSlash(destRoot);
//            destRoots.add(destRoot);
//            
//        }
    }
    
    private void prepareDestination() throws IngestException {
        for (String destRoot : destPaths) {
            File file = new File(destRoot);
            if (!file.exists()) {
                boolean success = file.mkdirs();
                if (!success) {
                    log.error("Unable to create destination directory " + destRoot);
                    throw new IngestException("Unable to create destination directory " + destRoot);
                }
            }
        }
    }

    private void prepareDeltaLogDir() throws IngestException {
        DateFormat dateFormat = new SimpleDateFormat(DELTA_LOG_FILENAME_PATTERN);
        Calendar calendar = Calendar.getInstance();
        String dateString = dateFormat.format(calendar.getTime());
        deltaLogDir = deltaLogRoot + (deltaLogRoot.endsWith(File.separator) ? "" : File.separator) + "dlog-" + dateString;
        log.debug("deltaLogDir = " + deltaLogDir);
        File deltaLogDirFile = new File(deltaLogDir);
        boolean success = deltaLogDirFile.mkdirs();
        if (!success) {
            log.error("Unable to create delta log directory " + deltaLogDir);
            throw new IngestException("Unable to create delta log directory " + deltaLogDir);
        }
    }
    
    private void queueExtraFileJobs() {
        log.info("queuing extra file jobs");
        for (String extraFilePath : extraFilePaths) {
            RsyncJobDescriptor rsyncExtraFileJob = new RsyncJobDescriptor(extraFilePath, destRoot, "", CopyrightEnum.PUBLIC_DOMAIN, username, password, sourceHost, separator, deltaLogDir, dryRun);
            jobQueue.enqueue(rsyncExtraFileJob);
        }
    }

    private void queueJobs() {
        
        queueExtraFileJobs();
        
        int size = sourcePaths.size();
        
        for (int i = 0; i < size; i++) {
            String sourcePath = sourcePaths.get(i);
            String destPath = destPaths.get(i);
            String prefix = Util.extractPrefix(sourcePath);
            
            if (!prefix.equals("")) {
                prefix += ".";
            }
            
            RsyncJobDescriptor job = new RsyncJobDescriptor(sourcePath, destPath, prefix, CopyrightEnum.PUBLIC_DOMAIN, username, password, sourceHost, separator, deltaLogDir, dryRun);
            jobQueue.enqueue(job);
        }
        
        jobQueue.markDone();
    }
    
    private List<Thread> launchRsyncThreads() {
        List<Thread> threadList = new ArrayList<Thread>();
        for (int i = 0; i < threadCount; i++) {
            Rsyncker rsyncker = new Rsyncker("t-" + i, jobQueue);
            Thread thread = new Thread(rsyncker);
            
            threadList.add(thread);
            thread.start();
        }
        return threadList;
        
    }
    
    private void joinRsyncThreads(List<Thread> threadList) {
        for (int i = 0; i < threadCount; i++) {
            try {
                Thread thread = threadList.get(i);
                thread.join();
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for thread " + i, e);
            }
        }
        for (int i = 0; i < threadCount; i++) {
            threadList.remove(0);
        }
        
    }
    
    private void retrieveTreeList() throws InterruptedException {
        log.info("Retrieving remote tree list");
        RsyncJobDescriptor rsyncTreeJob = new RsyncJobDescriptor(treeSourcePath, destRoot, "", CopyrightEnum.PUBLIC_DOMAIN, username, password, sourceHost, separator, deltaLogDir, dryRun);
        jobQueue.enqueue(rsyncTreeJob);
        Rsyncker rsyncker = new Rsyncker("x-0", jobQueue);
        Thread thread = new Thread(rsyncker);
        thread.start();
        jobQueue.markDone();
        thread.join();
        jobQueue.reset();
    }
    
    
    private void readTreeList() throws FileNotFoundException, IOException  {
        log.info("Reading tree list");
        int index = 0;
        if (sourceCleanseReplacement != null) {
            index = sourceCleanseReplacement.length();
        }
        String treePath = destRoot + treeSourcePath.substring(index);
            
        if (log.isTraceEnabled()) log.trace("local treePath is: " + treePath);
//        String treePath = destRoot + treeSourcePath;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(treePath));
        String line = null;
        sourceCount = 0;
        do {
            line = bufferedReader.readLine();
            if (log.isTraceEnabled()) log.trace("line read: " + line);
            if (line != null) {
                int hash = Util.hashMD5Checksum(line, hashSpace);
                if (hash == myHashKey) {
                    if (log.isInfoEnabled()) log.info("Accepted path: " + line + " hash: " + hash);
                    String cleanedSource = Util.addTrailingSlash(Util.cleanseSource(line, sourceCleanseTarget, sourceCleanseReplacement));
                    sourcePaths.add(cleanedSource);
                
                    destPaths.add(destRoot + cleanedSource.substring(index));
                    sourceCount++;
                } else {
                    if (log.isTraceEnabled()) log.trace("Not my responsibility hash: " + hash + " path: " + line);
                }
            }
        } while (line != null);
        
        bufferedReader.close();
        log.info("Branches read: " + sourceCount);
        
    }
    
    private void runJobs() {
        log.info("** Thread count: " + threadCount);
        log.info("** Source count: " + sourceCount);
        long startTime = System.currentTimeMillis();
        log.info("time start: " + startTime);

        List<Thread> rsyncThreadList = launchRsyncThreads();
        queueJobs();
        joinRsyncThreads(rsyncThreadList);
        
        long endTime = System.currentTimeMillis();
        log.info("time end: " + endTime);
        log.info("threadCount: " + threadCount + " duration: " + (endTime - startTime));
        log.info("Ingest finished");

        rsyncThreadList = null;

    }
    


    public void ingest() throws IngestException {
        
        prepareDeltaLogDir();
        
        try {
            retrieveTreeList();
            readTreeList();
            prepareDestination();
            runJobs();
        } catch (InterruptedException e) {
            throw new IngestException(e);
        } catch (FileNotFoundException e) {
            throw new IngestException(e);
        } catch (IOException e) {
            throw new IngestException(e);
        }
        
    }
}

