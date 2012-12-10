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
# Unless required by applicable law or areed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: HTRC-Ingester
# File:  Verifier.java
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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

import me.prettyprint.hector.api.exceptions.HTimedOutException;

import org.apache.log4j.Logger;

import edu.indiana.d2i.htrc.ingest.JobQueue;
import edu.indiana.d2i.htrc.ingest.Util;
import edu.indiana.d2i.htrc.ingest.verify.HectorAccessor.PageChecksumMetadata;
import edu.indiana.d2i.htrc.ingest.verify.HectorAccessor.PageSizeMetadata;
import gov.loc.repository.pairtree.Pairtree;

/**
 * @author Yiming Sun
 *
 */
public class Verifier implements Runnable {
    public static enum VerificationLevelEnum {
        VERIFY_PAGE_COUNT(0),
        VERIFY_PAGE_SIZE(1),
        VERIFY_PAGE_CHECKSUM(2);
        
        private int level;
        
        VerificationLevelEnum(int level) {
            this.level = level;
        }
        
        private int getLevel() {
            return this.level;
        }
        
        public boolean include(VerificationLevelEnum another) {
            return (this.getLevel() >= another.getLevel());
        }
    }
    
    private static Logger log = Logger.getLogger(Verifier.class);
    
    protected VerificationLevelEnum verificationLevel;
    protected HectorAccessor hectorAccessor;
    protected JobQueue<File> jobQueue;
    protected Pairtree pairtree;

    protected Verifier(JobQueue<File> jobQueue, VerificationLevelEnum verificationLevel) {
        this.jobQueue = jobQueue;
        this.verificationLevel = verificationLevel;
        this.hectorAccessor = HectorAccessor.getInstance();
        this.pairtree = new Pairtree();
        
    }
    
    public void run() {
        while (!jobQueue.isDone()) {
            File parsedDeltaLog = jobQueue.dequeue();
            if (log.isTraceEnabled()) log.trace("parsedDeltaLog dequeued: " + parsedDeltaLog.getPath());
            List<String> extractedVolumeIDs = extractVolumeIDs(parsedDeltaLog);
            for (String volumeID : extractedVolumeIDs) {
                verify(volumeID);
            }
            renameParsedFileToExtracted(parsedDeltaLog);
            
        }
    }
    
    protected List<String> extractVolumeIDs(File parsedDeltaLog) {
        List<String> volumeIDList = new LinkedList<String>();

        if (log.isTraceEnabled()) log.trace("extracting volumeID from " + parsedDeltaLog.getPath());
        BufferedReader bufferedReader = null;
        String prefix = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(parsedDeltaLog));
            String line = null;
            do {
                line = bufferedReader.readLine();
                if (line != null) {
                    if (line.endsWith(".zip")) {
                        if (!line.startsWith("deleting ")) {
                            int index = line.lastIndexOf('/');
                            String zipFilename = line.substring(index + 1);
                            String cleanedPartialVolumeID = zipFilename.substring(0, zipFilename.length() - ".zip".length());
                            String uncleanedPartialVolumeID = pairtree.uncleanId(cleanedPartialVolumeID);
                            String volumeID = prefix + uncleanedPartialVolumeID;
                            if (log.isTraceEnabled()) log.trace("extracted volumeID: " + volumeID);
                            volumeIDList.add(volumeID);
                        }
                    } else if (line.startsWith("PREFIX=")) {
                        prefix = line.substring("PREFIX=".length());
                    }
                }
            } while (line != null);
        } catch (FileNotFoundException e) {
            log.error("Delta log file not found " + parsedDeltaLog.getPath(), e);
        } catch (IOException e) {
            log.error("Error reading delta log file " + parsedDeltaLog.getPath(), e);
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    log.error("Failed to close BufferedReader on file " + parsedDeltaLog.getPath(), e);
                }
            }
        }
        return volumeIDList;

    }
    
    public void verify(String volumeID) {
        
        try {

            int pageCount = hectorAccessor.retrieveVolumePageCount(volumeID);
            verifyPages(volumeID, pageCount);
            log.info("volume " + volumeID + " verified at level " + verificationLevel.toString());
            
        } catch (VerificationException e) {
            log.error(e.toString(), e);
        } catch (UnsupportedEncodingException e) {
            log.error(e.toString(), e);
        } catch (NoSuchAlgorithmException e) {
            log.error(e.toString(), e);
        } catch (HTimedOutException e) {
            log.error(e.toString(), e);
        }
        
    }
    

    protected void renameParsedFileToExtracted(File parsedFile) {
        File parent = parsedFile.getParentFile();
        String filename = parsedFile.getName();
        
        if (filename.startsWith("_PARSED_")) {
            String newFilename = "_XTRACTED_" + filename.substring("_PARSED_".length());
            File newFile = new File(parent, newFilename);
            boolean renamed = parsedFile.renameTo(newFile);
            if (renamed) {
                
                log.info("Renamed parsed deltalog file " + parsedFile.getPath() + " to " + newFile.getPath());
                 
            } else {
                log.error("Failed to rename parsed deltalog file " + parsedFile.getPath() + " to " + newFile.getPath());
            }
        }
    }
    
    protected void verifyPages(String volumeID, int pageCount)  throws VerificationException, UnsupportedEncodingException, NoSuchAlgorithmException, HTimedOutException {

        
        for (int i = 0; i < pageCount; i++) {
            String pageSequence = Util.prepadString(Integer.toString(i + 1), '0', 8);

            byte[] pageContents = hectorAccessor.retrievePageContents(volumeID, pageSequence);

            if (verificationLevel.include(VerificationLevelEnum.VERIFY_PAGE_COUNT)) {
                verifyPageCount(volumeID, pageSequence, pageContents);
                if (log.isTraceEnabled()) log.trace("verified page count for page " + pageSequence + " volume " + volumeID);
            }
            
            if (verificationLevel.include(VerificationLevelEnum.VERIFY_PAGE_SIZE)) {
                PageSizeMetadata pageSizeMetadata = hectorAccessor.retrievePageSizeMetadata(volumeID, pageSequence);
                
                verifyPageSize(volumeID, pageSequence, pageContents, pageSizeMetadata);
                if (log.isTraceEnabled()) log.trace("verified page size for page " + pageSequence + " volume " + volumeID);
            }
            
            if (verificationLevel.include(VerificationLevelEnum.VERIFY_PAGE_CHECKSUM)) {
                PageChecksumMetadata pageChecksumMetadata = hectorAccessor.retrievePageChecksumMetadata(volumeID, pageSequence);
                
                verifyPageChecksum(volumeID, pageSequence, pageContents, pageChecksumMetadata);
                if (log.isTraceEnabled()) log.trace("verified page checksum for page " + pageSequence + " volume " + volumeID);

            }
        }
    }

    protected void verifyPageCount(String volumeID, String pageSequence, byte[] pageContents) throws VerificationException {
        if (pageContents == null) {
            throw new VerificationException("No page contents", volumeID, pageSequence);
        }
    }
    

    protected void verifyPageSize(String volumeID, String pageSequence, byte[] pageContents, PageSizeMetadata pageSizeMetadata) throws VerificationException, UnsupportedEncodingException {
        
        long actualByteCount = pageContents.length;
        String pageContentString = new String(pageContents, "utf-8");
        int actualCharacterCount = pageContentString == null ? 0 : pageContentString.length();
        
        if (actualByteCount != pageSizeMetadata.getByteCount()) {
            throw new VerificationException("Actual byte count and recorded page byte count mismatch", volumeID, pageSequence);
        }

        if (actualCharacterCount != pageSizeMetadata.getCharacterCount()) {
            throw new VerificationException("Actual character count and recorded page character count mismatch", volumeID, pageSequence);
        }
        
        if (actualByteCount != pageContentString.getBytes("utf-8").length) {
//            throw new VerificationException("Actual byte count and UTF-8 string converted byte count mismatch", volumeID, pageSequence);
            log.warn("Two way conversion from raw bytes to UTF-8 String and back to bytes causes information loss. Volume: " + volumeID + " page: " + pageSequence);

        }

    }
    
    protected void verifyPageChecksum(String volumeID, String pageSequence, byte[] pageContents, PageChecksumMetadata pageChecksumMetadata) throws VerificationException, NoSuchAlgorithmException {
        if (pageChecksumMetadata != null) {
            String checksumType = pageChecksumMetadata.getChecksumType();
            if (checksumType != null) {
                
                String actualChecksum = Util.calculateChecksum(pageContents, checksumType);
                if (!actualChecksum.equals(pageChecksumMetadata.getChecksum())) {
                    throw new VerificationException("Actual checksum and recorded checksum mismatch", volumeID, pageSequence);
                }
            } else {
                throw new VerificationException("No page checksum type", volumeID, pageSequence);
            }
        } else {
            throw new VerificationException("No page checksum", volumeID, pageSequence);
        }
    }
}

