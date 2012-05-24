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

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

import me.prettyprint.hector.api.exceptions.HTimedOutException;

import org.apache.log4j.Logger;

import edu.indiana.d2i.htrc.ingest.JobQueue;
import edu.indiana.d2i.htrc.ingest.Util;
import edu.indiana.d2i.htrc.ingest.verify.HectorAccessor.PageChecksumMetadata;
import edu.indiana.d2i.htrc.ingest.verify.HectorAccessor.PageSizeMetadata;

/**
 * @author Yiming Sun
 *
 */
public class Verifier implements Runnable {
    static enum VerificationLevelEnum {
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
    
    private VerificationLevelEnum verificationLevel;
    private HectorAccessor hectorAccessor;
    private JobQueue<String> jobQueue;

    protected Verifier(JobQueue<String> jobQueue, VerificationLevelEnum verificationLevel) {
        this.jobQueue = jobQueue;
        this.verificationLevel = verificationLevel;
        this.hectorAccessor = HectorAccessor.getInstance();
        
    }
    
    public void run() {
        while (!jobQueue.isDone()) {
            String volumeID = jobQueue.dequeue();
            if (log.isTraceEnabled()) log.trace("volumeID dequeued: " + volumeID);
            verify(volumeID);
        }
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

