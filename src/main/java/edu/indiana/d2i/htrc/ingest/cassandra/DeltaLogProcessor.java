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
# File:  DeltaLogProcessor.java
# Description: This class looks for delta log files from specified directories and initiate the processing of the delta logs 
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.cassandra;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.exceptions.HTimedOutException;

import org.apache.log4j.Logger;

import edu.indiana.d2i.htrc.ingest.Constants.CopyrightEnum;
import edu.indiana.d2i.htrc.ingest.JobQueue;
import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogParser.VolumeDeletionInfo;
import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogParser.VolumeUpdateInfo;

/**
 * This class looks for delta log files from specified directories and initiate the processing of the delta logs
 * @author Yiming Sun
 *
 */
public class DeltaLogProcessor implements Runnable {
    
    /**
     * This class is a container of volume level information
     * @author Yiming Sun
     *
     */
    static class VolumeRecord {
        /**
         * This class is a container of page level information
         * @author Yiming Sun
         *
         */
        static class PageRecord {
            
            protected static final int INITIAL_PAGE_FEATURE_LIST_SIZE = 20;
            
            private String ocrFilename;
            private String sequence;
            private String checksum;
            private String checksumType;
            private String label;
            private String id;
            private List<String> features;
            private long byteCount;
            private int characterCount;
            private int order;
            
            /**
             * Constructor
             */
            PageRecord() {
                this.sequence = null;
                this.ocrFilename = null;
                this.checksum = null;
                this.checksumType = null;
                this.id = null;
                this.byteCount = 0L;
                this.characterCount = 0;
                this.label = null;
                this.order = 0;
                this.features = null;
            }
            
            /**
             * Method to set the page sequence number of the page
             * @param sequence
             */
            void setSequence(String sequence) {
                this.sequence = sequence;
            }
            /**
             * Method to get the page sequence number of the page
             * @return the page sequence number of the page
             */
            String getSequence() {
                return sequence;
            }
            /**
             * Method to set the OCR text filename of the page
             * @param ocrFilename
             */
            void setOcrFilename(String ocrFilename) {
                this.ocrFilename = ocrFilename;
            }
            /**
             * Method to get the OCR text filename of the page
             * @return the OCR text filename of the page
             */
            String getOcrFilename() {
                return ocrFilename;
            }
            /**
             * Method to set the content checksum of the page
             * @param checksum String representation of the checksum in hexadecimal format
             * @param checksumType type of the checksum
             */
            void setChecksum(String checksum, String checksumType) {
                this.checksum = checksum;
                this.checksumType = checksumType;
            }
            /**
             * Method to get the content checksum of the page
             * @return the content checksum of the page
             */
            String getChecksum() {
                return checksum;
            }
            
            /**
             * Method to get the checksum type
             * @return the checksum type
             */
            String getChecksumType() {
                return checksumType;
            }
            
            /**
             * Method to set the size of the page content in bytes
             * @param byteCount size of the page content in bytes
             */
            void setByteCount(long byteCount) {
                this.byteCount = byteCount;
            }
            
            /**
             * Method to get the size of the page content in bytes
             * @return size of the page content in bytes
             */
            long getByteCount() {
                return byteCount;
            }
            
            /**
             * Method to set the size of the page content in characters
             * @param characterCount size of the page content in characters
             */
            void setCharacterCount(int characterCount) {
                this.characterCount = characterCount;
            }
            
            /**
             * Method to get the size of the page content in characters
             * @return size of the page content in characters
             */
            int getCharacterCount() {
                return characterCount;
            }
            /**
             * Method to set the page label of the page
             * @param label page label of the page
             */
            void setLabel(String label) {
                this.label = label;
            }
            
            /**
             * Method to get the page label of the page
             * @return page label of the page
             */
            String getLabel() {
                return label;
            }
            
            /**
             * Method to set the METS ID of the page 
             * @param id METS ID of the page
             */
            void setID(String id) {
                this.id = id;
            }
            
            /**
             * Method to get the METS ID of the page
             * @return METS ID of the page
             */
            String getID() {
                return id;
            }
            /**
             * Method to set the ordering of the page
             * @param order ordering of the page
             */
            void setOrder(int order) {
                this.order = order;
            }
            /**
             * Method to get the ordering of the page
             * @return ordering of the page
             */
            int getOrder() {
                return order;
            }
            /**
             * Method to add a page feature keyword for the page
             * @param feature a page feature keyword for the page
             */
            void addFeature(String feature) {
                if (features == null) {
                    features = new ArrayList<String>(INITIAL_PAGE_FEATURE_LIST_SIZE);
                }
                features.add(feature);
            }
            /**
             * Method to get the list of page feature keywords for the page
             * @return List of page feature keywords for the page
             */
            List<String> getFeatures() {
                return features;
            }
        }
        
        static final int INITIAL_HASHMAP_CAPACITY = 300;
        private String volumeID;
        private int pageCount;
        private HashMap<String, PageRecord> hashMapByFilename;
        private HashMap<String, PageRecord> hashMapByID;
        private CopyrightEnum copyright;
        private String metsContents;

        /**
         * Constructor
         * @param volumeID volumeID of the volume
         */
        VolumeRecord(String volumeID) {
            this.volumeID = volumeID;
            this.pageCount = 0;
            this.hashMapByFilename = new HashMap<String, PageRecord>(INITIAL_HASHMAP_CAPACITY);
            this.hashMapByID = new HashMap<String, PageRecord>(INITIAL_HASHMAP_CAPACITY);
            this.copyright = CopyrightEnum.PUBLIC_DOMAIN;
            this.metsContents = null;
        }
        /**
         * Method to return the volumeID
         * @return the volumeID
         */
        String getVolumeID() {
            return volumeID;
        }
        /**
         * Method to set the number of pages the volume has
         * @param pageCount number of pages the volume has
         */
        void setPageCount(int pageCount) {
            this.pageCount = pageCount;
        }
        /**
         * Method to get the number of pages the volume has
         * @return the number of pages the volume has
         */
        int getPageCount() {
            return pageCount;
        }
        /**
         * Method to add a PageRecord object
         * @param pageRecord a PageRecord object
         */
        void addPageRecord(PageRecord pageRecord) {
            String ocrFilename = pageRecord.getOcrFilename();
            String id = pageRecord.getID();
            hashMapByFilename.put(ocrFilename, pageRecord);
            hashMapByID.put(id, pageRecord);
        }
        /**
         * Method to get a Set containing filenames of all pages
         * @return a Set containing filenames of all pages
         */
        Set<String> getPageFilenameSet() {
           return hashMapByFilename.keySet(); 
        }
        
        /**
         * Method to get a Set containing internal IDs of the pages from METS
         * @return a Set containing internal IDs of the pages from METS
         */
        Set<String> getPageIDSet() {
            return hashMapByID.keySet();
        }
        /**
         * Method to get the PageRecord object for the page identified by the specified filename
         * @param filename filename of a page
         * @return the PageRecord object for the page identified by the specified filename
         */
        PageRecord getPageRecordByFilename(String filename) {
            return hashMapByFilename.get(filename);
        }
        /**
         * Method to get the PageRecord object for the page identified by the internal ID used in METS
         * @param id the internal ID of a page used in METS
         * @return the PageRecord object for the page identified by the internal ID used in METS
         */
        PageRecord getPageRecordByID(String id) {
            return hashMapByID.get(id);
        }
        /**
         * Method to set the copyright of the volume
         * @param copyright copyright of the volume
         */
        void setCopyright(CopyrightEnum copyright) {
            this.copyright = copyright;
        }
        /**
         * Method to get the copyright of the volume
         * @return copyright of the volume
         */
        CopyrightEnum getCopyright() {
            return copyright;
        }
        /**
         * Method to set the METS content of the volume
         * @param metsContents the METS content of the volume
         */
        void setMETSContents(String metsContents) {
            this.metsContents = metsContents;
        }
        /**
         * Method to get the METS content of the volume
         * @return the METS content of the volume
         */
        String getMETSContents() {
            return metsContents;
        }
        /**
         * Method that updates the page count of the volume based on the actual pages added
         */
        void updatePageCount() {
            pageCount = hashMapByFilename.size();
            pageCount = hashMapByID.size() > pageCount ? hashMapByID.size() : pageCount;
        }
    }
    
    private static Logger log = Logger.getLogger(DeltaLogProcessor.class);
    
    private JobQueue<String> jobQueue;
    private final int id;

    /**
     * Constructor
     * @param id an int to differentiate and identify instances
     * @param jobQueue a JobQueue of String objects representing directory paths containing delta log files
     */
    protected DeltaLogProcessor(int id, JobQueue<String> jobQueue) {
        this.id = id;
        this.jobQueue = jobQueue;
    }
    
    /**
     * Method to initiate the deletion of a volume from Cassandra
     * @param volumeDeletionList a List of VolumeDeletionInfo objects representing volumes to be deleted from Cassandra
     * @throws HInvalidRequestException thrown from Hector client
     * @throws HTimedOutException thrown from Hector client
     */
    private void deleteFromCassandra (List<VolumeDeletionInfo> volumeDeletionList) throws HInvalidRequestException, HTimedOutException {
        HectorManager hectorManager = HectorManager.getInstance();
        hectorManager.delete(volumeDeletionList);
    }
    
    /**
     * Method to initiate the update of a volume to Cassandra
     * @param volumeUpdateList a List of VolumeUpdateInfo objects representing volumes to be updated in Cassandra
     * @throws HInvalidRequestException thrown from Hector client
     * @throws HTimedOutException thrown from Hector client
     */
    private void updateToCassandra(List<VolumeUpdateInfo> volumeUpdateList) throws HInvalidRequestException, HTimedOutException {
        HectorManager hectorManager = HectorManager.getInstance();
        
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        
        for (VolumeUpdateInfo volumeUpdateInfo : volumeUpdateList) {
            String volumeID = volumeUpdateInfo.getVolumeID();
            String cleanedPartialVolumeID = volumeUpdateInfo.getCleanedPartialVoluemID();
            String volumePath = volumeUpdateInfo.getVolumePath();
            CopyrightEnum copyright = volumeUpdateInfo.getCopyright();
            
            VolumeRecord volumeRecord = new VolumeRecord(volumeID);
            volumeRecord.setCopyright(copyright);
            
            File metsFile = new File(volumePath, cleanedPartialVolumeID + ".mets.xml");
            METSParser metsParser = new METSParser(metsFile, volumeRecord, xmlInputFactory);
            try {
                metsParser.parse();
                volumeRecord = metsParser.getVolumeRecord();
                
                hectorManager.update(volumeUpdateInfo, volumeRecord);
                
            } catch (FileNotFoundException e) {
                log.error("mets file not found " + metsFile.getPath(), e);
            } catch (IOException e) {
                log.error("IOException while parsing mets file " + metsFile.getPath(), e);
            } catch (XMLStreamException e) {
                log.error("XMLStreamException while parsing mets file " + metsFile.getPath(), e);
            }
        }
        
    }
    /**
     * Method to mark a delta log file as being parsed
     * @param deltaLogPath path to a delta log file
     */
    private void markLogAsDone(String deltaLogPath) {
        File deltaLogFile = new File(deltaLogPath);
        
        String filename = deltaLogFile.getName();
        File parentFile = deltaLogFile.getParentFile();
        
        File newNameFile = new File(parentFile, "_PARSED_" + filename);
        
        
        boolean renameTo = deltaLogFile.renameTo(newNameFile);
        
        if (renameTo) {
            log.info("deltaLog " + deltaLogPath + " renamed to " + newNameFile.getPath());
        } else {
            log.warn("Failed to rename deltaLog " + deltaLogPath + " to " + newNameFile.getPath());
        }

    }
    /**
     * 
     * @see java.lang.Runnable#run()
     */
    public void run() {
        
        String deltaLogPath = null;
        boolean isAllDone = false;
        
        do {
            deltaLogPath = jobQueue.dequeue();
            if (log.isTraceEnabled()) log.trace("deltaLogPath dequeued: " + deltaLogPath);
            
            if (deltaLogPath != null) {
                DeltaLogParser deltaParser = new DeltaLogParser(deltaLogPath);
                try {
                    deltaParser.parse();
                    log.info("Parsed " + deltaLogPath);
                    
                    List<VolumeDeletionInfo> volumeDeletionList = deltaParser.getVolumeDeletionList();
                    if (log.isTraceEnabled()) log.trace("volumeDeletionList size " + volumeDeletionList.size());
                    
                    List<VolumeUpdateInfo> volumeUpdateList = deltaParser.getVolumeUpdateList();
                    if (log.isTraceEnabled()) log.trace("volumeUpdateList size " + volumeUpdateList.size());
                    
                    deleteFromCassandra(volumeDeletionList);
                    updateToCassandra(volumeUpdateList);
                    
                    
                    markLogAsDone(deltaLogPath);
                    
                    
                } catch (FileNotFoundException e) {
                    log.error("DeltaLog file not found: " + deltaLogPath, e);
                } catch (IOException e) {
                    log.error("Failed to parse deltaLog file " + deltaLogPath, e);
                } catch (HInvalidRequestException e) {
                    log.error("Invalid request exception. Reason: " + e.getWhy(), e);
                } catch (HTimedOutException e) {
                    log.error("Timed out exception", e);
                }
            } else {
                isAllDone = jobQueue.isDone();
                if (log.isTraceEnabled()) log.trace("isAllDone: " + isAllDone);
            }
        } while (!isAllDone);
            
    }
}

