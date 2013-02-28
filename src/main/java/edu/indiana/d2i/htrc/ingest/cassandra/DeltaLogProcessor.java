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
# File:  DeltaLogProcessor.java
# Description:  
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
 * @author Yiming Sun
 *
 */
public class DeltaLogProcessor implements Runnable {
    
    static class VolumeRecord {
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
            
            void setSequence(String sequence) {
                this.sequence = sequence;
            }
            String getSequence() {
                return sequence;
            }
            void setOcrFilename(String ocrFilename) {
                this.ocrFilename = ocrFilename;
            }
            String getOcrFilename() {
                return ocrFilename;
            }
            void setChecksum(String checksum, String checksumType) {
                this.checksum = checksum;
                this.checksumType = checksumType;
            }
            String getChecksum() {
                return checksum;
            }
            String getChecksumType() {
                return checksumType;
            }
            void setByteCount(long byteCount) {
                this.byteCount = byteCount;
            }
            long getByteCount() {
                return byteCount;
            }
            void setCharacterCount(int characterCount) {
                this.characterCount = characterCount;
            }
            int getCharacterCount() {
                return characterCount;
            }
            void setLabel(String label) {
                this.label = label;
            }
            String getLabel() {
                return label;
            }
            void setID(String id) {
                this.id = id;
            }
            String getID() {
                return id;
            }
            void setOrder(int order) {
                this.order = order;
            }
            int getOrder() {
                return order;
            }
            void addFeature(String feature) {
                if (features == null) {
                    features = new ArrayList<String>(INITIAL_PAGE_FEATURE_LIST_SIZE);
                }
                features.add(feature);
            }
            List<String> getFeatures() {
                return features;
            }
        }
        
        static final int INITIAL_HASHMAP_CAPACITY = 300;
        private String volumeID;
        private int pageCount;
        private HashMap<String, PageRecord> hashMapByFilename;
        private HashMap<String, PageRecord> hashMapByID;
//        private List<PageRecord> orderedPageList;
        private CopyrightEnum copyright;
        private String metsContents;
        
        VolumeRecord(String volumeID) {
            this.volumeID = volumeID;
            this.pageCount = 0;
            this.hashMapByFilename = new HashMap<String, PageRecord>(INITIAL_HASHMAP_CAPACITY);
            this.hashMapByID = new HashMap<String, PageRecord>(INITIAL_HASHMAP_CAPACITY);
//            this.orderedPageList = new ArrayList<PageRecord>();
            this.copyright = CopyrightEnum.PUBLIC_DOMAIN;
            this.metsContents = null;
        }
        String getVolumeID() {
            return volumeID;
        }
        void setPageCount(int pageCount) {
            this.pageCount = pageCount;
        }
        int getPageCount() {
            return pageCount;
        }
        void addPageRecord(PageRecord pageRecord) {
            String ocrFilename = pageRecord.getOcrFilename();
            String id = pageRecord.getID();
            hashMapByFilename.put(ocrFilename, pageRecord);
            hashMapByID.put(id, pageRecord);
        }
        Set<String> getPageFilenameSet() {
           return hashMapByFilename.keySet(); 
        }
        Set<String> getPageIDSet() {
            return hashMapByID.keySet();
        }
        PageRecord getPageRecordByFilename(String filename) {
            return hashMapByFilename.get(filename);
        }
        PageRecord getPageRecordByID(String id) {
            return hashMapByID.get(id);
        }
        
        void setCopyright(CopyrightEnum copyright) {
            this.copyright = copyright;
        }
        CopyrightEnum getCopyright() {
            return copyright;
        }
        void setMETSContents(String metsContents) {
            this.metsContents = metsContents;
        }
        String getMETSContents() {
            return metsContents;
        }
        void updatePageCount() {
            pageCount = hashMapByFilename.size();
            pageCount = hashMapByID.size() > pageCount ? hashMapByID.size() : pageCount;
        }
//        List<PageRecord> getOrderedPageList() {
//            return this.orderedPageList;
//        }
//        void addPageInOrderedList(int index, PageRecord pageRecord) {
//            this.orderedPageList.add(index, pageRecord);
//        }
    }
    
    private static Logger log = Logger.getLogger(DeltaLogProcessor.class);
    
    private JobQueue<String> jobQueue;
    private final int id;
    
    protected DeltaLogProcessor(int id, JobQueue<String> jobQueue) {
        this.id = id;
        this.jobQueue = jobQueue;
    }
    
    private void deleteFromCassandra (List<VolumeDeletionInfo> volumeDeletionList) throws HInvalidRequestException, HTimedOutException {
        HectorManager hectorManager = HectorManager.getInstance();
        hectorManager.delete(volumeDeletionList);
    }
    
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

