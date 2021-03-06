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
# File:  HectorManager.java
# Description: This class provides higher level methods to encapsulate some Hector API calls
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.StringKeyIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.exceptions.HTimedOutException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.log4j.Logger;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.Constants.CopyrightEnum;
import edu.indiana.d2i.htrc.ingest.PropertyReader;
import edu.indiana.d2i.htrc.ingest.Util;
import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogParser.VolumeDeletionInfo;
import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogParser.VolumeUpdateInfo;
import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogProcessor.VolumeRecord;
import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogProcessor.VolumeRecord.PageRecord;

/**
 * This class provides higher level methods to encapsulate some Hector API calls
 * @author Yiming Sun
 *
 */
public class HectorManager {
    

    private static Logger log = Logger.getLogger(HectorManager.class);
    
    protected static final byte[] VALUELESS_FILLER = new byte[0];
    
    protected static final HectorManager instance = new HectorManager();

    
    protected final Cluster cluster;
    protected final String keyspaceName;
    protected final String volumeContentsCFName;
    protected final String collectionsCFName;
    protected final String collectionNamesCFName;
    
    protected final int maxAttempts;
    protected final long initFailureDelay;
    protected final long maxFailureDelay;
    
    /**
     * Method to return the singleton instance of HectorManager
     * @return the singleton instance of HectorManager
     */
    public static HectorManager getInstance() {
        return instance;
    }
    
    /**
     * Constructor
     */
    private HectorManager() {
        PropertyReader propertyReader = PropertyReader.getInstance();
        
        String cassandraAddress = propertyReader.getProperty(Constants.PK_CASSANDRA_HOST);
        
        cluster = HFactory.getOrCreateCluster(propertyReader.getProperty(Constants.PK_CASSANDRA_CLUSTER_NAME), new CassandraHostConfigurator(cassandraAddress));
        keyspaceName = propertyReader.getProperty(Constants.PK_CASSANDRA_KEYSPACE_NAME);
        volumeContentsCFName = propertyReader.getProperty(Constants.PK_CASSANDRA_VOLUMECONTENTS_CF_NAME);
        collectionsCFName = propertyReader.getProperty(Constants.PK_CASSANDRA_COLLECTIONS_CF_NAME);
        collectionNamesCFName = propertyReader.getProperty(Constants.PK_CASSANDRA_COLLECTION_NAMES_CF_NAME);
        
        maxAttempts = Integer.parseInt(propertyReader.getProperty(Constants.PK_UPDATE_FAILURE_MAX_ATTEMPTS));
        initFailureDelay = Long.parseLong(propertyReader.getProperty(Constants.PK_UPDATE_FAILURE_BACK_OFF_INIT_DELAY_IN_MS));
        maxFailureDelay = Long.parseLong(propertyReader.getProperty(Constants.PK_UPDATE_FAILURE_BACK_OFF_MAX_DELAY_IN_MS));
        
    }

    /**
     * Method to create a ColumnFamilyTemplate object
     * @param keySerializer the serializer for the row key
     * @param columnNameSerializer the serializer for the column name
     * @param columnFamilyName the name of the column family
     * @return a ColumnFamily object
     */
    protected <K, N> ColumnFamilyTemplate<K, N> createColumnFamilyTemplate(Serializer<K> keySerializer, Serializer<N> columnNameSerializer, String columnFamilyName) {
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        ColumnFamilyTemplate<K, N> columnFamilyTemplate = new ThriftColumnFamilyTemplate<K, N>(keyspace, columnFamilyName, keySerializer, columnNameSerializer);
        return columnFamilyTemplate;
    }
    
    /**
     * Method to carry out the deletion of volumes from Cassandra via Hector
     * @param volumeDeletionList a List of VolumeDeletionInfo objects representing volumes to be deleted
     * @throws HInvalidRequestException thrown from Hector client
     * @throws HTimedOutException thrown from Hector client
     */
    public void delete(List<VolumeDeletionInfo> volumeDeletionList) throws HInvalidRequestException, HTimedOutException {
        boolean successful = false;
        int attemptsLeft = maxAttempts;
        long failureDelay = initFailureDelay;
        
        Serializer<String> stringSerializer = StringSerializer.get();
        
        ColumnFamilyTemplate<String, String> volumeContentsCFTemplate = createColumnFamilyTemplate(stringSerializer, stringSerializer, volumeContentsCFName);
        volumeContentsCFTemplate.setBatched(true);
        
        Mutator<String> volumeContentsCFMutator = volumeContentsCFTemplate.createMutator();
        
        ColumnFamilyTemplate<String, String> collectionsCFTemplate = createColumnFamilyTemplate(stringSerializer, stringSerializer, collectionsCFName);
        collectionsCFTemplate.setBatched(true);
        
        Mutator<String> collectionsCFMutator = collectionsCFTemplate.createMutator();
        
        
        
        for (VolumeDeletionInfo volumeDeletionInfo : volumeDeletionList) {
            String volumeID = volumeDeletionInfo.getVolumeID();
            
            boolean isDeleteVolume = volumeDeletionInfo.getIsDeleteVolume();
            if (isDeleteVolume) {
                volumeContentsCFMutator.addDeletion(volumeID, volumeContentsCFName);
                log.info("row marked for delete for: " + volumeID);
                
                String copyrightString = volumeDeletionInfo.getCopyright().toString();
                
                collectionsCFMutator.addDeletion("ALL", collectionsCFName, volumeID, stringSerializer);
                log.info("volume marked for delete from ALL collection: " + volumeID);
                
                collectionsCFMutator.addDeletion(copyrightString, collectionsCFName, volumeID, stringSerializer);
                log.info("volume marked for delete from " + copyrightString + " collection: " + volumeID);
                
                
            } else {
                boolean isDeleteMETS = volumeDeletionInfo.getIsDeleteMETS();
                
                if (isDeleteMETS) {
                    ColumnFamilyUpdater<String, String> updater = volumeContentsCFTemplate.createUpdater(volumeID);
                    updater.deleteColumn(Constants.CN_VOLUME_METS);
                    if (log.isTraceEnabled()) log.trace("METS marked for delete for: " + volumeID);
                }

            }
        }
        
        if (log.isTraceEnabled()) log.trace("done iteration in HectorManager.delete");
        
       
        do {
            try {
                volumeContentsCFTemplate.executeBatch(volumeContentsCFMutator);
                successful = true;
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to batch delete on VolumeContents. Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                } else {
                    log.error("Failed to batch delete on VolumeContents.", e);
                    throw e;
                }
            }
        } while (!successful && attemptsLeft > 0);
        
        successful = false;
        attemptsLeft = maxAttempts;
        failureDelay = initFailureDelay;

        do {
            try {
                collectionsCFTemplate.executeBatch(collectionsCFMutator);
                successful = true;
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to batch delete on Collections. Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                } else {
                    log.error("Failed to batch delete on Collections.", e);
                    throw e;
                }
            }
        } while (!successful && attemptsLeft > 0);

        
        
        if (log.isTraceEnabled()) log.trace("done executingBatch in HectorManager.delete");
    }
    
    /**
     * Method to carry out the update of a volume in Cassandra
     * @param volumeUpdateInfo a VolumeUpdateInfo object representing the volume to be updated
     * @param volumeRecord a VolumeRecord object containing the metadata of the volume
     */
    public void update(VolumeUpdateInfo volumeUpdateInfo, VolumeRecord volumeRecord) {
        Serializer<String> stringSerializer = new StringSerializer();

        ColumnFamilyTemplate<String, String> volumeContentsCFTemplate = createColumnFamilyTemplate(stringSerializer, stringSerializer, volumeContentsCFName);
        
        ColumnFamilyTemplate<String, String> collectionsCFTemplate = createColumnFamilyTemplate(stringSerializer, stringSerializer, collectionsCFName);
        
        
        
        String volumeID = volumeUpdateInfo.getVolumeID();
        String cleanedPartialVoluemID = volumeUpdateInfo.getCleanedPartialVoluemID();
        String volumePath = volumeUpdateInfo.getVolumePath();
        CopyrightEnum copyright = volumeUpdateInfo.getCopyright();
        
        String volumeZipPath = volumePath + cleanedPartialVoluemID + ".zip";
        
        boolean volumeAdded = updateVolumeContents(volumeID, volumeZipPath, volumeRecord, volumeContentsCFTemplate);
        
        if (volumeAdded) {
            HashSet<String> collectionNameSet = new HashSet<String>();
            
            updateCollectionsColumnFamily(volumeID, copyright, collectionsCFTemplate, VALUELESS_FILLER, collectionNameSet);
            updateCollectionNamesColumnFamily(collectionNameSet, VALUELESS_FILLER);
        }
    }
    

    /**
     * Method to update the collection names column family
     * @param collectionNameSet a HashSet containing collection names
     * @param valuelessFiller a filler used for creating value-less Cassandra columns
     * @throws HInvalidRequestException thrown from Hector client
     * @throws HTimedOutException thrown from Hector client
     */
    protected void updateCollectionNamesColumnFamily(HashSet<String> collectionNameSet, byte[] valuelessFiller) throws HInvalidRequestException, HTimedOutException {
        boolean successful = false;
        int attemptsLeft = maxAttempts;
        long failureDelay = initFailureDelay;

        Serializer<String> stringSerializer = new StringSerializer();
        
        ColumnFamilyTemplate<String, String> collectionNamesCFTemplate = createColumnFamilyTemplate(stringSerializer, stringSerializer, collectionNamesCFName);
        ColumnFamilyUpdater<String, String> updater = collectionNamesCFTemplate.createUpdater("name");
        
        for (String collectionName : collectionNameSet) {
            updater.setByteArray(collectionName, valuelessFiller);
        }
        
        do {
            try {
                collectionNamesCFTemplate.update(updater);
                successful = true;
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to update collectionNames. Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                } else {
                    log.error ("Failed to update collectionNames.", e);
                    throw e;
                }
            }
        } while (!successful && attemptsLeft > 0);
        
        if (log.isTraceEnabled()) log.trace("finished updating CollectionNames");
        
    }
    
    /**
     * Method to update the collections column family in Cassandra with the newly updated volume
     * @param volumeID volumeID of the volume
     * @param copyright copyright of the volume
     * @param collectionsCFTemplate the ColumnFamilyTemplate object of the collections column family
     * @param valuelessFiller a filler used to create a value-less column in Cassandra
     * @param collectionNameSet a Set containing collection names
     * @throws HInvalidRequestException thrown from Hector client
     * @throws HTimedOutException thrown from Hector client
     */
    protected void updateCollectionsColumnFamily(String volumeID, CopyrightEnum copyright, ColumnFamilyTemplate<String, String> collectionsCFTemplate, byte[] valuelessFiller, HashSet<String> collectionNameSet) throws HInvalidRequestException, HTimedOutException {
        boolean successful = false;
        int attemptsLeft = maxAttempts;
        long failureDelay = initFailureDelay;
        
        ColumnFamilyUpdater<String, String> allCollectionCFUpdater = collectionsCFTemplate.createUpdater("ALL");
        ColumnFamilyUpdater<String, String> copyrightBasedCollectionCFUpdater = collectionsCFTemplate.createUpdater(copyright.toString());
        
        collectionNameSet.add("ALL");
        collectionNameSet.add(copyright.toString());
        
        allCollectionCFUpdater.setByteArray(volumeID, valuelessFiller);
        copyrightBasedCollectionCFUpdater.setByteArray(volumeID, valuelessFiller);
        
        do {
            try {
                collectionsCFTemplate.update(allCollectionCFUpdater);
                successful = true;
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to update the ALL collection. Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                } else {
                    log.error ("Failed to update the ALL collection.", e);
                    throw e;
                }
            }
        } while (!successful && attemptsLeft > 0);
        
        if (log.isTraceEnabled()) log.trace("finished updating row ALL in Collections");
        
        successful = false;
        attemptsLeft = maxAttempts;
        failureDelay = initFailureDelay;
        
        do {
            try {
                collectionsCFTemplate.update(copyrightBasedCollectionCFUpdater);
                successful = true;
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to update the " + copyright.toString() + " collection. Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                } else {
                    log.error ("Failed to update the " + copyright.toString() + " collection.", e);
                    throw e;
                }
            }
        } while (!successful && attemptsLeft > 0);
        
        if (log.isTraceEnabled()) log.trace("finished updating row " + copyright.toString() + " in Collections");
    }
    
    
    /**
     * Method to update the volume content in Cassandra
     * @param volumeID volumeID of the volume
     * @param volumeZipPath path to the Zip file of the volume
     * @param volumeRecord a VolumeRecord object containing the metadata of the volume
     * @param volumeCFTemplate a ColumnFamilyTemplate object for the VolumeContents column family
     * @return a boolean flag indicating if the operation succeeded or not
     * @throws HInvalidRequestException thrown from Hector client
     * @throws HTimedOutException thrown from Hector client
     */
    protected boolean updateVolumeContents(String volumeID, String volumeZipPath, VolumeRecord volumeRecord, ColumnFamilyTemplate<String, String> volumeCFTemplate) throws HInvalidRequestException, HTimedOutException {
        HashMap<Integer, PageRecord> pageMapByOrder = new HashMap<Integer, PageRecord>();
        HashMap<String, List<String>> featuredPagesMap = new HashMap<String, List<String>>();
        
        boolean volumeAdded = false;
        
        try {

            boolean hasValidPage = false;
            
            FileInputStream fileInputStream = new FileInputStream(volumeZipPath);
            ZipInputStream zipInputStream = new ZipInputStream(fileInputStream);
            
            ZipEntry zipEntry = null;
            
            do {
                try {
                    zipEntry = zipInputStream.getNextEntry();
                    if (zipEntry != null) {
                        String entryName = zipEntry.getName();
                        if (log.isTraceEnabled()) log.trace("Extracting " + entryName + " from " + volumeZipPath);
                        String entryFilename = extractEntryFilename(entryName);
                        if (log.isTraceEnabled()) log.trace("entryFilename: " + entryFilename);
                        if (entryFilename != null && !("".equals(entryFilename))) {
                            PageRecord pageRecord = volumeRecord.getPageRecordByFilename(entryFilename);
                            if (pageRecord != null) {
                                
                                // STEP 1 - read page entry contents from zip as byte stream
                                byte[] pageContents = readPageContentsFromInputStream(zipInputStream);
                                
                                // STEP 2 - check if content length match what is declared in METS
                                if (pageContents.length != pageRecord.getByteCount()) {
                                    log.warn("Actual byte count and byte count from METS mismatch for entry " + entryName + " for volume " + volumeID + ". Actual: " + pageContents.length + " from METS: " + pageRecord.getByteCount());
                                    log.info("Recording actual byte count");
                                    pageRecord.setByteCount(pageContents.length);
                                } else {
                                    if (log.isTraceEnabled()) log.trace("Verified byte count for entry " + entryName + " for volume " + volumeID);
                                }
                                
                                // STEP 3 - check if checksum match what is declared in METS
                                String checksum = pageRecord.getChecksum();
                                String checksumType = pageRecord.getChecksumType();
                                try {
                                    String calculatedChecksum = Util.calculateChecksum(pageContents, checksumType);
                                    if (!checksum.equals(calculatedChecksum)) {
                                        log.warn("Actual checksum and checksum from METS mismatch for entry " + entryName + " for volume: " + volumeID + ". Actual: " + calculatedChecksum + " from METS: " + checksum);
                                        log.info("Recording actual checksum");
                                        pageRecord.setChecksum(calculatedChecksum, checksumType);
                                    } else {
                                        if (log.isTraceEnabled()) log.trace("Verified checksum for entry " + entryName + " for volume " + volumeID);
                                    }
                                    
                                } catch (NoSuchAlgorithmException e) {
                                    log.warn("NoSuchAlgorithmException for checksum algorithm " + checksumType, e);
                                    log.info("Using checksum found in METS with a leap of faith");
                                }
                                
                                // STEP 4 - push into map by order and generate sequence
                                int order = pageRecord.getOrder();
                                
                                if (order > 0) {
                                    if (log.isTraceEnabled()) log.trace("entryFilename: " + entryFilename + "  order: " + order);
                                    pageMapByOrder.put(order, pageRecord);
                                    String sequence = generateSequence(order);
                                    if (log.isTraceEnabled()) log.trace("entryFilename: " + entryFilename + "  sequence: " + sequence);
                                    pageRecord.setSequence(sequence);
                                    
                                    // STEP 5 - convert to string and count character count -- NOTE: some pages are not encoded in utf-8, but there is no charset indicator, so assume utf-8 for all for now
                                    String pageContentsString = new String(pageContents, "utf-8");
                                    pageRecord.setCharacterCount(pageContentsString.length());
                                    
                                    // STEP 6 - get page features - this step is done in updatePage as a side effect
                                    
                                    // STEP 7 - push page contents to noSQL
                                    try {
                                        updatePage(pageContents, volumeID, pageRecord, featuredPagesMap, volumeCFTemplate);
                                        if (log.isTraceEnabled()) log.trace("successfully updated page " + entryFilename + " for volume " + volumeID);
                                        
                                        hasValidPage = true;
                                    } catch (HInvalidRequestException e) {
                                        log.error("HInvalidRequestException when updating volume. volumeID: " + volumeID + " pageSequence: " + pageRecord.getSequence(), e);
                                    } catch (HTimedOutException e) {
                                        log.error("HTimedOutException when updating volume. volumeID: " + volumeID + " pageSequence: " + pageRecord.getSequence(), e);
                                    }
                                } else {
                                    log.warn("Invalid PageRecord order for " + entryFilename + " in volume zip " + volumeZipPath);
                                }
                                
                            } else {
                                log.warn("No PageRecord found by " + entryFilename + " in volume zip " + volumeZipPath);
                            }
                        }
                    }
                } catch (IOException e) {
                    log.error("IOException getting entry from ZIP " + volumeZipPath, e);
                } finally {
                    if (zipEntry != null) {
                        try {
                            zipInputStream.closeEntry();
                        } catch (IOException e) {
                            log.error("Failed to close zipEntry", e);
                        }
                    }
                }            
            } while (zipEntry != null);

            log.info("Successfully pushed all pages for volume " + volumeID);

            try {
//                bufferedReader.close();
                zipInputStream.close();
            } catch (IOException e) {
                log.error("Failed to close ZipInputStream on " + volumeZipPath, e);
            }

            if (hasValidPage) {
                updateVolumeMetadata(volumeID, volumeRecord, featuredPagesMap, volumeCFTemplate);
                log.info("successfully updated metadata for volume " + volumeID);
                volumeAdded = true;
            } else {
                log.error("Skipping update of volume metadata as no valid page order found. volumeID: " + volumeID);
            }

        } catch (FileNotFoundException e) {
            log.error("Volume zip file not found " + volumeZipPath, e);
        }
        
        return volumeAdded;
        
    }

    /**
     * Method to update the metadata for a volume
     * @param volumeID volumeID of the volume
     * @param volumeRecord a VolumeRecord object containing the metadata of the volume
     * @param featuredPagesMap a HashMap containing all page feature keywords of pages from the volume
     * @param volumeCFTemplate a ColumnFamilyTemplate object for the VolumeContents column family
     */
      protected void updateVolumeMetadata(String volumeID, VolumeRecord volumeRecord, HashMap<String, List<String>> featuredPagesMap, ColumnFamilyTemplate<String, String> volumeCFTemplate) {
        boolean successful = false;
        int attemptsLeft = maxAttempts;
        long failureDelay = initFailureDelay;

        ColumnFamilyUpdater<String, String> volumeMetadataUpdater = volumeCFTemplate.createUpdater(volumeID);
        
        
        CopyrightEnum copyright = volumeRecord.getCopyright();
        volumeMetadataUpdater.setString(Constants.CN_VOLUME_COPYRIGHT, copyright.toString());
        
        String metsContents = volumeRecord.getMETSContents();
        volumeMetadataUpdater.setString(Constants.CN_VOLUME_METS, metsContents);
        
        int pageCount = volumeRecord.getPageCount();
        volumeMetadataUpdater.setInteger(Constants.CN_VOLUME_PAGECOUNT, pageCount);

        // However, page feature set is still needed on the volume level because this lists all pages with one particular feature where as the page-level metadata is for each page only
        Set<String> featureSet = featuredPagesMap.keySet();
        if (featureSet != null) {
            for (String feature : featureSet) {
                List<String> list = featuredPagesMap.get(feature);
                if (list != null && !list.isEmpty()) {
                    Collections.sort(list);
                    StringBuilder featurePagesBuilder = new StringBuilder(list.get(0));
                    for (int i = 1; i < list.size(); i++) {
                        featurePagesBuilder.append(", ").append(list.get(i));
                    }
                    volumeMetadataUpdater.setString(Constants.CN_VOLUME_PAGEFEATURE_PREFIX + feature, featurePagesBuilder.toString());
                }
            }
        }
        
        do {
            try {
                volumeCFTemplate.update(volumeMetadataUpdater);
                successful = true;
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to update volume metadata. volume: " + volumeID + " Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                    
                } else {
                    log.error("Failed to update volume metadata. volume: " + volumeID, e);
                    throw e;
                }
            }
        } while (!successful && attemptsLeft > 0);

    }
    
    static final int SEQUENCE_LENGTH = 8;
    /**
     * Method to generate a fixed-length zero-padded page sequence number
     * @param order the ordering of a page
     * @return a fixed-length zero-padded page sequence number based on the ordering
     */
    protected String generateSequence(int order) {
        String orderString = Integer.toString(order);
        
        StringBuilder sequenceBuilder = new StringBuilder();
        
        int digitCount = orderString.length();
        for (int i = digitCount; i < SEQUENCE_LENGTH; i++) {
            sequenceBuilder.append('0');
        }
        sequenceBuilder.append(orderString);
        return sequenceBuilder.toString();
        
    }
    /**
     * Method to extract the filename from a ZipEntry name
     * @param entryName name of a ZipEntry
     * @return extracted filename
     */
    protected String extractEntryFilename(String entryName) {
        int lastIndex = entryName.lastIndexOf('/');
        return entryName.substring(lastIndex + 1);
    }
    

    /**
     * Method to read page contents from an InputStream object
     * @param inputStream an InputStream object opened for page contents
     * @return a byte array containing the raw page content
     */
    protected byte[] readPageContentsFromInputStream(InputStream inputStream) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[32767];
        int read = -1;
        try {
            do {
                read = inputStream.read(buffer);
                if (read > 0) {
                    byteArrayOutputStream.write(buffer, 0, read);
                }
            } while (read > 0);
        } catch (IOException e) {
            log.error("IOException while reading from input stream", e);
        }
        
        try {
            byteArrayOutputStream.close();
        } catch (IOException e) {
            log.error("IOException while attempting to close ByteArrayOutputStream()", e);
        }
        return byteArrayOutputStream.toByteArray();
    }
    
    /**
     * Method to update page content in Cassandra
     * @param pageContent a byte array containing the raw page content
     * @param volumeID volumeID of the volume
     * @param pageRecord a PageRecord object containing metadata for the page
     * @param featuredPagesMap a HashMap object containing all features the page has
     * @param volumeCFTemplate a ColumnFamilyTemplate object for the VolumeContents column family in Cassandra
     * @throws HInvalidRequestException thrown from Hector client
     * @throws HTimedOutException thrown from Hector client
     */
    protected void updatePage(byte[] pageContent, String volumeID, PageRecord pageRecord, HashMap<String, List<String>> featuredPagesMap, ColumnFamilyTemplate<String, String> volumeCFTemplate) throws HInvalidRequestException, HTimedOutException {
        
        boolean successful = false;
        int attemptsLeft = maxAttempts;
        long failureDelay = initFailureDelay;

        String pageID = pageRecord.getSequence();
        ColumnFamilyUpdater<String, String> pageUpdater = volumeCFTemplate.createUpdater(volumeID);
        
        pageUpdater.setByteArray(pageID + Constants.CN_CONTENTS_SUFFIX, pageContent);
        
        long byteCount = pageRecord.getByteCount();
        pageUpdater.setLong(pageID + Constants.CN_BYTECOUNT_SUFFIX, byteCount);
        
        int characterCount = pageRecord.getCharacterCount();
        pageUpdater.setInteger(pageID + Constants.CN_CHARACTERCOUNT_SUFFIX, characterCount);
        
        String checksum = pageRecord.getChecksum();
        String checksumType = pageRecord.getChecksumType();
        pageUpdater.setString(pageID + Constants.CN_CHECKSUM_MIDSEC + checksumType, checksum);
        
        String label = pageRecord.getLabel();
        if (label != null) {
            pageUpdater.setString(pageID + Constants.CN_PAGENUMBERLABEL_SUFFIX, label);
        }
        
        int sequence = pageRecord.getOrder();
        pageUpdater.setInteger(pageID + Constants.CN_SEQUENCE_SUFFIX, sequence);
        
        List<String> features = pageRecord.getFeatures();
        if (features != null) {
            for (String feature : features) {
                pageUpdater.setBoolean(pageID + Constants.CN_PAGEFEATURE_MIDSEC + feature, Boolean.TRUE);
                List<String> list = featuredPagesMap.get(feature);
                if (list == null) {
                    list = new ArrayList<String>();
                    featuredPagesMap.put(feature, list);
                }
                list.add(pageID);
            }
        }
        
        do {
            try {
                volumeCFTemplate.update(pageUpdater);
                successful = true;
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to update page. volume: " + volumeID + " page: " + pageID + " Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                    
                } else {
                    log.error("Failed to update page. volume: " + volumeID + " page: " + pageID, e);
                    throw e;
                }
            }
        } while (!successful && attemptsLeft > 0);

    }
    
    /**
     * Method to get the StringKeyIterator of the VolumeContents column family
     * @return a StringKeyIterator object of the VolumeContents column family
     */
    public StringKeyIterator getVolumeContentKeyIterator() {
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        StringKeyIterator keyIterator = new StringKeyIterator(keyspace, volumeContentsCFName);
        return keyIterator;
    }
    /**
     * Method to shutdown the resources used by Hector
     */
    public void shutdown() {
        cluster.getConnectionManager().shutdown();
    }
    
}

