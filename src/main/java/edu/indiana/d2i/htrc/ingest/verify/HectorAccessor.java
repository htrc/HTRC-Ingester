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
# Description: This class provides high level methods for operations against Cassandra 
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.verify;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HTimedOutException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.PropertyReader;

/**
 * This class provides high level methods for operations against Cassandra
 * @author Yiming Sun
 *
 */
public class HectorAccessor {
    
    /**
     * This class encapsulates metadata for page size
     * @author Yiming Sun
     *
     */
    static class PageSizeMetadata {
        private final long byteCount;
        private final int characterCount;
        
        /**
         * Constructor
         * @param byteCount page size as number of bytes
         * @param characterCount page size as number of characters
         */
        PageSizeMetadata(long byteCount, int characterCount) {
            this.byteCount = byteCount;
            this.characterCount = characterCount;
        }
        /**
         * Method to get page size as number of bytes
         * @return page size as number of bytes
         */
        long getByteCount() {
            return byteCount;
        }
        /**
         * MEthod to get page size as number of characters
         * @return page size as number of characters
         */
        int getCharacterCount() {
            return characterCount;
        }
    }
    
    /**
     * This class encapsulates checksum metadata for page
     * @author Yiming Sun
     *
     */
    static class PageChecksumMetadata {
        private final String checksum;
        private final String checksumType;
        
        /**
         * Constructor
         * @param checksum String representation of page content checksum
         * @param checksumType type of checksum
         */
        PageChecksumMetadata(String checksum, String checksumType) {
            this.checksum = checksum;
            this.checksumType = checksumType;
        }
        /**
         * Method to get checksum
         * @return
         */
        String getChecksum() {
            return checksum;
        }
        /**
         * Method to get checksum type
         * @return
         */
        String getChecksumType() {
            return checksumType;
        }
    }
    
    /**
     * This class is for storing page content 
     * @author Yiming Sun
     *
     */
    static class PageContents {
        private byte[] contentsBytes;
        private long byteCount;
        private int characterCount;
        private String checksum;
        private String checksumType;
        
        /**
         * Constructor
         * @param contentsBytes page content
         */
        PageContents(byte[] contentsBytes) {
            this.contentsBytes = contentsBytes;
            byteCount = -1;
            characterCount = -1;
            checksum = null;
            checksumType = null;
        }
        
        /**
         * Method to get page content as raw byte array
         * @return page content as raw byte array
         */
        byte[] getContentsBytes() {
            return contentsBytes;
        }
        
        /**
         * Method to get page size as number of bytes
         * @return page size as number of bytes
         */
        long getByteCount() {
            return byteCount;
        }
        /**
         * Method to set page size as number of bytes
         * @param byteCount page size as number of bytes
         */
        void setByteCount(long byteCount) {
            this.byteCount = byteCount;
        }
        /**
         * Method to get page size as number of characters
         * @return page size as number of characters
         */
        int getCharacterCount() {
            return characterCount;
        }
        /**
         * Method to set page size as number of characters
         * @param characterCount page size as number of characters
         */
        void setCharacterCount(int characterCount) {
            this.characterCount = characterCount;
        }
        /**
         * Method to set page checksum
         * @param checksum String representation of page content checksum
         * @param checksumType type of checksum
         */
        void setChecksum(String checksum, String checksumType) {
            this.checksum = checksum;
            this.checksumType = checksumType;
        }
        /**
         * Method to get page checksum
         * @return String representation of page content checksum
         */
        String getChecksum() {
            return checksum;
        }
        /**
         * Method to get page checksum type
         * @return page checksum type
         */
        String getChecksumType() {
            return checksumType;
        }
    }
    
    private static Logger log = Logger.getLogger(HectorAccessor.class);
    
    protected static final HectorAccessor instance = new HectorAccessor();
    
    protected final Cluster cluster;
    protected final String keyspaceName;
    protected final String volumeContentsCFName;
    protected final String collectionsCFName;
    protected final String collectionNamesCFName;
    protected final int maxAttempts;
    protected final long initFailureDelay;
    protected final long maxFailureDelay;
    
    /**
     * Method to get the singleton instance
     * @return the singleton instance
     */
    public static HectorAccessor getInstance() {
        return instance;
    }
    
    /**
     * Private constructor
     */
    private HectorAccessor() {
        PropertyReader propertyReader = PropertyReader.getInstance();
        String log4PropertiesPath = propertyReader.getProperty(Constants.PK_LOG4J_PROPERTIES_PATH);
        PropertyConfigurator.configure(log4PropertiesPath);

        String cassandraAddress = propertyReader.getProperty(Constants.PK_CASSANDRA_HOST);
        
        cluster = HFactory.getOrCreateCluster(propertyReader.getProperty(Constants.PK_CASSANDRA_CLUSTER_NAME), new CassandraHostConfigurator(cassandraAddress));
        keyspaceName = propertyReader.getProperty(Constants.PK_CASSANDRA_KEYSPACE_NAME);
        collectionsCFName = propertyReader.getProperty(Constants.PK_CASSANDRA_COLLECTIONS_CF_NAME);
        collectionNamesCFName = propertyReader.getProperty(Constants.PK_CASSANDRA_COLLECTION_NAMES_CF_NAME);
        volumeContentsCFName = propertyReader.getProperty(Constants.PK_CASSANDRA_VOLUMECONTENTS_CF_NAME);
        
        maxAttempts = Integer.parseInt(propertyReader.getProperty(Constants.PK_UPDATE_FAILURE_MAX_ATTEMPTS));
        initFailureDelay = Long.parseLong(propertyReader.getProperty(Constants.PK_UPDATE_FAILURE_BACK_OFF_INIT_DELAY_IN_MS));
        maxFailureDelay = Long.parseLong(propertyReader.getProperty(Constants.PK_UPDATE_FAILURE_BACK_OFF_MAX_DELAY_IN_MS));
        
    }
    
    /**
     * Method to get the number of pages a volume has
     * @param volumeID volumeID of the volume
     * @return number of pages a volume has
     * @throws VerificationException thrown if the volume or a page does not verify
     * @throws HTimedOutException thrown from Hector client
     */
    public int retrieveVolumePageCount(String volumeID) throws VerificationException, HTimedOutException {
        int pageCount = -1;
        
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        Serializer<String> stringSerializer = new StringSerializer();
        Serializer<Integer> integerSerializer = new IntegerSerializer();
        
        boolean successful = false;
        int attemptsLeft = maxAttempts;
        long failureDelay = initFailureDelay;
        

        ColumnQuery<String, String, Integer> columnQuery = HFactory.createColumnQuery(keyspace, stringSerializer, stringSerializer, integerSerializer);
        columnQuery.setColumnFamily(volumeContentsCFName);
        columnQuery.setKey(volumeID);
        columnQuery.setName(Constants.CN_VOLUME_PAGECOUNT);
        
        
        do {
            try {
                QueryResult<HColumn<String, Integer>> queryResult = columnQuery.execute();
                successful = true;
                
                if (queryResult != null) {
                    HColumn<String, Integer> hColumn = queryResult.get();
                    if (hColumn != null) {
                        Integer value = hColumn.getValue();
                        if (value != null) {
                            pageCount = value.intValue();
                        } else {
                            throw new VerificationException("Null page count value", volumeID);
                        }
                    } else {
                        throw new VerificationException("No page count column", volumeID);
                    }
                } else {
                    throw new VerificationException("No page count query result", volumeID);
                }
                
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to retrieve volume metadata. volume: " + volumeID + " Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                } else {
                    log.error("Failed to retrieve volume metadata: " + volumeID, e);
                    throw e;
                }
            }
        } while (!successful && attemptsLeft > 0);
        
        return pageCount;
    }
    
    
    
    /**
     * Method to get the content of a page
     * @param volumeID volumeID of the volume
     * @param pageSequence page sequence number of the page to retrieve
     * @return content of the page
     * @throws VerificationException thrown if the page content does not verify
     * @throws UnsupportedEncodingException thrown if the page content is not UTF-8 encoded
     * @throws HTimedOutException thrown from Hector client
     */
    public byte[] retrievePageContents(String volumeID, String pageSequence) throws VerificationException, UnsupportedEncodingException, HTimedOutException {
        byte[] contents = null;
        
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        Serializer<String> stringSerializer = new StringSerializer();
        Serializer<byte[]> bytesArraySerializer = new BytesArraySerializer();

        ColumnQuery<String, String, byte[]> columnQuery = HFactory.createColumnQuery(keyspace, stringSerializer, stringSerializer, bytesArraySerializer);
        columnQuery.setColumnFamily(volumeContentsCFName);
        columnQuery.setKey(volumeID);
        columnQuery.setName(pageSequence + Constants.CN_CONTENTS_SUFFIX);
        
        boolean successful = false;
        int attemptsLeft = maxAttempts;
        long failureDelay = initFailureDelay;

        do {
            try {
                QueryResult<HColumn<String, byte[]>> queryResult = columnQuery.execute();
                successful = true;
                if (queryResult != null) {
                    HColumn<String, byte[]> hColumn = queryResult.get();
                    if (hColumn != null) {
                         contents = hColumn.getValue();
                    } else {
                        throw new VerificationException("No page contents HColumn", volumeID, pageSequence);
                    }
                } else {
                    throw new VerificationException("No page contents query result", volumeID, pageSequence);
                }
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to retrieve page contents. volume: " + volumeID + " pageSequence: " + pageSequence + " Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                } else {
                    log.error("Failed to retrieve page contents: " + volumeID + " " + pageSequence, e);
                    throw e;
                }

            }
        } while (!successful && attemptsLeft > 0);
        
        return contents;
    }
    
    /**
     * Method to get page size metadata of a page
     * @param volumeID volumeID of the volume
     * @param pageSequence page sequence number of the page
     * @return page size metadata
     * @throws VerificationException thrown if the page does not verify
     * @throws HTimedOutException thrown from Hector client
     */
    public PageSizeMetadata retrievePageSizeMetadata(String volumeID, String pageSequence) throws VerificationException, HTimedOutException {
        PageSizeMetadata pageSizeMetadata = null;
        
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        Serializer<String> stringSerializer = new StringSerializer();
        Serializer<Long> longSerializer = new LongSerializer();
        Serializer<Integer> integerSerializer = new IntegerSerializer();
        Serializer<byte[]> bytesArraySerializer = new BytesArraySerializer();
        
        boolean successful = false;
        int attemptsLeft = maxAttempts;
        long failureDelay = initFailureDelay;

        SliceQuery<String, String, byte[]> sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, bytesArraySerializer);
        sliceQuery.setColumnFamily(volumeContentsCFName);
        sliceQuery.setKey(volumeID);
        
        String columnNameByteCount = pageSequence + Constants.CN_BYTECOUNT_SUFFIX;
        String columnNameCharacterCount = pageSequence + Constants.CN_CHARACTERCOUNT_SUFFIX;

        sliceQuery.setColumnNames(columnNameByteCount, columnNameCharacterCount);

        long byteCount = -1;
        int characterCount = -1;
        
        do {
            try {
                QueryResult<ColumnSlice<String, byte[]>> queryResult = sliceQuery.execute();
                successful = true;
                if (queryResult != null) {
                    ColumnSlice<String, byte[]> columnSlice = queryResult.get();
                    if (columnSlice != null) {
                        List<HColumn<String, byte[]>> columns = columnSlice.getColumns();
                        if (columns != null && !columns.isEmpty()) {
                            for (HColumn<String, byte[]> hColumn : columns) {
                                
                                String name = hColumn.getName();
                                byte[] value = hColumn.getValue();
                                
                                if (value != null) {
                                    if (columnNameByteCount.equals(name)) {
                                        byteCount = longSerializer.fromBytes(value);
                                    } else if (columnNameCharacterCount.equals(name)) {
                                        characterCount = integerSerializer.fromBytes(value);
                                    }
                                } else {
                                    throw new VerificationException("Null page size column value for column " + name, volumeID, pageSequence);
                                }
                            }
                            pageSizeMetadata = new PageSizeMetadata(byteCount, characterCount);
                        } else {
                            throw new VerificationException("No page size HColumn list", volumeID, pageSequence);
                        }
                    } else {
                        throw new VerificationException("No page size column slice", volumeID, pageSequence);
                    }
                } else {
                    throw new VerificationException("No page size query result", volumeID, pageSequence);
                }
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to retrieve page size metadata. volume: " + volumeID + " pageSequence: " + pageSequence + " Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                } else {
                    log.error("Failed to retrieve page size metadata: " + volumeID + " " + pageSequence, e);
                    throw e;
                }

            }
            
        } while (!successful && attemptsLeft > 0);

        return pageSizeMetadata;
    }
    
    /**
     * Method to get page checksum metadata
     * @param volumeID volumeID of the volume
     * @param pageSequence page sequence number of the page
     * @return page checksum metadata of the page
     * @throws VerificationException thrown if the page does not verify
     * @throws HTimedOutException thrown from Hector client
     */
    public PageChecksumMetadata retrievePageChecksumMetadata(String volumeID, String pageSequence) throws VerificationException, HTimedOutException {
        PageChecksumMetadata pageChecksumMetadata = null;
        
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        Serializer<String> stringSerializer = new StringSerializer();

        boolean successful = false;
        int attemptsLeft = maxAttempts;
        long failureDelay = initFailureDelay;

        SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        sliceQuery.setColumnFamily(volumeContentsCFName);
        sliceQuery.setKey(volumeID);

        String columnNameChecksumMD5 = pageSequence + Constants.CN_CHECKSUM_MD5_SUFFIX;
        String columnNameChecksumSHA1 = pageSequence + Constants.CN_CHECKSUM_SHA1_SUFFIX;

        sliceQuery.setColumnNames(columnNameChecksumMD5, columnNameChecksumSHA1);

        do {
            try {
                QueryResult<ColumnSlice<String, String>> queryResult = sliceQuery.execute();
                successful = true;
                if (queryResult != null) {
                    ColumnSlice<String, String> columnSlice = queryResult.get();
                    if (columnSlice != null) {
                        List<HColumn<String, String>> columns = columnSlice.getColumns();
                        if (columns != null && !columns.isEmpty()) {
                            for (HColumn<String, String> hColumn : columns) {
                                
                                String name = hColumn.getName();
                                String value = hColumn.getValue();
                                
                                if (columnNameChecksumMD5.equals(name)) {
                                    pageChecksumMetadata = new PageChecksumMetadata(value, "MD5");
                                    break;
                                } else if (columnNameChecksumSHA1.equals(name)) {
                                    pageChecksumMetadata = new PageChecksumMetadata(value, "SHA1");
                                    break;
                                } else {
                                    int prefixSize = (pageSequence + Constants.CN_CHECKSUM_MIDSEC).length();
                                    pageChecksumMetadata = new PageChecksumMetadata(value, name.substring(prefixSize));
                                }
                            }

                        } else {
                            throw new VerificationException("No page checksum HColumn list", volumeID, pageSequence);
                        }
                    } else {
                        throw new VerificationException("No page checksum column slice", volumeID, pageSequence);
                    }
                } else {
                    throw new VerificationException("No page checksum query result", volumeID, pageSequence);
                }
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to retrieve page checksum metadata. volume: " + volumeID + " pageSequence: " + pageSequence + " Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                } else {
                    log.error("Failed to retrieve page checksum metadata: " + volumeID + " " + pageSequence, e);
                    throw e;
                }

            }
            
        } while (!successful && attemptsLeft > 0);

        return pageChecksumMetadata;
        
    }
    
    /**
     * Method to get all volumeIDs
     * @return a List of volumeIDs
     * @throws HTimedOutException thrown from Hector client
     */
    public List<String> getAllVolumeIDs() throws HTimedOutException {
        List<String> volumeIDs = null;
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        Serializer<String> stringSerializer = new StringSerializer();
        
        boolean successful = false;
        int attemptsLeft = maxAttempts;
        long failureDelay = initFailureDelay;

        ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspace, collectionsCFName, stringSerializer, stringSerializer);
        
        do {
            try {
                ColumnFamilyResult<String, String> result = columnFamilyTemplate.queryColumns("ALL");
                successful = true;
                Collection<String> columnNames = result.getColumnNames();
                volumeIDs = new ArrayList<String>(columnNames);
                
            } catch (HTimedOutException e) {
                if (attemptsLeft > 0) {
                    log.warn("Unable to retrieve all volume IDs. Attempts left: " + attemptsLeft, e);
                    attemptsLeft--;
                    
                    try {
                        Thread.sleep(failureDelay);
                    } catch (InterruptedException ie) {
                        log.warn("Interrupted while backing off on HTimedOutException", ie);
                    }
                    
                    failureDelay = (failureDelay * 2) > maxFailureDelay ? maxFailureDelay : (failureDelay * 2);
                   
                } else {
                    log.error("Failed to retrieve all volume IDs.", e);
                    throw e;
                }
            }
        } while (!successful && attemptsLeft > 0);
        
        return volumeIDs;
        
    }
    
    
    /**
     * Method to shutdown and reclaim resources used by Hector client
     */
    public void shutdown() {
        cluster.getConnectionManager().shutdown();
    }
    
}

