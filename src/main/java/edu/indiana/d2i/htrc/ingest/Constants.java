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
# File:  Constants.java
# Description: This class contains constants used by the code  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest;

/**
 * This class contains constants used by the code
 * @author Yiming Sun
 *
 */
public class Constants {
    
    public static final String PK_LOG4J_PROPERTIES_PATH = "LOG4J_PROPERTIES_PATH";
    public static final String PK_DELTA_LOG_ROOT = "DELTA_LOG_ROOT";
    
    public static final String PK_PERFORM_NOSQL_PUSH = "PERFORM_NOSQL_PUSH";
    
    public static final String PK_DELTA_PROCESSOR_THREAD_COUNT = "DELTA_PROCESSOR_THREAD_COUNT";
    
    public static final String PK_CASSANDRA_HOST = "CASSANDRA_HOST";
    public static final String PK_CASSANDRA_CLUSTER_NAME = "CASSANDRA_CLUSTER_NAME";
    public static final String PK_CASSANDRA_KEYSPACE_NAME = "CASSANDRA_KEYSPACE_NAME";

    public static final String PK_CASSANDRA_COLLECTIONS_CF_NAME = "CASSANDRA_COLLECTIONS_CF_NAME";
    public static final String PK_CASSANDRA_COLLECTION_NAMES_CF_NAME = "CASSANDRA_COLLECTION_NAMES_CF_NAME";
    public static final String PK_CASSANDRA_VOLUMECONTENTS_CF_NAME = "CASSANDRA_VOLUMECONTENTS_CF_NAME";
    
    
    public static final String PK_RSYNC_USERNAME = "RSYNC_USERNAME";
    public static final String PK_RSYNC_PASSWORD = "RSYNC_PASSWORD";
    public static final String PK_RSYNC_SOURCE_HOST = "RSYNC_SOURCE_HOST";
    public static final String PK_RSYNC_SEPARATOR_TOKEN = "RSYNC_SEPARATOR_TOKEN";
    public static final String PK_RSYNC_DRY_RUN = "RSYNC_DRY_RUN";
    
    public static final String PK_RSYNC_THREAD_COUNT = "RSYNC_THREAD_COUNT";
    
    public static final String PK_INGEST_HASH_SPACE = "INGEST_HASH_SPACE";
    public static final String PK_INGEST_MY_HASH_KEY = "INGEST_MY_HASH_KEY";
    
    public static final String PK_FORCE_INGEST_LIST_DIR = "FORCE_INGEST_LIST_DIR";
    public static final String PK_PERFORM_RSYNC = "PERFORM_RSYNC";
    
    public static final String PK_UPDATE_FAILURE_MAX_ATTEMPTS = "UPDATE_FAILURE_MAX_ATTEMPTS";
    public static final String PK_UPDATE_FAILURE_BACK_OFF_INIT_DELAY_IN_MS = "UPDATE_FAILURE_BACK_OFF_INIT_DELAY_IN_MS";
    public static final String PK_UPDATE_FAILURE_BACK_OFF_MAX_DELAY_IN_MS = "UPDATE_FAILURE_BACK_OFF_MAX_DELAY_IN_MS";
    
    public static final String PK_PERSIST_WAKEUP_INTERVAL_IN_MS = "PERSIST_WAKEUP_INTERVAL_IN_MS";
    public static final String PK_PERSIST_INGEST_INTERVAL_IN_HOUR = "PERSIST_INGEST_INTERVAL_IN_HOUR";
    public static final String PK_PERSIST_CONTROL_FILE_PATH = "PERSIST_CONTROL_FILE_PATH";
    
    public static final String PK_RSYNC_TREE_SOURCE_PATH = "RSYNC_TREE_SOURCE_PATH";
    public static final String PK_RSYNC_DEST_ROOT = "RSYNC_DEST_ROOT";
    
    public static final String PK_SOURCE_CLEANSE_TARGET = "SOURCE_CLEANSE_TARGET";
    public static final String PK_SOURCE_CLEANSE_REPLACEMENT = "SOURCE_CLEANSE_REPLACEMENT";
    
    public static final String PK_RSYNC_EXTRA_FILES_COUNT = "RSYNC_EXTRA_FILES_COUNT";
    public static final String PKN_RSYNC_EXTRA_FILE_PATH = "RSYNC_EXTRA_FILE_PATH";
    
    public static final String PK_PERFORM_VERIFICATION = "PERFORM_VERIFICATION";
//    public static final String PK_VERIFICATION_SPACE = "VERIFICATION_SPACE";
    public static final String PK_VERIFICATION_PROPORTION = "VERFICATION_PROPORTION";
    public static final String PK_DELTALOG_VOLUMEID_EXTRACTOR_THREAD_COUNT = "DELTALOG_VOLUMEID_EXTRACTOR_THREAD_COUNT";
    public static final String PK_VERIFICATION_THREAD_COUNT = "VERIFICATION_THREAD_COUNT";
    public static final String PK_VERIFICATION_LEVEL_PAGECOUNT = "VERIFICATION_LEVEL_PAGECOUNT";
    public static final String PK_VERIFICATION_LEVEL_PAGESIZE = "VERIFICATION_LEVEL_PAGESIZE";
    public static final String PK_VERIFICATION_LEVEL_PAGECHECKSUM = "VERIFICATION_LEVEL_PAGECHECKSUM";
    
    public static final String CN_VOLUME_PREFIX = "volume.";
    public static final String CN_VOLUME_METS = CN_VOLUME_PREFIX + "METS";
    public static final String CN_VOLUME_COPYRIGHT = CN_VOLUME_PREFIX + "copyright";
    public static final String CN_VOLUME_PAGECOUNT = CN_VOLUME_PREFIX + "pageCount";
    public static final String CN_VOLUME_PAGEFEATURE_PREFIX = CN_VOLUME_PREFIX + "pageFeature.";

    public static final String CN_CONTENTS_SUFFIX = ".contents";
    public static final String CN_BYTECOUNT_SUFFIX = ".byteCount";
    public static final String CN_CHARACTERCOUNT_SUFFIX = ".characterCount";
    public static final String CN_PAGENUMBERLABEL_SUFFIX = ".pageNumberLabel";
    public static final String CN_SEQUENCE_SUFFIX = ".sequence";
    public static final String CN_CHECKSUM_MD5_SUFFIX = ".checksum.MD5";
    public static final String CN_CHECKSUM_SHA1_SUFFIX = ".checksum.SHA1";
    public static final String CN_PAGEFEATURE_MIDSEC = ".pageFeature.";
    public static final String CN_CHECKSUM_MIDSEC = ".checksum.";
    

    public static enum CopyrightEnum {
        PUBLIC_DOMAIN,
        IN_COPYRIGHT;
    }
}

