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
# File:  DeltaParser.java
# Description: This class parses delta logs produced by rsync with verbose mode enabled
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.cassandra;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.indiana.d2i.htrc.ingest.Constants.CopyrightEnum;
import gov.loc.repository.pairtree.Pairtree;

/**
 * This class parses delta logs produced by rsync with verbose mode enabled
 * @author Yiming Sun
 *
 */
public class DeltaLogParser {
    
    /**
     * This class holds information on a volume to be updated
     * @author Yiming Sun
     *
     */
    static class VolumeUpdateInfo {
        
        
        protected String volumeID;
        protected String cleanedPartialVolumeID;
        protected String volumePath;

        protected CopyrightEnum copyright;

        /**
         * Constructor
         * @param volumeID volumeID
         * @param cleanedPartialVolumeID volumeID with prefix removed and the rest cleaned by Pairtree
         * @param volumePath path to the volume in Pairtree
         */
        VolumeUpdateInfo(String volumeID, String cleanedPartialVolumeID, String volumePath) {
            this.volumeID = volumeID;
            this.cleanedPartialVolumeID = cleanedPartialVolumeID;
            this.volumePath = volumePath;

            copyright = CopyrightEnum.PUBLIC_DOMAIN;
  
        }
        /**
         * Method to return the volumeID
         * @return the original volumeID
         */
        String getVolumeID() {
            return volumeID;
        }
        /**
         * Method to return the Pairtree path to the volume
         * @return the Pairtree path to the volume
         */
        String getVolumePath() {
            return volumePath;
        }
        /**
         * Method to set the Pairtree path to the volume
         * @param volumePath the Pairtree path to the volume
         */
        void setVolumePath(String volumePath) {
            this.volumePath = volumePath;
        }
        /**
         * Method to return the Pairtree cleaned volumeID with the prefix removed
         * @return the Pairtree cleaned volumeID with the prefix removed
         */
        String getCleanedPartialVoluemID() {
            return cleanedPartialVolumeID;
        }

        /**
         * Method to set the copyright of the volume
         * @param copyright the copyright of the volume
         */
        void setCopyright(CopyrightEnum copyright) {
            this.copyright = copyright;
        }
        /**
         * Method to get the copyright of the volume
         * @return the copyright of the volume
         */
        CopyrightEnum getCopyright() {
            return copyright;
        }
    }
    
    /**
     * This class holds information on a volume to be deleted
     * @author Yiming Sun
     *
     */
    static class VolumeDeletionInfo {
        
        protected String volumeID;

        protected boolean isDeleteVolume;
        protected boolean isDeleteMETS;

        protected CopyrightEnum copyright;
        
        /**
         * Constructor
         * @param volumeID volumeID of the volume to be deleted
         * @param copyright copyright of the volume
         */
        VolumeDeletionInfo(String volumeID, CopyrightEnum copyright) {
            this.volumeID = volumeID;
            this.copyright = copyright;

            this.isDeleteVolume = false;
            this.isDeleteMETS = false;

        }
        
        /**
         * Method to get the volumeID
         * @return the volumeID
         */
        String getVolumeID() {
            return volumeID;
        }
        
        /**
         * Method to set the flag to indicate if the actual volume is to be deleted
         * @param isDeleteVolume flag to indicate if the actual volume is to be deleted
         */
        void setIsDeleteVolume(boolean isDeleteVolume) {
            this.isDeleteVolume = isDeleteVolume;

        }
        
        /**
         * Method to get the flag to indicate if the actual volume is to be deleted
         * @return flag to indicate if the actual volume is to be deleted
         */
        boolean getIsDeleteVolume() {
            return isDeleteVolume;
        }

        /**
         * Method to set the flag to indicate if the METS accompanying the volume is to be deleted
         * @param isDeleteMETS flag to indicate if the METS accompanying the volume is to be deleted
         */
        void setIsDeleteMETS(boolean isDeleteMETS) {
            this.isDeleteMETS = isDeleteMETS;
        }
        
        /**
         * Method to get the flag to indicate if the METS accompanying the volume is to be deleted
         * @return flag to indicate if the METS accompanying the volume is to be deleted
         */
        boolean getIsDeleteMETS() {
            return isDeleteMETS;
        }
        
        /**
         * Method to get the copyright of the volume
         * @return copyright of the volume
         */
        CopyrightEnum getCopyright() {
            return copyright;
        }
        
        /**
         * Method to set the copyright of the volume
         * @param copyright copyright of the volume
         */
        void setCopyright(CopyrightEnum copyright) {
            this.copyright = copyright;
        }
    }

    
    
    private static Logger log = Logger.getLogger(DeltaLogParser.class);
    
    private static final String DEST_ROOT_PREFIX = "DEST_ROOT=";
    private static final int DEST_ROOT_PREFIX_SIZE = DEST_ROOT_PREFIX.length();
    
    private static final String PREFIX_PREFIX = "PREFIX=";
    private static final int PREFIX_PREFIX_SIZE = PREFIX_PREFIX.length();
    
    private static final String COPYRIGHT_PREFIX = "COPYRIGHT=";
    private static final int COPYRIGHT_PREFIX_SIZE = COPYRIGHT_PREFIX.length();
    
    private static final String DELETING_PREFIX = "deleting ";
    private static final int DELETING_PREFIX_SIZE = DELETING_PREFIX.length();
    
    private static final String METS_SUFFIX = ".mets.xml";
    private static final int METS_SUFFIX_LENGTH = METS_SUFFIX.length();
    
    private static final String ZIP_SUFFIX = ".zip";
    private static final int ZIP_SUFFIX_LENGTH = ZIP_SUFFIX.length();
    
    private static final String PAIRTREE_ROOT_SEGMENT = "pairtree_root";
    
    
    private String deltaLogPath;
    
    private HashMap<String, VolumeDeletionInfo> volumeDeletionMap;
    
    private HashMap<String, VolumeUpdateInfo> volumeUpdateMap;
    
    private String destRoot;
    private String prefix;
    private CopyrightEnum copyright;  // XXX: here we assume copyrighted and public domain volumes have separate pairtrees... if they are to be mixed, need a way to determine if a particular volume is copyrighted or public
    
    private Pairtree pairtree;
    
    // this variable is used to tell the code if it has seen an actual rsync line (not the stuff like COPYRIGHT=, DEST_ROOT_PREFIX= added on by the ingest service) from the current dlog file yet.
    // the code is supposed to look at the first actual rsync line and determine if the prefix information is in the path itself (by looking for pairtree_root and then grab the path segment
    // before it).  If so, this prefix information shall override the one got passed in; if not, use the prefix information that was passed in.
    // there will not be a case where a dlog file contains rsync lines from different prefixes because the prefix is just below the root path, so any subbranch from the pairtree assigned to
    // an rsync task can only belong to one prefix, and therefore it is sufficient to only examine the prefix for the very first line.
    private boolean seenALine;
    
    /**
     * Constructor
     * @param deltaLogPath path to a delta log file
     */
    protected DeltaLogParser(String deltaLogPath) {
        this.deltaLogPath = deltaLogPath;
        
        this.volumeDeletionMap = new HashMap<String, VolumeDeletionInfo>();
        this.volumeUpdateMap = new HashMap<String, VolumeUpdateInfo>();
        
        this.pairtree = new Pairtree();
        this.seenALine = false;
        
    }
    
    /**
     * Method to parse the delta log file
     * @throws FileNotFoundException thrown if the delta log file does not exist
     * @throws IOException thrown if reading of the delta log file failed
     */
    protected void parse() throws FileNotFoundException, IOException {
        BufferedReader reader = new BufferedReader(new FileReader(deltaLogPath));
//        String destRoot = null;
        String line = null;
        
        do {
            line = reader.readLine();
            if (log.isTraceEnabled()) log.trace("line " + line);
            
            if (line != null) {
                if (line.startsWith(DELETING_PREFIX)) {
                    if (log.isTraceEnabled()) log.trace("got a delete line");
                    // handling the delete case. 
                    parseDeleteLine(line);
                } else if (line.endsWith(ZIP_SUFFIX)) {
                    if (log.isTraceEnabled()) log.trace("got a volume file line");
                    if (!seenALine) {
                        parsePrefixFromLine(line);
                        seenALine = true;
                    }
                    parseNewVolumeLine(line, ZIP_SUFFIX, ZIP_SUFFIX_LENGTH);
//                    parseNewZipLine(line);
                
                } else if (line.endsWith(METS_SUFFIX)) {
                    if (log.isTraceEnabled()) log.trace("got a METS line");
                    if (!seenALine) {
                        parsePrefixFromLine(line);
                        seenALine = true;
                    }
                    parseNewVolumeLine(line, METS_SUFFIX, METS_SUFFIX_LENGTH);
                    // handling the METS.  parse METS xml and check with cassandra for each page.
//                    parseNewMETSLine(line);
                    
                } else if (line.startsWith(DEST_ROOT_PREFIX)) {
                    if (log.isTraceEnabled()) log.trace("got DEST_ROOT_PREFIX");
                    destRoot = line.substring(DEST_ROOT_PREFIX_SIZE);
                } else if (line.startsWith(PREFIX_PREFIX)) {
                    if (log.isTraceEnabled()) log.trace("got PREFIX_PREFIX");
                    prefix = line.substring(PREFIX_PREFIX_SIZE);
                } else if (line.startsWith(COPYRIGHT_PREFIX)) {
                    if (log.isTraceEnabled()) log.trace("got COPYRIGHT_PREFIX");
                    String copyrightString = line.substring(COPYRIGHT_PREFIX_SIZE);
                    copyright = CopyrightEnum.valueOf(copyrightString);
                }
                
            }
        } while (line != null);
        
        reader.close();
        
        log.info("Finished parsing " + deltaLogPath);
    }
    
    /**
     * Method to parse out prefix from a line
     * @param line a line containing the prefix
     */
    private void parsePrefixFromLine(String line) {
        int index = line.indexOf(PAIRTREE_ROOT_SEGMENT);
        if (index > -1) {
            String partialLine = line.substring(0, index);
            int lastSlashIndex = partialLine.lastIndexOf('/');
            if (lastSlashIndex > -1) {
                partialLine = partialLine.substring(0, lastSlashIndex);
                lastSlashIndex = partialLine.lastIndexOf('/');
                prefix = partialLine.substring(lastSlashIndex + 1);
                if (log.isTraceEnabled()) log.trace("prefix from line: " + prefix);
            }
            
        }
        
    }
    
    /**
     * Method to parse a line about deleting a volume
     * @param line a line about deleting a volume
     */
    private void parseDeleteLine(String line) {
        // the line is deleting dd/yy/zz/.../yyzz...wwuu/????? where dd is the repeated last portion of destRoot
        int index = line.indexOf("/");
        String headlessLine = line.substring(index + 1);
        // headlessLine = yy/zz/.../yyzz...wwuu/?????
        if (log.isTraceEnabled()) log.trace("headlessLine: " + headlessLine);
        

        if (headlessLine.endsWith(ZIP_SUFFIX)) {
            int lastIndex = headlessLine.lastIndexOf("/");
            String volumeZipFilename = headlessLine.substring(lastIndex + 1);
            // volumeZipFilename = yy+zz...wwuu.zip
            if (log.isTraceEnabled()) log.trace("volumeZipFilename: " + volumeZipFilename);
            String cleanedPartialVolumeID = volumeZipFilename.substring(0, volumeZipFilename.length() - ZIP_SUFFIX_LENGTH);
            // cleanedPartialVolumeID = yy+zz...wwuu
            if (log.isTraceEnabled()) log.trace("cleanedPartialVolumeID: " + cleanedPartialVolumeID);
            String uncleanedPartialVolumeID = pairtree.uncleanId(cleanedPartialVolumeID);
            // uncleanedPartialVolumeID = yy:zz...wwuu
            if (log.isTraceEnabled()) log.trace("uncleanedPartialVolumeID: " + uncleanedPartialVolumeID);
            String volumeID = prefix + uncleanedPartialVolumeID;
            // volumeID = uc2.yy:zz...wwuu
            if (log.isTraceEnabled()) log.trace("volumeID: " + volumeID);
            VolumeDeletionInfo volumeDeletionInfo = volumeDeletionMap.get(volumeID);
            if (volumeDeletionInfo == null) {
                volumeDeletionInfo = new VolumeDeletionInfo(volumeID, copyright);
                volumeDeletionMap.put(volumeID, volumeDeletionInfo);
            }
            volumeDeletionInfo.setIsDeleteVolume(true);
            
        } else if (headlessLine.endsWith(METS_SUFFIX)) {
            // delete the mets?? makes no sense... maybe delete the whole volume?
            int lastIndex = headlessLine.lastIndexOf("/");
            String metsFilename = headlessLine.substring(lastIndex + 1);
            // metsFilename = yy+zz...wwuu.mets.xml
            if (log.isTraceEnabled()) log.trace("metsFilename: " + metsFilename);
            
            String cleanedPartialVolumeID = metsFilename.substring(0, metsFilename.length() - METS_SUFFIX_LENGTH);
            // cleanedPartialVolumeID = yy+zz...wwuu
            if (log.isTraceEnabled()) log.trace("cleanedPartialVolumeID: " + cleanedPartialVolumeID);
            
            String uncleanedPartialVolumeID = pairtree.uncleanId(cleanedPartialVolumeID);
            // uncleanedPartialVolumeID = yy:zz...wwuu
            if (log.isTraceEnabled()) log.trace("uncleanedPartialVolumeID: " + uncleanedPartialVolumeID);
            
            
            String volumeID = prefix + uncleanedPartialVolumeID;
            // volumeID = uc2.yy:zz...wwuu
            if (log.isTraceEnabled()) log.trace("volumeID: " + volumeID);
            
            VolumeDeletionInfo volumeDeletionInfo = volumeDeletionMap.get(volumeID);
            if (volumeDeletionInfo == null) {
                volumeDeletionInfo = new VolumeDeletionInfo(volumeID, copyright);
                volumeDeletionMap.put(volumeID, volumeDeletionInfo);
            }
            volumeDeletionInfo.setIsDeleteMETS(true);
            

        }
        
    }
    
    /**
     * Method to parse a line about a new/update volume
     * @param line a line about a new/update volume
     * @param suffix filename suffix to match
     * @param suffixLength length of the filename suffix
     */
    private void parseNewVolumeLine(String line, String suffix, int suffixLength) {
        // the line is escaped x+/yy/.../x+yy...wwuu/x+yy...wwuu.<suffix>
        int lastSlashIndex = line.lastIndexOf('/');
        
        String cleanedPartialVolumeID = line.substring(lastSlashIndex + 1, line.length() - suffixLength);
        if (log.isTraceEnabled()) log.trace("cleanedPartialVolumeID: " + cleanedPartialVolumeID);
        
        VolumeUpdateInfo volumeUpdateInfo = volumeUpdateMap.get(cleanedPartialVolumeID);
        if (volumeUpdateInfo == null) {
            
            String partialVolumeID = pairtree.uncleanId(cleanedPartialVolumeID);
            // partialVolumeID = x:yy...wwuu
            if (log.isTraceEnabled()) log.trace("partialVolumeID: " + partialVolumeID);
            
            String volumeID = prefix + partialVolumeID;
            // volumeID = <prefix>.x:yy...wwuu
            if (log.isTraceEnabled()) log.trace("volumeID: " + volumeID);
            
            String volumePath = destRoot + line.substring(0, lastSlashIndex + 1);
            // volumePath = x+/yy/.../x+yy...wwuu/
            if (log.isTraceEnabled()) log.trace("volumePath: " + volumePath);
            
            volumeUpdateInfo = new VolumeUpdateInfo(volumeID, cleanedPartialVolumeID, volumePath);
            volumeUpdateInfo.setCopyright(copyright);
            volumeUpdateMap.put(cleanedPartialVolumeID, volumeUpdateInfo);
            
        } else {
            if (log.isTraceEnabled()) log.trace("cleanedPartialVolumeID already encountered: " + cleanedPartialVolumeID);
        }
    }

    /**
     * Method to convert a HashMap object to an ordered List containing the values from the HashMap 
     * @param hashMap a HashMap object
     * @return an ordered List
     */
    private <K> List<K> convertMapValuesToList(HashMap<String, K> hashMap) {
        List<K> list = null;
        
        if (hashMap != null) {
            Set<Entry<String, K>> entrySet = hashMap.entrySet();
            if (log.isTraceEnabled()) log.trace("Set size " + entrySet.size());
            list = new ArrayList<K>(entrySet.size());
            for (Entry<String, K> entry : entrySet) {
                list.add(entry.getValue());
            }
        }
        
        return list;
    }
    
    /**
     * Method to get the List of VolumeUpdateInfo objects
     * @return
     */
    protected List<VolumeUpdateInfo> getVolumeUpdateList() {
        return convertMapValuesToList(volumeUpdateMap);
    }
    
    /**
     * Method to get the List of VolumeDeletionInfo objects
     * @return
     */
    protected List<VolumeDeletionInfo> getVolumeDeletionList() {
        return convertMapValuesToList(volumeDeletionMap);
    }
    
}

