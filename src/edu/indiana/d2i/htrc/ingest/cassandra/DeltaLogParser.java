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
# File:  DeltaParser.java
# Description:  
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
 * @author Yiming Sun
 *
 */
public class DeltaLogParser {
    
    
    static class VolumeUpdateInfo {
        
        
        protected String volumeID;
        protected String cleanedPartialVolumeID;
        protected String volumePath;

        protected CopyrightEnum copyright;
//        protected String volumeMETSPath;
        

        VolumeUpdateInfo(String volumeID, String cleanedPartialVolumeID, String volumePath) {
            this.volumeID = volumeID;
            this.cleanedPartialVolumeID = cleanedPartialVolumeID;
            this.volumePath = volumePath;

            copyright = CopyrightEnum.PUBLIC_DOMAIN;
  
        }
        String getVolumeID() {
            return volumeID;
        }
        String getVolumePath() {
            return volumePath;
        }
        void setVolumePath(String volumePath) {
            this.volumePath = volumePath;
        }
        String getCleanedPartialVoluemID() {
            return cleanedPartialVolumeID;
        }

        void setCopyright(CopyrightEnum copyright) {
            this.copyright = copyright;
        }
        CopyrightEnum getCopyright() {
            return copyright;
        }
//        void setMETSPath(String volumeMETSPath) {
//            this.volumeMETSPath = volumeMETSPath;
//        }
//        String getMETSPath() {
//            return volumeMETSPath;
//        }

    }
    static class VolumeDeletionInfo {
        
        protected String volumeID;

        protected boolean isDeleteVolume;
        protected boolean isDeleteMETS;

        protected CopyrightEnum copyright;
        
        VolumeDeletionInfo(String volumeID, CopyrightEnum copyright) {
            this.volumeID = volumeID;
            this.copyright = copyright;

            this.isDeleteVolume = false;
            this.isDeleteMETS = false;

        }
        

        String getVolumeID() {
            return volumeID;
        }
        void setIsDeleteVolume(boolean isDeleteVolume) {
            this.isDeleteVolume = isDeleteVolume;

        }
        boolean getIsDeleteVolume() {
            return isDeleteVolume;
        }

        void setIsDeleteMETS(boolean isDeleteMETS) {
            this.isDeleteMETS = isDeleteMETS;
        }
        boolean getIsDeleteMETS() {
            return isDeleteMETS;
        }
        CopyrightEnum getCopyright() {
            return copyright;
        }
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
    
//    private static final String TXT_SUFFIX = ".txt";
//    private static final int TXT_SUFFIX_LENGTH = TXT_SUFFIX.length();
    
    private static final String METS_SUFFIX = ".mets.xml";
    private static final int METS_SUFFIX_LENGTH = METS_SUFFIX.length();
    
    private static final String ZIP_SUFFIX = ".zip";
    private static final int ZIP_SUFFIX_LENGTH = ZIP_SUFFIX.length();
    
    private static final String PAIRTREE_ROOT_SEGMENT = "pairtree_root";
    
//    private static final String PAGE_TXT_FILE_PATTERN = "([^/]{2}/)+([^/]{1}/)?([^/]{3,}/)+\\d{8}\\.txt";
    
    
    
    private String deltaLogPath;
    
    private HashMap<String, VolumeDeletionInfo> volumeDeletionMap;
    
    private HashMap<String, VolumeUpdateInfo> volumeUpdateMap;
    
    private String destRoot;
    private String prefix;
    private CopyrightEnum copyright;  // XXX: here we assume copyrighted and public domain volumes have separate pairtrees... if they are to be mixed, need a way to determine if a particular volume is copyrighted or public
    
    private Pairtree pairtree;
    
    // this variable is used to tell the code if it has seen an actual rsync line (not the stuff like COPYRIGHT=, DEST_ROOT_PREFIX= added on my the ingest service) from the current dlog file yet.
    // the code is supposed to look at the first actual rsync line and determine if the prefix information is in the path itself (by looking for pairtree_root and then grab the path segment
    // before it).  If so, this prefix information shall override the one got passed in; if not, use the prefix information that was passed in.
    // there will not be a case where a dlog file contains rsync lines from different prefixes because the prefix is just below the root path, so any subbranch from the pairtree assigned to
    // an rsync task can only belong to one prefix, and therefore it is sufficient to only examine the prefix for the very first line.
    private boolean seenALine;
    
    protected DeltaLogParser(String deltaLogPath) {
        this.deltaLogPath = deltaLogPath;
        
        this.volumeDeletionMap = new HashMap<String, VolumeDeletionInfo>();
        this.volumeUpdateMap = new HashMap<String, VolumeUpdateInfo>();
        
        this.pairtree = new Pairtree();
        this.seenALine = false;
        
    }
    
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
    
//    private void parseNewZipLine(String line) {
//        // the line is escaped x+/yy/.../xxyy...wwuu/x+yy...wwuu.zip
//        int lastSlashIndex = line.lastIndexOf("/");
//        String zipFilename = line.substring(lastSlashIndex + 1);
//        
//        if (log.isTraceEnabled()) log.trace("zipFilename: " + zipFilename);
//        
//        String escapedPartialVolumeID = zipFilename.substring(0, zipFilename.length() - ZIP_SUFFIX_LENGTH);
//        // escapedPartialVolumeID = x+yy...wwuu
//        if (log.isTraceEnabled()) log.trace("escapedPartialVolumeID: " + escapedPartialVolumeID);
//        
//        String partialVolumeID = pairtree.uncleanId(escapedPartialVolumeID);
//        // partialVolumeID = x:yy...wwuu
//        if (log.isTraceEnabled()) log.trace("partialVolumeID: " + partialVolumeID);
//        
//        String volumeID = prefix + partialVolumeID;
//        // volumeID = <prefix>.x:yy...wwuu
//        if (log.isTraceEnabled()) log.trace("volumeID: " + volumeID);
//        
//        String volumeZipPath = destRoot + line;
//        if (log.isTraceEnabled()) log.trace("volumeZipPath: " + volumeZipPath);
//
//        VolumeUpdateInfo volumeUpdateInfo = volumeUpdateMap.get(volumeID);
//        
//        if (volumeUpdateInfo == null) {
//            volumeUpdateInfo = new VolumeUpdateInfo(volumeID, volumeZipPath);
//            volumeUpdateInfo.setCopyright(copyright);
//            volumeUpdateMap.put(volumeID, volumeUpdateInfo);
//        } else {
//            volumeUpdateInfo.setVolumePath(volumeZipPath);
//        }
//    }
    
//    private void parseNewTxtLine(String line) {
//        // the line is xx/yy/zz/.../xxyyzz...wwuu/00000001.txt
//        int lastSlashIndex = line.lastIndexOf("/");
//        String pageFilename = line.substring(lastSlashIndex + 1);
//        // pageFilename = 00000001.txt
//        if (log.isTraceEnabled()) log.trace("pageFilename: " + pageFilename);
//                
//        String partialVolumePath = line.substring(0, lastSlashIndex);
//        // partialVolumePath = xx/yy/zz/.../xxyyzz...wwuu
//        if (log.isTraceEnabled()) log.trace("partialVolumePath: " + partialVolumePath);
//                
//        lastSlashIndex = partialVolumePath.lastIndexOf("/");
//        
//        String volumeID = prefix + partialVolumePath.substring(lastSlashIndex + 1);
//        // volumeID = inu.xxyyzz...wwuu
//        if (log.isTraceEnabled()) log.trace("volumeID: " + volumeID);
//        
//        VolumeUpdateInfo volumeUpdateInfo = volumeUpdateMap.get(volumeID);
//        if (volumeUpdateInfo == null) {
//            String volumePath = destRoot + partialVolumePath + "/";
//            volumeUpdateInfo = new VolumeUpdateInfo(volumeID, volumePath);
//            volumeUpdateInfo.setCopyright(copyright);
//            volumeUpdateMap.put(volumeID, volumeUpdateInfo);
//        }
//        volumeUpdateInfo.setVolumePath(destRoot + partialVolumePath + "/");
//        PageUpdateInfo pageUpdateInfo = new PageUpdateInfo(pageFilename);
//        
//        volumeUpdateInfo.addPageUpdateInfo(pageUpdateInfo);
//       
//    }
    
    
//    protected void parseNewMETSLine(String line) {
//        // the line is yy/+z/zz/.../yy+zzz...wwuu/yy+zzz...wwuu.mets.xml
//        
//        int lastIndex = line.lastIndexOf("/");
//        String metsFilename = line.substring(lastIndex + 1);
//        // metsFilename = yy+zzz...wwuu.mets.xml
//        if (log.isTraceEnabled()) log.trace("metsFilename: " + metsFilename);
//        
//        String cleanedPartialVolumeID = metsFilename.substring(0, metsFilename.length() - METS_SUFFIX_LENGTH);
//        // cleanedPartialVolumeID = yy+zzz...wwuu
//        if (log.isTraceEnabled()) log.trace("cleanedPartialVolumeID: " + cleanedPartialVolumeID);
//        
//        String uncleanedPartialVolumeID = pairtree.uncleanId(cleanedPartialVolumeID);
//        // uncleanedPartialVolumeID = yy:zzz...wwuu
//        if (log.isTraceEnabled()) log.trace("uncleanedPartialVolumeID: " + uncleanedPartialVolumeID);
//        
//        String volumeID = prefix + uncleanedPartialVolumeID;
//        // volumeID = uc2.yy+zzz...wwuu
//        if (log.isTraceEnabled()) log.trace("volumeID: " + volumeID);
//        
//        VolumeUpdateInfo volumeUpdateInfo = volumeUpdateMap.get(volumeID);
//        if (volumeUpdateInfo == null) {
//            String partialVolumePath = line.substring(0, lastIndex + 1);
//            // partialVolumePath = yy/zz/.../yyzz...wwuu/
//            if (log.isTraceEnabled()) log.trace("partialVolumePath: " + partialVolumePath);
//            String volumePath = destRoot + partialVolumePath;
//            if (log.isTraceEnabled()) log.trace("volumePath: " + volumePath);
//            volumeUpdateInfo = new VolumeUpdateInfo(volumeID, volumePath);
//            volumeUpdateInfo.setCopyright(copyright);
//            volumeUpdateMap.put(volumeID, volumeUpdateInfo);
//        }
//        
//        String metsPath = destRoot + line;
//        if (log.isTraceEnabled()) log.trace("metsPath: " + metsPath);
//        volumeUpdateInfo.setMETSPath(metsPath);
//    }
    
    
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
    
    protected List<VolumeUpdateInfo> getVolumeUpdateList() {
        return convertMapValuesToList(volumeUpdateMap);
    }
    
    protected List<VolumeDeletionInfo> getVolumeDeletionList() {
        return convertMapValuesToList(volumeDeletionMap);
    }
    
}

