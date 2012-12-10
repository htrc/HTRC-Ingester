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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: HTRC-Ingester
# File:  ReingestToolBridge.java
# Description:
#   This is not part of the standard ingest service, but a utility/tool that
#  can be used to re-ingest volumes that failed to be ingested correctly 
#  during the standard ingest process.  Since it reuses parts of the standard
#  ingest code, I have decided to place it in this package so I won't need to
#  change the scope of other code.  This code here does the heavy lifting
#  while a light-weight entry point exists in the tools package. 
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
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.hector.api.exceptions.HTimedOutException;
import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.Constants.CopyrightEnum;
import edu.indiana.d2i.htrc.ingest.PropertyReader;
import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogParser.VolumeDeletionInfo;
import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogParser.VolumeUpdateInfo;
import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogProcessor.VolumeRecord;
import edu.indiana.d2i.htrc.ingest.verify.VerificationException;
import edu.indiana.d2i.htrc.ingest.verify.Verifier;
import edu.indiana.d2i.htrc.ingest.verify.Verifier.VerificationLevelEnum;
import gov.loc.repository.pairtree.Pairtree;

/**
 * @author Yiming Sun
 *
 */
public class HectorToolBridge {
    
    protected static class SyncVerifier extends Verifier {

        /**
         * @param jobQueue
         * @param verificationLevel
         */
        protected SyncVerifier(VerificationLevelEnum verificationLevel) {
            super(null, verificationLevel);
        }
        
        public void verify(String volumeID) {
            try {

                int pageCount = hectorAccessor.retrieveVolumePageCount(volumeID);
                verifyPages(volumeID, pageCount);
                System.out.println("volume " + volumeID + " verified at level " + verificationLevel.toString());
                
            } catch (VerificationException e) {
                e.printStackTrace(System.err);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace(System.err);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace(System.err);
            } catch (HTimedOutException e) {
                e.printStackTrace(System.err);
            }
            

        }
        
    }
    
//    protected List<String> volumeList;
    protected final HectorManager hectorManager;
    
    public HectorToolBridge() {
//        this.volumeList = volumeList;
        this.hectorManager = HectorManager.getInstance();
    }
    
    public void reingest(List<String> volumeList) {
        SyncVerifier verifier = new SyncVerifier(VerificationLevelEnum.VERIFY_PAGE_CHECKSUM);
        
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
//        HectorManager hectorManager = HectorManager.getInstance();
        Pairtree pairtree = new Pairtree();
        PropertyReader reader = PropertyReader.getInstance();
        String destRoot = reader.getProperty(Constants.PK_RSYNC_DEST_ROOT);
        if (!destRoot.endsWith("/")) {
            destRoot = destRoot + "/";
        }
        
        for (String volumeID : volumeList) {
            
            deleteVolumeFromCassandra(volumeID, hectorManager);
            
            String prefix = getPrefix(volumeID);
            String headlessVolumeID = getHeadlessVolumeID(volumeID);
            String headlessCleanedID = pairtree.cleanId(headlessVolumeID);
            List<String> segmentList = getSegmentList(headlessCleanedID);
            
            StringBuilder parentPathBuilder = new StringBuilder(destRoot);
            parentPathBuilder.append(prefix).append("/").append("pairtree_root").append("/");
            for (String segment : segmentList) {
                parentPathBuilder.append(segment).append("/");
            }
            parentPathBuilder.append(headlessCleanedID).append("/");
            
            File metsFile = new File(parentPathBuilder.toString(), headlessCleanedID + ".mets.xml");
            File volumeZipFile = new File(parentPathBuilder.toString(), headlessCleanedID + ".zip");
            
            if (!metsFile.exists()) {
                System.out.println("*** volume " + volumeID + " METS missing. Cannot reingest");
            } else if (!volumeZipFile.exists()) {
                System.out.println("*** volume " + volumeID + " ZIP missing. Cannot reingest");
            } else {
                VolumeUpdateInfo volumeUpdateInfo = new VolumeUpdateInfo(volumeID, headlessCleanedID, parentPathBuilder.toString());
                volumeUpdateInfo.setCopyright(CopyrightEnum.PUBLIC_DOMAIN);
                updateVolumeToCassandra(metsFile, volumeID, volumeUpdateInfo, xmlInputFactory, hectorManager);
                verifier.verify(volumeID);
            }
            
            
        }
        
//        hectorManager.shutdown();
        
    }
    
    public KeyIterator<String> getKeyIterator() {
        KeyIterator<String> keyIterator = null;
        
        keyIterator = hectorManager.getVolumeContentKeyIterator();
        
        return keyIterator;
    }
    
    public void shutdown() {
        hectorManager.shutdown();
    }
    
    protected void deleteVolumeFromCassandra(String volumeID, HectorManager hectorManager) {
        VolumeDeletionInfo volumeDeletionInfo = new VolumeDeletionInfo(volumeID, CopyrightEnum.PUBLIC_DOMAIN);
        volumeDeletionInfo.setIsDeleteVolume(true);
        List<VolumeDeletionInfo> deleteList = new ArrayList<VolumeDeletionInfo>();
        deleteList.add(volumeDeletionInfo);
        hectorManager.delete(deleteList);
        System.out.println("volume " + volumeID + " deleted from Cassandra");
    }
    
    protected void updateVolumeToCassandra(File metsFile, String volumeID, VolumeUpdateInfo volumeUpdateInfo, XMLInputFactory xmlInputFactory, HectorManager hectorManager) {
        VolumeRecord volumeRecord = new VolumeRecord(volumeID);
        volumeRecord.setCopyright(volumeUpdateInfo.getCopyright());
        METSParser metsParser = new METSParser(metsFile, volumeRecord, xmlInputFactory);
        
        try {
            metsParser.parse();
            volumeRecord = metsParser.getVolumeRecord();
            
            hectorManager.update(volumeUpdateInfo, volumeRecord);
            
            System.out.println("volume " + volumeID + " reingested into Cassandra");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        } catch (IOException e) {
            e.printStackTrace(System.out);
        } catch(XMLStreamException e) {
            e.printStackTrace(System.out);
        }
        
    }
    
    protected String getPrefix(String volumeID) {
        String prefix = volumeID;
        int index = volumeID.indexOf('.');
        prefix = volumeID.substring(0, index);
        return prefix;
    }
    
    protected String getHeadlessVolumeID(String volumeID) {
        String headlessVolumeID = volumeID;
        int index = volumeID.indexOf('.');
        headlessVolumeID = volumeID.substring(index + 1);
        return headlessVolumeID;
    }
    
    protected List<String> getSegmentList(String cleanedID) {
        List<String> segmentList = new LinkedList<String>();
        int count = cleanedID.length() / 2;
        for (int i = 0; i < count; i++) {
            StringBuilder builder = new StringBuilder();
            builder.append(cleanedID.charAt(i * 2)).append(cleanedID.charAt(i * 2 + 1));
            segmentList.add(builder.toString());
        }
        
        if (cleanedID.length() % 2 == 1) {
            segmentList.add(cleanedID.substring(cleanedID.length() - 1));
        }
        return segmentList;
    }
    
}

