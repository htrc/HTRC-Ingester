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
# File:  TestMETSParser.java
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
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogProcessor.VolumeRecord;
import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogProcessor.VolumeRecord.PageRecord;



/**
 * @author Yiming Sun
 *
 */
public class TestMETSParser {
    private VolumeRecord volumeRecord;
    private XMLInputFactory xmlInputFactory;
    private File metsFile;
    
    public static final String TEST_VOLUME_ID = "yale.39002034644600";
    //'nnc2.ark:/13960/t7zk5gs46'
    
    @Before
    public void setup() {
        volumeRecord = new VolumeRecord(TEST_VOLUME_ID);
        xmlInputFactory = XMLInputFactory.newInstance();
        metsFile = new File("src/test/resources/39002034644600.mets.xml");
    }
    
    @Test
    public void testParsing() {
        METSParser metsParser = new METSParser(metsFile, volumeRecord, xmlInputFactory);
        
        try {
            metsParser.parse();
            VolumeRecord volumeRecord2 = metsParser.getVolumeRecord();
            
            Set<String> pageFilenameSet = volumeRecord2.getPageFilenameSet();
            
            for (String filename : pageFilenameSet) {
                System.out.println(filename);
                PageRecord pageRecordByFilename = volumeRecord2.getPageRecordByFilename(filename);
                Assert.assertNotNull("page record not found by filename " + filename, pageRecordByFilename);
                long byteCount = pageRecordByFilename.getByteCount();
                System.out.println("byteCount: " + byteCount);
                String sequence = pageRecordByFilename.getSequence();
                System.out.println("sequence: " + sequence);
            }
            
            Assert.assertEquals("page filename set incorrect", pageFilenameSet.size(), volumeRecord.getPageCount());
            
            System.out.println("****************************");
            Set<String> pageIDSet = volumeRecord2.getPageIDSet();

            for (String pageID : pageIDSet) {
                System.out.println(pageID);
                PageRecord pageRecordByID = volumeRecord2.getPageRecordByID(pageID);
                Assert.assertNotNull("page record not found by ID " + pageID, pageRecordByID);
                long byteCount = pageRecordByID.getByteCount();
                System.out.println("byteCount: " + byteCount);
                String sequence = pageRecordByID.getSequence();
                System.out.println("sequence: " + sequence);
            }

            Assert.assertEquals("page ID set incorrect", pageIDSet.size(), volumeRecord.getPageCount());
            
            

        } catch (FileNotFoundException e) {
            e.printStackTrace(System.err);
            Assert.assertTrue("test mets file not found", false);
        } catch (IOException e) {
            e.printStackTrace(System.err);
            Assert.assertTrue("IO Exception parsing mets file", false);
        } catch (XMLStreamException e) {
            e.printStackTrace(System.err);
            Assert.assertTrue("xml stream exception parsing mets file", false);
        }
        
        
    }
}

