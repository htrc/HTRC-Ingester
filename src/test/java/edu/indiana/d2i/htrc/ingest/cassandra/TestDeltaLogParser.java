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
# File:  TestDeltaLogParser.java
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
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.indiana.d2i.htrc.ingest.cassandra.DeltaLogParser.VolumeUpdateInfo;

/**
 * @author Yiming Sun
 *
 */
public class TestDeltaLogParser {
    private File deltalogFile = null;
    
    private static final String PREFIX = "test.";
    private static final String DEST_ROOT = "/no/such/dest/test/pairtree_root/no/su/";
    private static final String COPYRIGHT = "PUBLIC_DOMAIN";
    
    @Before
    public void setupDeltalogFile() throws IOException {
        Random random = new Random();
        int filename = random.nextInt();
        deltalogFile = new File(Integer.toString(filename));
        FileWriter writer = new FileWriter(deltalogFile);
        writer.write("DEST_ROOT=" + DEST_ROOT);
        writer.write(System.getProperty("line.separator"));
        writer.write("PREFIX=" + PREFIX);
        writer.write(System.getProperty("line.separator"));
        writer.write("COPYRIGHT=" + COPYRIGHT);
        writer.write(System.getProperty("line.separator"));
        writer.write(System.getProperty("line.separator"));
        writer.write("receiving incremental file list");
        writer.write(System.getProperty("line.separator"));
        writer.write("./");
        writer.write(System.getProperty("line.separator"));
        writer.write("ch/");
        writer.write(System.getProperty("line.separator"));
        writer.write("ch/vo/");
        writer.write(System.getProperty("line.separator"));
        writer.write("ch/vo/lu/");
        writer.write(System.getProperty("line.separator"));
        writer.write("ch/vo/lu/me/");
        writer.write(System.getProperty("line.separator"));
        writer.write("ch/vo/lu/me/nosuchvolume/");
        writer.write(System.getProperty("line.separator"));
        writer.write("ch/vo/lu/me/nosuchvolume/nosuchvolume.mets.xml");
        writer.write(System.getProperty("line.separator"));
        writer.write("ch/vo/lu/me/nosuchvolume/nosuchvolume.zip");
        writer.write(System.getProperty("line.separator"));
        writer.write(System.getProperty("line.separator"));
        writer.write("sent 12345 bytes  received 54321 bytes  3600.12 bytes/sec");
        writer.write(System.getProperty("line.separator"));
        writer.write("total size is 65535  speedup is 1.00");
        writer.write(System.getProperty("line.separator"));
        writer.flush();
        writer.close();
    }
    
    @After
    public void teardownDeltalogFile() {
        if (deltalogFile.exists()) {
            boolean deleted = deltalogFile.delete();
            if (!deleted) {
                System.err.println("failed to delete temp file " + deltalogFile.getAbsolutePath());
                deltalogFile.deleteOnExit();
            }
        }
    }
    
    @Test
    public void testDeltaLogParserUpdateList() {
        DeltaLogParser parser = new DeltaLogParser(deltalogFile.getPath());
        List<VolumeUpdateInfo> volumeUpdateList = null;
        try {
            parser.parse();
            volumeUpdateList = parser.getVolumeUpdateList();
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }
        
        VolumeUpdateInfo volumeUpdateInfo = volumeUpdateList.remove(0);
        
        Assert.assertEquals("volumeIDs mismatch", "test.nosuchvolume", volumeUpdateInfo.getVolumeID());
    }

}

