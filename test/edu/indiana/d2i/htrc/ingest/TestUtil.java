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
# File:  TestUtil.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Yiming Sun
 *
 */
public class TestUtil {
    
    @Test
    public void testAddTrailingSlashShouldAdd() {
        String string = "my/string/without/trailing/slash";
        String expected = string + "/";
        String addTrailingSlash = Util.addTrailingSlash(string);
        Assert.assertEquals("result not expected", addTrailingSlash, expected);
    }
    
    @Test
    public void testAddTrailingSlashShouldNotAdd() {
        String string = "my/string/with/trailing/slash/";
        String addTrailingSlash = Util.addTrailingSlash(string);
        Assert.assertEquals("result not expected", addTrailingSlash, string);
    }

    @Test
    public void testCalculateChecksumMd5Empty() {
        String checksum = "d41d8cd98f00b204e9800998ecf8427e";
        byte[] contents = new byte[0];
        String result = null;
        try {
            result = Util.calculateChecksum(contents, "md5");
        } catch (NoSuchAlgorithmException e) {
            
        }
        
        Assert.assertEquals("checksum mismatch", result, checksum);
        
    }

    @Test
    public void testCalculateChecksumMd5NonEmpty() {
        String checksum = "f95f3ff0e2552f54f533075e24b8b3d7";
        byte[] contents = null; //"a long string to run".getBytes("utf-8");
        String result = null;
        try {
            contents = "a long string to run\n".getBytes("utf-8");
            result = Util.calculateChecksum(contents, "md5");
//            System.out.println(result);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace(System.err);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace(System.err);
        }
        
        Assert.assertEquals("checksum mismatch", result, checksum);
        
    }
    
    @Test
    public void testCalculateChecksumSha1Empty() {
        String checksum = "da39a3ee5e6b4b0d3255bfef95601890afd80709";
        byte[] contents = new byte[0];
        String result = null;
        try {
            result = Util.calculateChecksum(contents, "sha1");
        } catch (NoSuchAlgorithmException e) {
            
        }
        
        Assert.assertEquals("checksum mismatch", result, checksum);
    }

    @Test
    public void testCalculateChecksumSha1NonEmpty() {
        String checksum = "b3bbf6028e9b58af2089dff988bf7d65a8b87fff";
        byte[] contents = null; //"a long string to run".getBytes("utf-8");
        String result = null;
        try {
            contents = "a long string to run\n".getBytes("utf-8");
            result = Util.calculateChecksum(contents, "sha1");
//            System.out.println(result);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace(System.err);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace(System.err);
        }
        
        Assert.assertEquals("checksum mismatch", result, checksum);
    }
}

