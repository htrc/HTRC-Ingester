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
# File:  Util.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

/**
 * @author Yiming Sun
 *
 */
public class Util {
    private static Logger log = Logger.getLogger(Util.class);
    
    public static String addTrailingSlash(String path) {
        String newPath = null;
        if (path != null) {
            newPath = path.endsWith("/") ? path : path + "/";
        }
        return newPath;
    }
    
    
    public static String calculateMD5(String contents) {
        String md5 = "";
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(contents.getBytes());
            byte[] md5sum = digest.digest();
            BigInteger bigInt = new BigInteger(1, md5sum);
            md5 = bigInt.toString(16).toLowerCase();
        } catch (Exception e) {
            log.error("Failed to calculate MD5 ", e);
        }
        return md5;
    }
    
    public static int hashMD5Checksum(String contents, BigInteger hashSpace) {
        int hash = -1;
        
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(contents.getBytes());
            byte[] md5sum = digest.digest();
            BigInteger bigInt = new BigInteger(1, md5sum);
            BigInteger mod = bigInt.mod(hashSpace);
            hash = mod.intValue();
            
        } catch (Exception e) {
            log.error("Failed to hash MD5 checksum", e);
        }
        
        return hash;
        
    }
    
    
    public static String calculateChecksum(byte[] contents, String algorithm) throws NoSuchAlgorithmException {
        StringBuilder checksumBuilder = new StringBuilder();
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        digest.update(contents);
        byte[] checksumBytes = digest.digest();
        for (byte bite : checksumBytes) {
            checksumBuilder.append(byteToHex(bite));
        }
        return checksumBuilder.toString();
    }
    
    static final char[] hexDigit = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    public static char[] byteToHex(byte bite) {
        char[] hexByteChars = new char[2];
        hexByteChars[0] = hexDigit[(int)(bite & 0xFF) / 16];
        hexByteChars[1] = hexDigit[(int)(bite & 0xFF) % 16];
        return hexByteChars;
    }
    
    public static String cleanseSource(String original, String target, String replacement) {
        StringBuilder builder = new StringBuilder(original);
        if (target != null && !target.equals("")) {
            if (original.startsWith(target)) {
                builder.delete(0, target.length());
            }
        }
        
        if (replacement != null && !replacement.equals("")) {
            builder.insert(0, replacement);
        }
        
        return builder.toString();
    }

    protected static final String PAIRTREE_ROOT_SEGMENT = "/pairtree_root";
    public static String extractPrefix(String line) {
        StringBuilder builder = new StringBuilder(line);
        int index = builder.indexOf(PAIRTREE_ROOT_SEGMENT);
        if (index > -1) {
            builder.delete(index, line.length());
            index = builder.indexOf("/");
            if (index > -1) {
                builder.delete(0, index + 1);
            }
        }
        return builder.toString();
    }

    public static String prepadString(String string, char pad, int fixedLength) {
//        StringBuilder stringBuilder = new StringBuilder(string);
        StringBuilder padBuilder = new StringBuilder();
        int difference = fixedLength - string.length();
        for (int i = 0; i < difference; i++) {
            padBuilder.append(pad);
        }
        padBuilder.append(string);
        return padBuilder.toString();
    }
    
    public static boolean getBooleanProperty(String propertyValue) {
        boolean result = false;
        
        if (propertyValue != null) {
            if ("true".equalsIgnoreCase(propertyValue) || "yes".equalsIgnoreCase(propertyValue)) {
                result = true;
            }
        }
        return result;
    }
}

