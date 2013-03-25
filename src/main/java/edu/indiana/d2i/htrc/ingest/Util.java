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
# File:  Util.java
# Description: This class contains several utility methods 
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
 * This class contains several utility methods
 * @author Yiming Sun
 *
 */
public class Util {
    private static Logger log = Logger.getLogger(Util.class);
    
    /**
     * Method to add a trailing slash to a path if the path does not end with one already
     * @param path a path
     * @return the path guaranteed to have a trailing slash
     */
    public static String addTrailingSlash(String path) {
        String newPath = null;
        if (path != null) {
            newPath = path.endsWith("/") ? path : path + "/";
        }
        return newPath;
    }
    
    /**
     * Method to calculate MD5 checksum
     * @param contents contents whose MD5 checksum to be computed
     * @return a String representation of the MD5 checksum computed, in lower-case hexadecimal format
     */
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
    
    /**
     * Method to calculate the modulo hash of the MD5 checksum of the specified contents
     * @param contents contents whose MD5 checksum to be computed and to be modulo hashed
     * @param hashSpace the hash space as a BigInteger object. If the hash space is n, the possible modulo hash values are between 0 and (n - 1)
     * @return the modulo hash value in the specified hash space calculated from the MD5 checksum of the specified contents
     */
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
    
    /**
     * Method to compute checksum using the specified checksum algorithm
     * @param contents contents whose checksum is to be computed
     * @param algorithm name of the checksum computation algorithm
     * @return String representation of the computed checksum in hexadecimal format
     * @throws NoSuchAlgorithmException thrown if the specified algorithm is not known
     */
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
    
    /**
     * Method to convert a byte to its 2-digit zero-padded hexadecimal representation
     * @param bite a byte
     * @return a 2-digit zero-padded hexdecimal representation of the byte
     */
    public static char[] byteToHex(byte bite) {
        char[] hexByteChars = new char[2];
        hexByteChars[0] = hexDigit[(int)(bite & 0xFF) / 16];
        hexByteChars[1] = hexDigit[(int)(bite & 0xFF) % 16];
        return hexByteChars;
    }
    
    /**
     * Method to replace the starting portion of a String with the specified replacement if the starting portion matches the specified target
     * @param original the original String to be checked and possibly replaced
     * @param target the starting portion to be matched
     * @param replacement the replacement for the starting portion if the starting portion matches the target
     * @return either the original String if the starting portion does not match the target, the a modified String whose starting portion is replaced by the replacement
     */
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
    
    /**
     * Method to extract a prefix from a line representing a path to a Pairtree
     * @param line a line representing a path to a Pairtree
     * @return a prefix
     */
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

    /**
     * Method to pad a String with the specified character from the starting point until the String length exceeds the specified length
     * @param string the String to be padded
     * @param pad the padding character
     * @param fixedLength the length of the padded String
     * @return the padded String
     */
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
    
    /**
     * Method to convert the Strings <code>true</code> and <code>yes</code> to boolean <code>true</code>, and everything else to boolean <code>false</code>, regardless of cases
     * @param propertyValue a String to be converted to boolean
     * @return <code>true</code> if the property value is either <code>true</code> or <code>yes</code> regardless of case, or <code>false</code> if the property value is anything else
     */
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

