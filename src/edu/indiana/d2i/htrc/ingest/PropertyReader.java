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
# Project: rsync_test
# File:  PropertyReader.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Yiming Sun
 *
 */
public class PropertyReader {
    public static final String PROPERTIES_LOCATION = "PROPERTIES_LOCATION";
    
    private static final PropertyReader reader = new PropertyReader();
    
    private static Properties properties;
    
    private PropertyReader() {
        String propertyLocation = System.getProperty(PROPERTIES_LOCATION);
        System.out.println("propertyLocation = " + propertyLocation);
        properties = null;
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(propertyLocation);
            properties = new Properties();
            properties.load(fileInputStream);
            
        } catch (Exception e) {
            System.out.println("Path to properties file is invalid: " + propertyLocation);
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                }
            }
        }
        
    }

    public static PropertyReader getInstance() {
        return reader;
    }
    
    public String getProperty(String fieldName) { // throws Exception {
        if (properties.containsKey(fieldName)) {
            return properties.getProperty(fieldName);
        } else {
            return null;
        }
    }
    
    public void setProperty(String fieldName, String fieldValue) {
        properties.put(fieldName, fieldValue);
    }
}

