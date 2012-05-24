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
# File:  TestPropertyReader.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Yiming Sun
 *
 */
public class TestPropertyReader {
    
    private static Random random = null;
    private File propertyFile = null;
    
    private static final String NOT_A_PROPERTY_NAME = "# NOT_A_PROPERTY";
    private static final String NOT_A_PROPERTY_VALUE = "not a property";
    
    private static final String PROPERTY_ALPHA_NAME = "PROPERTY_ALPHA";
    private static final String PROPERTY_ALPHA_VALUE = "alpha";
    
    private static final String PROPERTY_BETA_NAME = "PROPERTY_BETA";
    private static final String PROPERTY_BETA_VALUE = "beta";
    
    private static final String PROPERTY_CHARLIE_NAME = "PROPERTY_CHARLIE";
    private static final String PROPERTY_CHARLIE_VALUE = "charlie";
    
    @BeforeClass
    public static void constructRandom() {
        random = new Random();
    }
    
    @AfterClass
    public static void destroyRandom() {
        random = null;
    }
    
    @Before
    public void setupPropertyFile() throws IOException {
        int filename = random.nextInt();
        
        propertyFile = new File(Integer.toString(filename));
        
        FileWriter fileWriter = new FileWriter(propertyFile);
        fileWriter.write(NOT_A_PROPERTY_NAME + "=" + NOT_A_PROPERTY_VALUE);
        fileWriter.write(System.getProperty("line.separator"));
        fileWriter.write(PROPERTY_ALPHA_NAME + "=" + PROPERTY_ALPHA_VALUE);
        fileWriter.write(System.getProperty("line.separator"));
        fileWriter.write(PROPERTY_BETA_NAME + "=" + PROPERTY_BETA_VALUE);
        fileWriter.write(System.getProperty("line.separator"));
        fileWriter.flush();
        fileWriter.close();
        fileWriter = null;
        System.setProperty("PROPERTIES_LOCATION", propertyFile.getPath());
    }
    
    @After
    public void teardownPropertyFile() {
        if (propertyFile.exists()) {
            boolean deleted = propertyFile.delete();
            if (!deleted) {
                System.err.println("Failed to delete temp file in teardownPropertyFile " + propertyFile.getAbsolutePath());
            }
        }
    }
    
    @Test
    public void testGetInstance() {
        PropertyReader reader = PropertyReader.getInstance();
        Assert.assertNotNull("should not be null", reader);
    }
    
    @Test
    public void testReadProperty() {
        PropertyReader reader = PropertyReader.getInstance();
        String property = reader.getProperty(PROPERTY_ALPHA_NAME);
        Assert.assertEquals("property value mismatch", property, PROPERTY_ALPHA_VALUE);
    }

    @Test
    public void testReadInvalidProperty() {
        PropertyReader reader = PropertyReader.getInstance();
        String property = reader.getProperty(NOT_A_PROPERTY_NAME);
        Assert.assertNull("should be null", property);
    }
    
    @Test
    public void testSetProperty() {
        PropertyReader reader = PropertyReader.getInstance();
        reader.setProperty(PROPERTY_CHARLIE_NAME, PROPERTY_CHARLIE_VALUE);
        String property = reader.getProperty(PROPERTY_CHARLIE_NAME);
        Assert.assertEquals("property value mismatch", property, PROPERTY_CHARLIE_VALUE);
    }
}

