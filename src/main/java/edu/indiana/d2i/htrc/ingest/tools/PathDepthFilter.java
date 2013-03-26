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
# File:  PathLevelFilter.java
# Description: This class filters out any paths listed by the linux tree command that is shallower than a specified depth 
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.StringTokenizer;

/**
 * This class filters out any paths listed by the linux tree command that is shallower than a specified depth 
 * @author Yiming Sun
 *
 */
public class PathDepthFilter {
    private final int level;
    private final String delimiter;
    private final String inputFilename;
    private final String outputFilename;
    
    
    /**
     * Constructor
     * @param inputFilename name of the input file containing paths to be filtered
     * @param outputFilename name of the output file to which filtered paths are written
     * @param level the threshold depth
     * @param delimiter path delimiter
     */
    public PathDepthFilter(String inputFilename, String outputFilename, int level, String delimiter) {
        this.level = level;
        this.delimiter = delimiter;
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
    }
    
    /**
     * Method to filter the paths
     * @throws Exception thrown if anything failed
     */
    public void filter() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(inputFilename));
        FileWriter writer = new FileWriter(outputFilename);
        
        String line = null;
        
        final String lineSeparator = System.getProperty("line.separator");
        
        do {
            line = reader.readLine();
            if (line != null) {
                StringTokenizer tokenizer = new StringTokenizer(line, delimiter);
                int tokenCount = tokenizer.countTokens();
                if (tokenCount >= level) {
                    writer.write(line);
                    writer.write(lineSeparator);
                }
            }
        } while (line != null);
            
        reader.close();
        writer.close();
        
    }

    /**
     * Method to print the usage
     */
    public static void printUsage() {
        System.out.println("PathLevelFilter <inputFilename> <outputFilename> <level> <delimiter>");
    }
    /**
     * main method
     * @param args arguments passed in by the system
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            printUsage();
            System.exit(-1);
        }
        
        PathDepthFilter filter = new PathDepthFilter(args[0], args[1], Integer.parseInt(args[2]), args[3]);
        filter.filter();

    }

    
}

