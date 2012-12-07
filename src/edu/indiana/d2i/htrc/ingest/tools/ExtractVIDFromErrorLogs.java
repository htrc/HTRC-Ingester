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
# File:  ExtractVIDFromErrorLogs.java
# Description:  
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
 * @author Yiming Sun
 * 
 * this tool extracts the volume IDs from error logs
 *
 */
public class ExtractVIDFromErrorLogs {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            printUsage();
            System.exit(-1);
        }
        
        String errorLogFile = args[0];
        String volumeListFile = args[1];
        
        BufferedReader reader = new BufferedReader(new FileReader(errorLogFile));
        FileWriter writer = new FileWriter(volumeListFile);
        
        String line = null;
        do {
            line = reader.readLine();
            if (line != null) {
                StringTokenizer tokenizer = new StringTokenizer(line);
                while (tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();
                    if ("Volume:".equals(token)) {
                        String volumeID = tokenizer.nextToken();
                        writer.write(volumeID);
                        writer.write("\n");
                        break;
                    }
                }
            }
        } while (line != null);
        
        writer.close();
        reader.close();
        

    }

    private static void printUsage() {
        System.out.println("Usage: ExtractVIDFromErrorLogs <error log file>  <volume list file>");
    }
}

