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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
        boolean withPage = args.length > 2;
        
        HashMap<String, Set<String>> idMap = new HashMap<String, Set<String>>();
        
        BufferedReader reader = new BufferedReader(new FileReader(errorLogFile));
//        FileWriter writer = new FileWriter(volumeListFile);
        
        String currentVolumeID = null;
        String line = null;
        do {
            line = reader.readLine();
            if (line != null) {
                StringTokenizer tokenizer = new StringTokenizer(line);
                while (tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();
                    if ("Volume:".equalsIgnoreCase(token)) {
                        currentVolumeID = tokenizer.nextToken();
                        Set<String> set = idMap.get(currentVolumeID);
                        if (set == null) {
                            set = new HashSet<String>();
                            idMap.put(currentVolumeID, set);
                        }
                        
                        if (!withPage) {
                            break;
                        }
                    } else if ("Page:".equalsIgnoreCase(token)) {
                        String pageSequence = tokenizer.nextToken();
                        Set<String> set = idMap.get(currentVolumeID);
                        set.add(pageSequence);
                    }
                }
            }
        } while (line != null);
        
//        writer.close();
        reader.close();
        
        FileWriter writer = new FileWriter(volumeListFile);
        Set<String> keySet = idMap.keySet();
        List<String> volumeIDList = new ArrayList<String> (keySet.size());
        volumeIDList.addAll(keySet);
        Collections.sort(volumeIDList);
        for (String volumeID : volumeIDList) {
            writer.write(volumeID);
            if (withPage) {
                Set<String> set = idMap.get(volumeID);
                if (set != null && !set.isEmpty()) {
                    List<String> pageSequenceList = new ArrayList<String>(set);
                    Collections.sort(pageSequenceList);

                    writer.write(" ");
                    boolean first = true;
                    for (String pageSequence : pageSequenceList) {
                        if (!first) {
                            writer.write(",");
                        }
                        writer.write(pageSequence);
                        first = false;
                    }
                }
            }
            writer.write("\n");
        }
        writer.close();

    }

    private static void printUsage() {
        System.out.println("Usage: ExtractVIDFromErrorLogs <error log file>  <volume list file> [--with-pages]");
        System.out.println("Assumes volume IDs are preceeded with 'Volume: ' and page sequence numbers with 'Page: '");
    }
}

