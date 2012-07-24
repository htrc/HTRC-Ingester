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
# File:  Reingester.java
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
import java.util.LinkedList;
import java.util.List;

import edu.indiana.d2i.htrc.ingest.cassandra.ReingestToolBridge;

/**
 * @author Yiming Sun
 *
 */
public class Reingester {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsage();
            System.exit(-1);
        }
        
        List<String> volumeList = new LinkedList<String>();
        
        BufferedReader reader = new BufferedReader(new FileReader(args[0]));
        String line = null;
        do {
            line = reader.readLine();
            if (line != null) {
                volumeList.add(line.trim());
            }
        } while (line != null);
        reader.close();
        
        ReingestToolBridge reingester = new ReingestToolBridge(volumeList);
        reingester.reingest();
    }

    
    private static void printUsage() {
        System.out.println("Reingest <volume_list_file>");
    }


}

