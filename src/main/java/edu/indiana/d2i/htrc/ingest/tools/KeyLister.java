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
# File:  KeyLister.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * A utility to list all volumeIDs inside Cassandra.  Not the most efficient
 * way since KeyIterator is slow.  An alternative is to list the ZIP or METS
 * files directly from the pairtree, but it may not accurately reflect what's
 * actually in Cassandra.
 */
package edu.indiana.d2i.htrc.ingest.tools;

import me.prettyprint.cassandra.service.KeyIterator;
import edu.indiana.d2i.htrc.ingest.cassandra.HectorToolBridge;

/**
 * @author Yiming Sun
 *
 */
public class KeyLister {
    
    public static void main(String[] args) throws Exception {
        HectorToolBridge hectorToolBridge = new HectorToolBridge();
        KeyIterator<String> keyIterator = hectorToolBridge.getKeyIterator();
        for (String key : keyIterator) {
            System.out.println(key);
        }
        hectorToolBridge.shutdown();
    }

}

