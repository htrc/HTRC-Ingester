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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: HTRC-Ingester
# File:  RsynkJobDescriptor.java
# Description: This class stores information regarding rsync  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.rsync;

import edu.indiana.d2i.htrc.ingest.Constants.CopyrightEnum;

/**
 * This class stores information regarding rsync
 * @author Yiming Sun
 *
 */
public class RsyncJobDescriptor {
    protected final String sourceRoot;
    protected final String destRoot;
    protected final String prefix;
    protected final CopyrightEnum copyright;
    protected final String username;
    protected final String password;
    protected final String host;
    protected final String separator;
    protected final String deltaLogDir;
    protected final boolean dryRun;
    
    /**
     * Constructor
     * @param sourceRoot root directory of rsync source
     * @param destRoot destination root directory
     * @param prefix the prefix/namespace of a Pairtree
     * @param copyright copyright information
     * @param username username for the rsync
     * @param password password for the rsync
     * @param host rsync remote host
     * @param separator rsync separator symbol
     * @param deltaLogDir delta log directory
     * @param dryRun a boolean flag indicating if the rsync is a dry run
     */
    protected RsyncJobDescriptor(String sourceRoot, String destRoot, String prefix, CopyrightEnum copyright, String username, String password, String host, String separator, String deltaLogDir, boolean dryRun) {
        this.sourceRoot = sourceRoot;
        this.destRoot = destRoot;
        this.prefix = prefix;
        this.copyright = copyright;
        this.username = username;
        this.password = password;
        this.host = host;
        this.separator = separator;
        this.deltaLogDir = deltaLogDir;
        this.dryRun = dryRun;
    }
    
    /**
     * Method to return the rsync source root
     * @return the rsync source root
     */
    protected String getSourceRoot() {
        return this.sourceRoot;
    }
    
    /**
     * Method to return the destination root directory
     * @return the destination root directory
     */
    protected String getDestRoot() {
        return this.destRoot;
    }
    
    /**
     * Method to return the Pairtree namespace prefix
     * @return the Pairtree namespace prefix
     */
    protected String getPrefix() {
        return this.prefix;
    }
    
    /**
     * Method to get the copyright
     * @return the copyright
     */
    protected CopyrightEnum getCopyright() {
        return this.copyright;
    }
    
    /**
     * Method to get the username for rsync
     * @return the username for rsync
     */
    protected String getUsername() {
        return username;
    }
    
    /**
     * Method to get the password for rsync
     * @return the password for rsync
     */
    protected String getPassword() {
        return password;
    }
    /**
     * Method to get the remote host for rsync
     * @return the remote host for rsync
     */
    protected String getHost() {
        return host;
    }
    /**
     * Method to get the separator symbol for rsync
     * @return the separator symbol for rsync
     */
    protected String getSeparator() {
        return separator;
    }
    /**
     * Method to get the delta directory
     * @return  the delta directory
     */
    protected String getDeltaLogDir() {
        return deltaLogDir;
    }
    /**
     * Method to get the dry run boolean flag
     * @return the dry run boolean flag
     */
    protected boolean isDryRun() {
        return dryRun;
    }
}

