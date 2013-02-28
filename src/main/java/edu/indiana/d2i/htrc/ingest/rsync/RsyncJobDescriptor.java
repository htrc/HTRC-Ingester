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
# File:  RsynkJobDescriptor.java
# Description:  
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
    
    protected String getSourceRoot() {
        return this.sourceRoot;
    }
    
    protected String getDestRoot() {
        return this.destRoot;
    }
    
    protected String getPrefix() {
        return this.prefix;
    }
    
    protected CopyrightEnum getCopyright() {
        return this.copyright;
    }
    
    protected String getUsername() {
        return username;
    }
    
    protected String getPassword() {
        return password;
    }
    
    protected String getHost() {
        return host;
    }
    
    protected String getSeparator() {
        return separator;
    }
    
    protected String getDeltaLogDir() {
        return deltaLogDir;
    }
    
    protected boolean isDryRun() {
        return dryRun;
    }
}

