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
# File:  VerificationException.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.verify;

/**
 * @author Yiming Sun
 *
 */
public class VerificationException extends Exception {
    private String volumeID;
    private String pageSequence;
    
    public VerificationException(String message, String volumeID, String pageSequence) {
        super(message);
        this.volumeID = volumeID;
        this.pageSequence = pageSequence;
    }
    
    public VerificationException(String message, String volumeID) {
        this(message, volumeID, null);
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(VerificationException.class.getName() + " Volume: ");
        builder.append(volumeID);
        if (pageSequence != null) {
            builder.append(" page: " ).append(pageSequence);
        }
        builder.append(" ").append(this.getMessage());
        return builder.toString();
    }

}

