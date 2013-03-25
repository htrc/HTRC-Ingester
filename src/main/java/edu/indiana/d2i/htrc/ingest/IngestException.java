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
# File:  IngestException.java
# Description: This class extends the Exception class for all Exceptions raised during the ingest process 
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest;

/**
 * This class extends the Exception class for all Exceptions raised during the ingest process
 * @author Yiming Sun
 *
 */
public class IngestException extends Exception {
    /**
     * Constructor
     */
    public IngestException() {
        super();
    }
    
    /**
     * Constructor taking a message
     * @param message a message
     */
    public IngestException(String message) {
        super(message);
    }
    
    /**
     * Constructor taking a message and a Throwable cause
     * @param message a message
     * @param cause a Throwable cause
     */
    public IngestException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructor taking a Throwable cause
     * @param cause a Throwable cause
     */
    public IngestException(Throwable cause) {
        super(cause);
    }

}

