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
# File:  PersistentIngestService.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.indiana.d2i.htrc.ingest.cassandra.HectorManager;
import edu.indiana.d2i.htrc.ingest.cassandra.PairtreeToCassandraDataSyncker;
import edu.indiana.d2i.htrc.ingest.rsync.Ingester;
import edu.indiana.d2i.htrc.ingest.verify.HectorAccessor;
import edu.indiana.d2i.htrc.ingest.verify.Verificationer;

/**
 * @author Yiming Sun
 *
 */
public class PersistentIngestService {
    
    static class ControlFileBean {
        private static Logger log = Logger.getLogger(ControlFileBean.class);
        private File controlFile;

        private static final String PK_STOP = "STOP";
        private static final String SEPARATOR = "=";
        
        private boolean stop = false;
        
        ControlFileBean(String controlFilePath) {
            controlFile = new File(controlFilePath);
            if (!controlFile.exists()) {
                this.stop = false;
                write();
            }
            
        }
        
        void write() {
            FileWriter writer = null;
            try {
                writer = new FileWriter(controlFile);
                writer.write(PK_STOP);
                writer.write(SEPARATOR);
                writer.write(Boolean.toString(stop));
                writer.write(System.getProperty("line.separator"));
                
            } catch (IOException e) {
                log.warn("Unable to write control file", e);
            } finally {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        log.warn("Unable to close FileWriter", e);
                    }
                }
            }
            
        }
        
        void read() {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(controlFile));
                String line = null;
                do {
                    line = reader.readLine();
                    if (line != null) {
                      StringTokenizer tokenizer = new StringTokenizer(line, SEPARATOR);
                      if (tokenizer.countTokens() > 1) {
                          String token = tokenizer.nextToken();
                          if (token.trim().equalsIgnoreCase(PK_STOP)) {
                              String value = tokenizer.nextToken();
                              this.stop = Boolean.parseBoolean(value);
                              break;
                          }
                      }
                    }
                } while (line != null);
                
            } catch (FileNotFoundException e) {
                log.warn("Control file not found", e);
            } catch (IOException e) {
                log.warn("Unable to read control file", e);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        log.warn("Unable to close BufferedReader", e);
                    }
                }
            }
        }
        
        boolean getStop() {
            if (controlFile.exists()) {
                read();
            }
            return this.stop;
        }
    }
    
    private static Logger log = Logger.getLogger(PersistentIngestService.class);
    
    private static boolean checkStopSignal(ControlFileBean controlFileBean) {
        return controlFileBean.getStop();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        PropertyReader propertyReader = PropertyReader.getInstance();
        String log4PropertiesPath = propertyReader.getProperty(Constants.PK_LOG4J_PROPERTIES_PATH);
        PropertyConfigurator.configure(log4PropertiesPath);
        
        boolean stop = false;
        long wakeupInterval = Long.parseLong(propertyReader.getProperty(Constants.PK_PERSIST_WAKEUP_INTERVAL_IN_MS));
        long ingestInterval = Long.parseLong(propertyReader.getProperty(Constants.PK_PERSIST_INGEST_INTERVAL_IN_HOUR));
        long ingestIntervalInMs = ingestInterval * 3600000L;
        String controlFilePath = propertyReader.getProperty(Constants.PK_PERSIST_CONTROL_FILE_PATH);
        
        boolean performRsync = Util.getBooleanProperty(propertyReader.getProperty(Constants.PK_PERFORM_RSYNC));
        boolean performNosqlPush = Util.getBooleanProperty(propertyReader.getProperty(Constants.PK_PERFORM_NOSQL_PUSH));
        boolean performVerification = Util.getBooleanProperty(propertyReader.getProperty(Constants.PK_PERFORM_VERIFICATION));
        
        long nextIngestTime = 0L;
        
        if (!performRsync || !performNosqlPush) {
            log.warn("Persistent Ingest Service must run rsync and nosql push. Exiting now...");
        } else {
            ControlFileBean controlFileBean = new ControlFileBean(controlFilePath);
            
            log.info("Persistent Ingest Service started");
        
            do {
                if (System.currentTimeMillis() >= nextIngestTime) {
                    try {
                        log.info("Rsyncking data from remote source to local pairtree...");
                        Ingester ingester = new Ingester();
                        ingester.ingest();
                    
                        log.info("Done rsyncking with remote source");
                        
                        log.info("Syncking data from local pairtree to cassandra...");
                        
                        PairtreeToCassandraDataSyncker dataSyncker = new PairtreeToCassandraDataSyncker();
                        dataSyncker.process();
        
                        log.info("Done syncking data from local pairtree");
                        
                        if (performVerification) {
                            log.info("Verifying data...");
                            Verificationer verificationer = new Verificationer();
                            verificationer.verify();
                            log.info("Done verifying data");
                        } else {
                            log.info("Skipping data verification");
                        }
    
                    } catch (IngestException e) {
                        log.error("Ingest process terminated abnormally due to exception", e);
                    }

                    nextIngestTime = System.currentTimeMillis() + ingestIntervalInMs;
                    log.info("Next ingest time: " + (new Date(nextIngestTime)).toString());
                }
                
                stop = checkStopSignal(controlFileBean);
                
                if (!stop) {
                    try {
                        Thread.sleep(wakeupInterval);
                    } catch (InterruptedException e) {
                        log.warn("Interrupted while sleeping", e);
                    }
                }
            } while (!stop);
            
            log.info("Shutting down hector manager singleton");
            HectorManager.getInstance().shutdown();
            
            log.info("Shutting down hector accessor singleton");
            HectorAccessor.getInstance().shutdown();
            
            log.info("Persistent Ingest Service stopped");
        }
    }

}

