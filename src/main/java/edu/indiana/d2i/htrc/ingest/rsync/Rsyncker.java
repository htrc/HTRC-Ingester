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
# Project: rsync_test
# File:  Rsyncker.java
# Description: This class performs rsync 
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.rsync;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import edu.indiana.d2i.htrc.ingest.JobQueue;


/**
 * This class performs rsync
 * @author Yiming Sun
 *
 */
public class Rsyncker implements Runnable {
    
    /**
     * This factory class captures output from a Java Runtime process, both the output and error streams
     * @author Yiming Sun
     *
     */
    protected static class OutputCatcherFactory {
    	
        /**
         * This class captures the output from an InputStream and writes to a Log4j logger
         * @author Yiming Sun
         *
         */
    	protected static class OutputLogger implements Runnable {
    		private static Logger log = Logger.getLogger(OutputLogger.class);
    		private final Logger oLog;
    		private final InputStream inputStream;
    		private final Priority priority;
    		
    		/**
    		 * Constructor
    		 * @param oLog a Log4j Logger
    		 * @param inputStream an InputStream to read from
    		 * @param priority the log level to write the log message
    		 */
    		OutputLogger(Logger oLog, InputStream inputStream, Priority priority) {
    			this.oLog = oLog;
    			this.inputStream = inputStream;
    			this.priority = priority;
    		}
    		
    		/**
    		 * 
    		 * @see java.lang.Runnable#run()
    		 */
    		public void run() {
    			String line = null;
    			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    			try {
    				do {
    					line = reader.readLine();
    					if (line != null) {
    						oLog.log(priority, line);
    					}
    				} while (line != null);
    				reader.close();

    			} catch (IOException e) {
    				log.error("IOException: ", e);
    			}
    		}
    		
    	}
    		
    	/**
    	 * This class captures the output from an InputStream and writes to the provided Writer object
    	 * @author Yiming Sun
    	 *
    	 */
    	protected static class OutputWriter implements Runnable {
    		private static final String EOL = System.getProperty("line.separator");
    		private static Logger log = Logger.getLogger(OutputWriter.class);
    		private final InputStream inputStream;
    		private final Writer writer;
    		
    		/**
    		 * Constructor
    		 * @param writer a Writer object to which to write the captured output
    		 * @param inputStream an InputStream from which to read information
    		 */
    		OutputWriter(Writer writer, InputStream inputStream) {
    			this.writer = writer;
    			this.inputStream = inputStream;
    		}
    		
    		/**
    		 * 
    		 * @see java.lang.Runnable#run()
    		 */
    		public void run() {

    			String line = null;
    			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    			try {
    				do {
    					line = reader.readLine();
    					if (line != null) {
    						writer.write(line);
    						writer.write(EOL);
    					}
    				} while (line != null);
    				reader.close();
    			} catch (IOException e) {
    				log.error("IOException: ", e);
    			}
    		}
    		
    	}
    	
    	/**
    	 * Method to get a new Thread that runs the OutputLogger 
    	 * @param log a Log4j logger
    	 * @param inputStream an InputStream to read
    	 * @param priority log level
    	 * @return a Thread with an OutputLogger Runnable object
    	 */
    	static Thread getOutputLogger(Logger log, InputStream inputStream, Priority priority) {
    		OutputLogger outputLogger = new OutputLogger(log, inputStream, priority);
    		Thread thread = new Thread(outputLogger);
    		return thread;
    	}
    	
    	/**
    	 * Method to get a new Thread that runs the OutputWriter
    	 * @param writer a Writer object
    	 * @param inputStream an InputStream to read
    	 * @return a Thread with an OutputWriter Runnable object
    	 */
    	static Thread getOutputWriter(Writer writer, InputStream inputStream) {
    		OutputWriter outputWriter = new OutputWriter(writer, inputStream);
    		Thread thread = new Thread(outputWriter);
    		return thread;
    	}
    	
    }
    

    private static Logger log = Logger.getLogger(Rsyncker.class);
    
    protected final String id;
  
    protected String deltaLogDir;
    protected boolean writeLog;
    
    protected JobQueue<RsyncJobDescriptor> jobQueue;

    /**
     * Constructor
     * @param id an int to differentiate and identify instances
     * @param jobQueue a JobQueue of RsyncJobDescriptor objects
     */
    protected Rsyncker(String id, JobQueue<RsyncJobDescriptor> jobQueue) {
        this.id = id;
        this.jobQueue = jobQueue;
        deltaLogDir = null;
        writeLog = false;
    }
    
    /**
     * Method to build the rsync commandline command
     * @param job a RsyncJobDescriptor object
     * @return a String array containing the command with arguments
     */
    protected String[] buildCommandArray(RsyncJobDescriptor job) {
        List<String> strings = new ArrayList<String>();
        strings.add("rsync");
        strings.add("--copy-links");
        strings.add("--delete");
        strings.add("--ignore-errors");
        strings.add("--perms");
        strings.add("--recursive");
        strings.add("--times");
//        strings.add("--archive");
        strings.add("--verbose");
//        strings.add("--progress");
        
        if (job.isDryRun()) {
        	strings.add("--dry-run");
        }
        

        
        String password = job.getPassword();
        if (password != null && !password.equals("")) {
            strings.add("--password-file=" + password);
        }

        String username = job.getUsername();
        if (username != null && !username.equals("")) {
            strings.add(username + "@");
        }

        String separator = job.getSeparator();
        String host = job.getHost();
        String sourceRoot = job.getSourceRoot();
        
        strings.add(host + separator + sourceRoot);
        
        strings.add(job.getDestRoot());
        
        String[] commandArray = strings.toArray(new String[0]);
        if (log.isTraceEnabled()) {
            StringBuilder builder = new StringBuilder();
            for (String string : commandArray) {
                builder.append(string).append(" ");
            }
            log.trace("command: " + builder.toString());
        }
        return commandArray;
    }
    
    /**
     * Method to prepare a delta log file
     * @param iteration an int to identify the current iteration
     * @return a Writer object to which the delta log conten will be written
     */
    protected Writer prepareDeltaLogFile(int iteration) {
    	Writer writer = null;
	    String dlogFileName = id + "-" + iteration + ".txt";
	    File dlogFile = new File(deltaLogDir, dlogFileName);
    	try {
    	    writer = new FileWriter(dlogFile);
    	} catch (IOException e) {
    		log.error("Unable to create dlog file " + dlogFile.getPath());
    		writeLog = false;
    	}
    	return writer;
    	
    }
    /**
     * @see java.lang.Runnable#run()
     */
    public void run() {
        RsyncJobDescriptor job = null;
        Writer logWriter = null;

        
        String EOL = System.getProperty("line.separator");
        try {
        	int iteration = 0;
        	boolean isAllJobDone = false;
        	
            do {
                
            	Thread outputCatcherThread = null;
                Thread errorCatcherThread = null;
                job = jobQueue.dequeue();

                if (job != null) {
                    
                	deltaLogDir = job.getDeltaLogDir();
                    if (deltaLogDir != null) {
                    	writeLog = true;
                    }
                	if (writeLog) {
                		logWriter = prepareDeltaLogFile(iteration);
                	}
                    Runtime runtime = Runtime.getRuntime();
                    Process process = runtime.exec(buildCommandArray(job));
                    
                    if (writeLog) {
                    	try {
                    	logWriter.write("DEST_ROOT=" + job.getDestRoot());
                    	logWriter.write(EOL);
                    	logWriter.write("PREFIX=" + job.getPrefix());
                    	logWriter.write(EOL);
                    	logWriter.write("COPYRIGHT=" + job.getCopyright().toString());
                    	logWriter.write(EOL);
                    	outputCatcherThread = OutputCatcherFactory.getOutputWriter(logWriter, process.getInputStream());
                    	} catch (IOException e) {
                    		log.error("writing DEST_ROOT to dlog throws IOException. Switch to log4j log.", e);
                    		try {
                    			logWriter.close();
                    		} catch (IOException ioe) {
                    			log.error("Unable to close logWriter.", ioe);
                    		}
                			writeLog = false;
                			logWriter = null;
                			outputCatcherThread = OutputCatcherFactory.getOutputLogger(log, process.getInputStream(), Level.INFO);
                    	}
                    } else {
                    	outputCatcherThread = OutputCatcherFactory.getOutputLogger(log, process.getInputStream(), Level.INFO);
                    }
                    outputCatcherThread.start();
                    
                    errorCatcherThread = OutputCatcherFactory.getOutputLogger(log, process.getErrorStream(), Level.ERROR);
                    errorCatcherThread.start();
                    
                    
                    try {
                    	int exitValue = process.waitFor();

                    	if (exitValue == 0) {
                    		log.info(id + " exitValue:" + exitValue);
                    	} else {
                    		log.error("rsync returned abnormal exitValue: Thread " + id + " exitValue: " + exitValue);
                    	}
                    } catch (InterruptedException e) {
                    	log.error("Thread " + id + " interrupted with exitValue: " + process.exitValue(), e);
                    }

                    try {
                    	outputCatcherThread.join();
                    } catch (InterruptedException e) {
                    	log.error("Thread " + id + " joining output catcher thread interrupted", e);
                    }
                    
                    try {
                    	errorCatcherThread.join();
                    } catch (InterruptedException e) {
                    	log.error("Thread " + id + " joining error catcher thread interrupted", e);
                    }
                    
                    if (logWriter != null) {
                    	try {
                    		logWriter.close();
                    	} catch (IOException e) {
                    		log.error("attempt to close dlog writer throws IOException.", e);
                    	}
                    }
                } else {
                    isAllJobDone = jobQueue.isDone();
                }
                iteration++;
            } while (!isAllJobDone);
            log.info("Job Thread " +  id + " finished.");
        } catch (IOException e) {
        	log.error("Job Thread " + id + " throws IOException", e);
        }
    } 

    
}

