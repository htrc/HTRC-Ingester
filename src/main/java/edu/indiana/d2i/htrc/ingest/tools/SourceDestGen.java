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
# File:  SourceDestGen.java
# Description: This class is a tool for generating rsync source and destination pairs  
#
# -----------------------------------------------------------------
# 
*/


package edu.indiana.d2i.htrc.ingest.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;

/**
 * This class is a tool for generating rsync source and destination pairs
 * @author Yiming Sun
 *
 */
public class SourceDestGen {
	final String inputFilename;
	final String outputFilename;
	final String sourceRoot;
	final String destRoot;
	final String sourceAlias;
	
	/**
	 * Constructor
	 * @param inputFilename an input file containing paths to be rsyncked, typically the output from the linux tree command
	 * @param outputFilename the output file to which the source-destination pairs are written
	 * @param sourceRoot source root directory
	 * @param destRoot destination root directory
	 * @param sourceAlias alias of the source
	 */
	public SourceDestGen(String inputFilename, String outputFilename, String sourceRoot, String destRoot, String sourceAlias) {
		this.inputFilename = inputFilename;
		this.outputFilename = outputFilename;
		this.sourceRoot = sourceRoot;
		this.destRoot = destRoot;
		this.sourceAlias = sourceAlias;
		
		if (!sourceRoot.endsWith("/")) {
			sourceRoot = sourceRoot + "/";
		}
		
		if (!destRoot.endsWith("/")) {
			destRoot = destRoot + "/";
		}
	}
	
	/**
	 * Method to generate the pairs
	 * @throws Exception thrown if anything failed
	 */
	public void generate() throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilename)));
		FileWriter writer = new FileWriter(outputFilename);
		
		String line = null;
		boolean addSlash = false;
		int count = 1;
		do {
			addSlash = false;
			line = reader.readLine();
			if (line != null) {

				if (!line.endsWith("/")) {
					addSlash = true;
				}
				
				int index = line.indexOf(sourceRoot);
				if (index >= 0) {
					String relativePath = line.substring(sourceRoot.length());
					String sourceLine = null;
					if (sourceAlias == null) {
					    sourceLine = "RSYNC_SOURCE_ROOT" + count + "=" + line + (addSlash ? "/" : "") + System.getProperty("line.separator");
					} else {
					    sourceLine = "RSYNC_SOURCE_ROOT" + count + "=" + sourceAlias + relativePath + (addSlash ? "/" : "") + System.getProperty("line.separator");
					}
					String destLine = "RSYNC_DEST_ROOT" + count + "=" + destRoot + relativePath + (addSlash ? "/" : "") + System.getProperty("line.separator");
					count++;
					writer.write(sourceLine);
					writer.write(destLine);
					
				}
			}
		} while (line != null);
		reader.close();
		writer.close();
	}
	
	/**
	 * main method
	 * @param args arguments passed in by the system
	 * @throws Exception thrown if anything failed
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			printUsage();
			System.exit(-1);
		}
		
		if (args.length == 4) {
		    SourceDestGen gen = new SourceDestGen(args[0], args[1], args[2], args[3], null);
		    gen.generate();
		} else if (args.length == 5) {
		    SourceDestGen gen = new SourceDestGen(args[0], args[1], args[2], args[3], args[4]);
		    gen.generate();
		}
	}
	
	/**
	 * Method to print the usage
	 */
	static void printUsage() {
		System.out.println("Usage: java SourceDestGen <inputFile> <outputFile> <sourceRoot> <destRoot> [sourceAlias]");
	}

}
