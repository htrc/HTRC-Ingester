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
# Unless required by applicable law or areed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: HTRC-Ingester
# File:  RevertDeltaLogs.java
# Description: This class is a tool to modify the state of the delta logs and delta log directories to another state
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.tools;

import java.io.File;
import java.io.FileFilter;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.PropertyReader;
import edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.FromToFactory.FromDirSelector;
import edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.FromToFactory.ToDirRenamer;

/**
 * This class is a tool to modify the state of the delta logs and delta log directories to another state
 * @author Yiming Sun
 *
 */
public class RevertDeltaLogs {
    
    private static final String ALL = "ALL";
    private static final String PROCESSED = "PROCESSED";
    private static final String VERIFIED = "VERIFIED";
    private static final String RAW = "RAW";
    
    /**
     * This class implements FileFilter interface and uses regex to pick up files and/or directories matching the pattern
     * @author Yiming Sun
     *
     */
    static class RegexFileFilter implements FileFilter {
        private final String[] regexes;
        
        /**
         * Constructor
         * @param regexes an array of regexes to match
         */
        RegexFileFilter(String[] regexes) {
            this.regexes = regexes.clone();
        }

        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            boolean result = true;
            for (String regex: regexes) {
                result = result && name.matches(regex);
                if (!result) break;
            }
            return result;
        }
    }
    
    /**
     * This implements FileFilter interface and uses the String contains() method to pick p files and/or directories containing the specified pattern
     * @author Yiming Sun
     *
     */
    static class ContainsFileFilter implements FileFilter {
        private final String[] patterns;
        
        /**
         * Constructor
         * @param patterns an array of patterns to check for containment
         */
        ContainsFileFilter(String[] patterns) {
            this.patterns = patterns;
        }

        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            boolean result = true;
            for (String pattern : patterns) {
                result = result && name.contains(pattern);
                if (!result) break;
            }
            return result;
        }
        
    }
    
    /**
     * This class implements FileFilter interface to use regexes and literal string patterns to match and check for containment when picking up files and/or directories
     * @author Yiming Sun
     *
     */
    static class RegexContainsFileFilter implements FileFilter {
        private final RegexFileFilter regexFileFilter;
        private final ContainsFileFilter containsFileFilter;
        
        /**
         * Constructor
         * @param regexes an array of regexes to match
         * @param patterns an array of patterns to check for containment
         */
        RegexContainsFileFilter(String[] regexes, String[] patterns) {
            this.regexFileFilter = new RegexFileFilter(regexes);
            this.containsFileFilter = new ContainsFileFilter(patterns);
        }

        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File pathname) {
            return (regexFileFilter.accept(pathname) && containsFileFilter.accept(pathname));
        }
        
        
    }
    
    
    
    /**
     * This class is a factory for generating the correct FileFilter based on the arguments 
     * @author Yiming Sun
     *
     */
    static class FromToFactory {
        /**
         * interface definition for FromDirSelector
         * @author Yiming Sun
         *
         */
        static interface FromDirSelector {
            /**
             * Method to get the FileFilter for the delta log directory
             * @return a FileFilter instance for the delta log directory
             */
            public FileFilter getDirFileFilter();
            /**
             * Method to get the FileFilter for the delta log file
             * @return a FileFilter instance for the delta log file
             */
            public FileFilter getFileFileFilter();
            /**
             * Method to get the raw filename of a delta log file
             * @param file a File representing a delta log file
             * @return the raw filename of a delta log file
             */
            public String getRawFilename(File file);
            /**
             * Method to get the raw directory name of a delta log directory
             * @param dir a File representing a delta log directory
             * @return the raw directory name of a delta log directory
             */
            public String getRawDirname(File dir);
        }

        /**
         * interface definition for ToDirRenamer
         * @author Yiming Sun
         *
         */
        static interface ToDirRenamer {
            /**
             * Method to get the renamed File of a delta log file
             * @param file a File representing a delta log file
             * @param rawFilename the raw filename of the delta log file
             * @return a File with a new name representing the delta log file
             */
            public File getRenamedFile(File file, String rawFilename);
            
            /**
             * Method to get the renamed File of a delta log directory
             * @param file a File representing a delta log directory
             * @param rawDirname the raw filename of the delta log directory
             * @return a File with a new name representing the delta log directory
             */
            public File getRenamedDir(File file, String rawDirname);
        }
        
        private static final String VERIFIED_DIRNAME_REGEX = "_VERIFIED_(dlog-\\d{8}-\\d{9})";
        private static final Pattern VERIFIED_DIRNAME_PATTERN = Pattern.compile(VERIFIED_DIRNAME_REGEX);
        
        private static final String PROCESSED_DIRNAME_REGEX = "_PROCESSED_(dlog-\\d{8}-\\d{9})";
        private static final Pattern PROCESSED_DIRNAME_PATTERN = Pattern.compile(PROCESSED_DIRNAME_REGEX);
        
        private static final String PARSED_T_FILENAME_REGEX = "_PARSED_(t-\\d+-\\d+\\.txt)";
        private static final Pattern PARSED_T_FILENAME_PATTERN = Pattern.compile(PARSED_T_FILENAME_REGEX);
        
        private static final String EXTRACTED_T_FILENAME_REGEX = "_XTRACTED_(t-\\d+-\\d+\\.txt)";
        private static final Pattern EXTRACTED_T_FILENAME_PATTERN = Pattern.compile(EXTRACTED_T_FILENAME_REGEX);

        private static final String RAW_T_FILENAME_REGEX = "t-\\d+-\\d+\\.txt";
        public static final Pattern RAW_T_FILENAME_PATTERN = Pattern.compile(RAW_T_FILENAME_REGEX);
        
        private static final String RAW_DIRNAME_REGEX = "dlog-\\d{8}-\\d{9}";
        private static final Pattern RAW_DIRNAME_PATTERN = Pattern.compile(RAW_DIRNAME_REGEX);

        
        /**
         * This abstract class is a basic implementation of the FromDirSelector
         * @author Yiming Sun
         *
         */
        private static abstract class AbstractFromDirSelector implements FromDirSelector {
            protected FileFilter dirFileFilter;
            protected FileFilter fileFileFilter;

            /**
             * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.FromDirSelector#getDirFileFilter()
             */
            @Override
            public FileFilter getDirFileFilter() {
                return this.dirFileFilter;
            }

            /**
             * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.FromDirSelector#getFileFileFilter()
             */
            @Override
            public FileFilter getFileFileFilter() {
                return this.fileFileFilter;
            }

            /**
             * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.FromDirSelector#getRawFilename(java.io.File)
             */
            @Override
            public String getRawFilename(File file) {
                String name = file.getName();
                String rawFilename = null;

                Matcher parsedTFilenameMatcher = PARSED_T_FILENAME_PATTERN.matcher(name);
                
                if (parsedTFilenameMatcher.matches()) {
                    rawFilename = parsedTFilenameMatcher.group(1);
                } else {
                    Matcher extractedTFilenameMatcher = EXTRACTED_T_FILENAME_PATTERN.matcher(name);
                    if (extractedTFilenameMatcher.matches()) {
                        rawFilename = extractedTFilenameMatcher.group(1);
                    } else {
                        Matcher rawTFilenameMatcher = RAW_T_FILENAME_PATTERN.matcher(name);
                        if (rawTFilenameMatcher.matches()) {
                            rawFilename = name;
                        }
                    }
                }
                
                return rawFilename;
            }
            
            /**
             * 
             * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.FromToFactory.FromDirSelector#getRawDirname(java.io.File)
             */
            @Override
            public String getRawDirname(File file) {
                String name = file.getName();
                String rawDirname = null;
                
                Matcher verifiedDirnameMatcher = VERIFIED_DIRNAME_PATTERN.matcher(name);
                if (verifiedDirnameMatcher.matches()) {
                    rawDirname = verifiedDirnameMatcher.group(1);
                } else {
                    Matcher processedDirnameMatcher = PROCESSED_DIRNAME_PATTERN.matcher(name);
                    if (processedDirnameMatcher.matches()) {
                        rawDirname = processedDirnameMatcher.group(1);
                    } else {
                        Matcher rawDirnameMatcher = RAW_DIRNAME_PATTERN.matcher(name);
                        if (rawDirnameMatcher.matches()) {
                            rawDirname = name;
                        }
                    }
                }

                return rawDirname;
            }
            
        }
        
        /**
         * This class extends the AbstractFromDirSelector for selecting delta log directories that are verified and delta log files that are extracted
         * @author Yiming Sun
         *
         */
        private static class VerifiedFromDirSelector extends AbstractFromDirSelector {
            
            /**
             * Constructor
             */
            VerifiedFromDirSelector() {
                String[] dirRegexes = new String[] {VERIFIED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {EXTRACTED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexFileFilter(dirRegexes);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            /**
             * Constructor taking an array of patterns
             * @param patterns an array of patterns for containment check
             */
            VerifiedFromDirSelector(String[] patterns) {
                String[] dirRegexes = new String[] {VERIFIED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {EXTRACTED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexContainsFileFilter(dirRegexes, patterns);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            
        }
        
        /**
         * This class extends the AbstractFromDirSelector for selecting delta log directories that are verified and delta log files that are either extracted or parsed
         * @author Yiming Sun
         *
         */
        private static class TwoStepVerifiedFromDirSelector extends AbstractFromDirSelector {
            /**
             * Constructor
             */
            TwoStepVerifiedFromDirSelector() {
                String[] dirRegexes = new String[] {VERIFIED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {EXTRACTED_T_FILENAME_REGEX, PARSED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexFileFilter(dirRegexes);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            
            /**
             * Constructor taking an array of patterns
             * @param patterns an array of patterns for containment check
             */
            TwoStepVerifiedFromDirSelector(String[] patterns) {
                String[] dirRegexes = new String[] {VERIFIED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {EXTRACTED_T_FILENAME_REGEX, PARSED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexContainsFileFilter(dirRegexes, patterns);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
        }
    
        /**
         * This class extends the AbstractFromDirSelector for selecting delta log directories that are processed and delta log files that are parsed
         * @author Yiming Sun
         *
         */
        private static class ProcessedFromDirSelector extends AbstractFromDirSelector {
            
            /**
             * Constructor
             */
            ProcessedFromDirSelector() {
                String[] dirRegexes = new String[] {PROCESSED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {PARSED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexFileFilter(dirRegexes);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);

            }
            
            /**
             * Constructor taking an array of patterns
             * @param patterns an array of patterns for containment check
             */
            ProcessedFromDirSelector(String[] patterns) {
                String[] dirRegexes = new String[] {PROCESSED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {PARSED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexContainsFileFilter(dirRegexes, patterns);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }

        }
        
        /**
         * This class extends the AbstractFromDirSelector for delta log directories that are processed or verified and delta log files that are parsed or extracted
         * @author Yiming Sun
         *
         */
        private static class VerifiedOrProcessedFromDirSelector extends AbstractFromDirSelector {
            /**
             * Constructor
             */
            VerifiedOrProcessedFromDirSelector() {
                String[] dirRegexes = new String[] {PROCESSED_DIRNAME_REGEX, VERIFIED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {PARSED_T_FILENAME_REGEX, EXTRACTED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexFileFilter(dirRegexes);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            /**
             * Constructor taking an array of patterns
             * @param patterns an array of patterns for containment check
             */
            VerifiedOrProcessedFromDirSelector(String[] patterns) {
                String[] dirRegexes = new String[] {PROCESSED_DIRNAME_REGEX, VERIFIED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {PARSED_T_FILENAME_REGEX, EXTRACTED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexContainsFileFilter(dirRegexes, patterns);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);

            }
        }
    
        /**
         * This class extends AbstractFromDirSelector for delta log directories that are raw and delta log files that are raw
         * @author Yiming Sun
         *
         */
        private static class RawFromDirSelector extends AbstractFromDirSelector {
            /**
             * Constructor
             */
            RawFromDirSelector() {
                String[] dirRegexes = new String[] {RAW_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {RAW_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexFileFilter(dirRegexes);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            /**
             * Constructor taking an array of patterns
             * @param patterns an array of patterns for containment check
             */
            RawFromDirSelector(String[] patterns) {
                String[] dirRegexes = new String[] {RAW_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {RAW_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexContainsFileFilter(dirRegexes, patterns);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            
        }
        
        /**
         * This class implements the ToDirRenamer for renaming delta log directory to processed and renaming delta log files to parsed
         * @author Yiming Sun
         *
         */
        private static class ProcessedToDirRenamer implements ToDirRenamer {

            private static final String PARSED_PREFIX = "_PARSED_";
            private static final String PROCESSED_PREFIX = "_PROCESSED_";
            /**
             * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.FromDirSelectorFactory.ToDirRenamer#getRenamedFile(java.io.File, java.lang.String)
             */
            @Override
            public File getRenamedFile(File file, String rawFilename) {
                File returnFile = null;
            
                if (rawFilename != null) {
                    File parentFile = file.getParentFile();
                    if (parentFile != null) {
                        returnFile = new File(parentFile, PARSED_PREFIX + rawFilename);
                    }
                } 
                return returnFile;
            }

            /**
             * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.FromDirSelectorFactory.ToDirRenamer#getRenamedDir(java.io.File, java.lang.String)
             */
            @Override
            public File getRenamedDir(File file, String rawDirname) {
                
                File returnDir = null;
                
                if (rawDirname != null) {
                    File parentFile = file.getParentFile();
                    if (parentFile != null) {
                        returnDir = new File(parentFile, PROCESSED_PREFIX + rawDirname);
                    }
                } 
                return returnDir;
            }
            
        }
        
        /**
         * This class implements ToDirRenamer for renaming delta log directories and delta log files to their raw state 
         * @author Yiming Sun
         *
         */
        private static class RawToDirRenamer implements ToDirRenamer {

            /**
             * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.FromDirSelectorFactory.ToDirRenamer#getRenamedFile(java.io.File, java.lang.String)
             */
            @Override
            public File getRenamedFile(File file, String rawFilename) {
                File returnFile = null;
                
                if (rawFilename != null) {
                    File parentFile = file.getParentFile();
                    if (parentFile != null) {
                        returnFile = new File(parentFile, rawFilename);
                    }
                } 
                return returnFile;
            }

            /**
             * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.FromDirSelectorFactory.ToDirRenamer#getRenamedDir(java.io.File, java.lang.String)
             */
            @Override
            public File getRenamedDir(File file, String rawDirname) {
                File returnDir = null;
                
                if (rawDirname != null) {
                    File parentFile = file.getParentFile();
                    if (parentFile != null) {
                        returnDir = new File(parentFile, rawDirname);
                    }
                } 
                return returnDir;
            }
            
        }
    
        /**
         * Method to select the correct FromDirSelector based on the input
         * @param from the state of the delta logs to revert from
         * @param to the state of the delta logs to revert to
         * @param patterns an array of patterns to check for containment
         * @return the correct FromDirSelector
         */
        static FromDirSelector getFromDirSelector(String from, String to, String[] patterns) {
            FromDirSelector fromDirSelector = null;
            if (patterns != null && patterns.length > 0) {
                if (ALL.equals(from)) {
                    fromDirSelector = new VerifiedOrProcessedFromDirSelector(patterns);
                } else if (VERIFIED.equals(from)) {
                    if (RAW.equals(to)) {
                        fromDirSelector = new TwoStepVerifiedFromDirSelector(patterns);
                    } else {
                        fromDirSelector = new VerifiedFromDirSelector(patterns);
                    }
                } else if (PROCESSED.equals(from)) {
                    fromDirSelector = new ProcessedFromDirSelector(patterns);
                } else if (RAW.equals(from)) {
                    fromDirSelector = new RawFromDirSelector(patterns);
                }
            } else {
                if (ALL.equals(from)) {
                    fromDirSelector = new VerifiedOrProcessedFromDirSelector();
                } else if (VERIFIED.equals(from)) {
                    if (RAW.equals(to)) {
                        fromDirSelector = new TwoStepVerifiedFromDirSelector();
                    } else {
                        fromDirSelector = new VerifiedFromDirSelector();
                    }
                } else if (PROCESSED.equals(from)) {
                    fromDirSelector = new ProcessedFromDirSelector();
                } else if (RAW.equals(from)) {
                    fromDirSelector = new RawFromDirSelector();
                }
            }
            
            return fromDirSelector;
        }
    
        /**
         * Method to select the correct ToDirRenamer based on the input
         * @param to the state of the delta logs to revert to
         * @param from the state of the delta logs to revert from
         * @return the correct ToDirRenamer
         */
        static ToDirRenamer  getToDirRenamer(String to, String from) {
            ToDirRenamer toDirRenamer = null;
            if (PROCESSED.equals(to)) {
                toDirRenamer = new ProcessedToDirRenamer();
            } else if (RAW.equals(to)) {
                toDirRenamer = new RawToDirRenamer();
            }
            return toDirRenamer;
        }
        
    }
    
    /**
     * main method
     * @param args arguments passed in by the system
     */
    public static void main (String[] args) {

        final HashSet<String> validFromSet = new HashSet<String>();
        validFromSet.add(ALL);
        validFromSet.add(PROCESSED);
        validFromSet.add(VERIFIED);
        
        final HashSet<String> validToSet = new HashSet<String>();
        validToSet.add(PROCESSED);
        validToSet.add(RAW);
        
        HashSet<String> targetSet = new HashSet<String>();

        String from = ALL;
        String to = RAW;
        
        FromDirSelector fromDirSelector = null;
        ToDirRenamer toDirRenamer = null;

        boolean special = false;
        
        try {
            int i = 0;
            while (i < args.length) {
                if ("-f".equals(args[i]) || "--from".equals(args[i])) {
                    i++;
                    from = args[i].toUpperCase();
                } else if ("-t".equals(args[i]) || "--to".equals(args[i])) {
                    i++;
                    to = args[i].toUpperCase();
                } else if ("-h".equals(args[i]) || "--help".equals(args[i])) {
                    printUsage();
                    System.exit(-1);
                } else if ("-s".equals(args[i]) || "--special".equals(args[i])) {
                    special = true;
                } else {
                    targetSet.add(args[i]);
                }
                
                i++;
            }
            
            if (!special) {
                if (!validFromSet.contains(from)) {
                    throw new IllegalArgumentException("Invalid \"from\" argument: " + from);
                }
                if (!validToSet.contains(to)) {
                    throw new IllegalArgumentException("Invalid \"to\" argument: " + to);
                }
                
            } else {
                from = RAW;
                to = PROCESSED;
            }
            
            String[] patterns = targetSet.toArray(new String[0]);
            
            fromDirSelector = FromToFactory.getFromDirSelector(from, to, patterns);
            toDirRenamer = FromToFactory.getToDirRenamer(to, from);
            
            PropertyReader propertyReader = PropertyReader.getInstance();
            String deltaLogRootPath = propertyReader.getProperty(Constants.PK_DELTA_LOG_ROOT);
            
            File deltaLogRoot = new File(deltaLogRootPath);
            
            FileFilter dirFileFilter = fromDirSelector.getDirFileFilter();
            FileFilter fileFileFilter = fromDirSelector.getFileFileFilter();
            
            File[] listFiles = deltaLogRoot.listFiles(dirFileFilter);
            
            for (File file : listFiles) {
                File[] tfilesList = file.listFiles(fileFileFilter);
                for (File tfile : tfilesList) {
                    String rawFilename = fromDirSelector.getRawFilename(tfile);
                    File renamedFile = toDirRenamer.getRenamedFile(tfile, rawFilename);

                    if (tfile.getName() != renamedFile.getName()) {
                        boolean renameTo = tfile.renameTo(renamedFile);
                        if (!renameTo) {
                            System.err.println("ERROR: Failed to rename file " + tfile.getPath() + " to " + renamedFile.getPath());
                        } else {
                            System.out.println("Renamed " + tfile.getPath() + " to " + renamedFile.getPath());
                        }
                    } else {
                        System.out.println("Skipping renaming file " + tfile.getPath() + " to its current name");
                    }
                }
                
                
                String rawDirname = fromDirSelector.getRawDirname(file);
                File renamedDir = toDirRenamer.getRenamedDir(file, rawDirname);
                
                if (file.getName() != renamedDir.getName()) {
                    boolean renameTo = file.renameTo(renamedDir);
                    if (!renameTo) {
                        System.err.println("ERROR: Failed to revert dir " + file.getPath() + " to " + renamedDir.getPath());
                    } else {
                        System.out.println("Reverted " + file.getPath() + " to " + renamedDir.getPath());
                    }
                } else {
                    System.out.println("Skipping reverting dir " + file.getPath() + " to its current name");
                }
            }
        } catch (IllegalArgumentException e) {
            printUsage();
        }
    }
    
    /**
     * Method to print the usage
     */
    private static void printUsage() {
        System.out.println("Usage: RevertDeltaLogs [<-f | --from> <VERIFIED | PROCESSED | ALL>]  [<-t | --to> <PROCESSED | RAW>]  [pattern [pattern ...]]");
        
        System.out.println("       default values:  -f ALL -t RAW");
        
        System.out.println("Usage: RevertDeltaLogs <-h | --help>");
    }

}

