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
# Description:  
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
 * @author Yiming Sun
 *
 */
public class RevertDeltaLogs {
    
    private static final String ALL = "ALL";
    private static final String PROCESSED = "PROCESSED";
    private static final String VERIFIED = "VERIFIED";
    private static final String RAW = "RAW";
    
    
    static class RegexFileFilter implements FileFilter {
        private final String[] regexes;
        
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
    
    static class ContainsFileFilter implements FileFilter {
        private final String[] patterns;
        
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
    
    static class RegexContainsFileFilter implements FileFilter {
        private final RegexFileFilter regexFileFilter;
        private final ContainsFileFilter containsFileFilter;
        
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
    
    
    
    
    static class FromToFactory {
        static interface FromDirSelector {
            public FileFilter getDirFileFilter();
            public FileFilter getFileFileFilter();
            public String getRawFilename(File file);
            public String getRawDirname(File dir);
        }

        static interface ToDirRenamer {
            public File getRenamedFile(File file, String rawFilename);
            public File getRenamedDir(File file, String rawDirname);
        }
        
        private static final String VERIFIED_DIRNAME_REGEX = "_VERIFIED_(dlog-\\d{8}-\\d{9})";
        private static final Pattern VERIFIED_DIRNAME_PATTERN = Pattern.compile(VERIFIED_DIRNAME_REGEX);
        
        private static final String PROCESSED_DIRNAME_REGEX = "_PROCESSED_(dlog-\\d{8}-\\d{9})";
        private static final Pattern PROCESSED_DIRNAME_PATTERN = Pattern.compile(PROCESSED_DIRNAME_REGEX);
        
        private static final String PARSED_T_FILENAME_REGEX = "_PARSED_(t-\\d+-\\d+\\.txt)";
        private static final Pattern PARSED_T_FILENAME_PATTERN = Pattern.compile(PARSED_T_FILENAME_REGEX);
        
        private static final String RAW_T_FILENAME_REGEX = "t-\\d+-\\d+\\.txt";
        public static final Pattern RAW_T_FILENAME_PATTERN = Pattern.compile(RAW_T_FILENAME_REGEX);
        
        private static final String RAW_DIRNAME_REGEX = "dlog-\\d{8}-\\d{9}";
        private static final Pattern RAW_DIRNAME_PATTERN = Pattern.compile(RAW_DIRNAME_REGEX);

        
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
                    Matcher rawTFilenameMatcher = RAW_T_FILENAME_PATTERN.matcher(name);
                    if (rawTFilenameMatcher.matches()) {
                        rawFilename = name;
                    } 
                }
                
                return rawFilename;
            }
            
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
        
        private static class VerifiedFromDirSelector extends AbstractFromDirSelector {
            
            VerifiedFromDirSelector() {
                String[] dirRegexes = new String[] {VERIFIED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {PARSED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexFileFilter(dirRegexes);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            
            VerifiedFromDirSelector(String[] patterns) {
                String[] dirRegexes = new String[] {VERIFIED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {PARSED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexContainsFileFilter(dirRegexes, patterns);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            
        }
    
        private static class ProcessedFromDirSelector extends AbstractFromDirSelector {
            
            ProcessedFromDirSelector() {
                String[] dirRegexes = new String[] {PROCESSED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {PARSED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexFileFilter(dirRegexes);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);

            }
            
            ProcessedFromDirSelector(String[] patterns) {
                String[] dirRegexes = new String[] {PROCESSED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {PARSED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexContainsFileFilter(dirRegexes, patterns);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }

        }
        
        private static class VerifiedOrProcessedFromDirSelector extends AbstractFromDirSelector {
            VerifiedOrProcessedFromDirSelector() {
                String[] dirRegexes = new String[] {PROCESSED_DIRNAME_REGEX, VERIFIED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {PARSED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexFileFilter(dirRegexes);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            
            VerifiedOrProcessedFromDirSelector(String[] patterns) {
                String[] dirRegexes = new String[] {PROCESSED_DIRNAME_REGEX, VERIFIED_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {PARSED_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexContainsFileFilter(dirRegexes, patterns);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);

            }
        }
    
        private static class RawFromDirSelector extends AbstractFromDirSelector {
            RawFromDirSelector() {
                String[] dirRegexes = new String[] {RAW_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {RAW_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexFileFilter(dirRegexes);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            
            RawFromDirSelector(String[] patterns) {
                String[] dirRegexes = new String[] {RAW_DIRNAME_REGEX};
                String[] fileRegexes = new String[] {RAW_T_FILENAME_REGEX};
                this.dirFileFilter = new RegexContainsFileFilter(dirRegexes, patterns);
                this.fileFileFilter = new RegexFileFilter(fileRegexes);
            }
            
        }
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
    
        static FromDirSelector getFromDirSelector(String from, String[] patterns) {
            FromDirSelector fromDirSelector = null;
            if (patterns != null && patterns.length > 0) {
                if (ALL.equals(from)) {
                    fromDirSelector = new VerifiedOrProcessedFromDirSelector(patterns);
                } else if (VERIFIED.equals(from)) {
                    fromDirSelector = new VerifiedFromDirSelector(patterns);
                } else if (PROCESSED.equals(from)) {
                    fromDirSelector = new ProcessedFromDirSelector(patterns);
                } else if (RAW.equals(from)) {
                    fromDirSelector = new RawFromDirSelector(patterns);
                }
            } else {
                if (ALL.equals(from)) {
                    fromDirSelector = new VerifiedOrProcessedFromDirSelector();
                } else if (VERIFIED.equals(from)) {
                    fromDirSelector = new VerifiedFromDirSelector();
                } else if (PROCESSED.equals(from)) {
                    fromDirSelector = new ProcessedFromDirSelector();
                } else if (RAW.equals(from)) {
                    fromDirSelector = new RawFromDirSelector();
                }
            }
            
            return fromDirSelector;
        }
    
        static ToDirRenamer  getToDirRenamer(String to) {
            ToDirRenamer toDirRenamer = null;
            if (PROCESSED.equals(to)) {
                toDirRenamer = new ProcessedToDirRenamer();
            } else if (RAW.equals(to)) {
                toDirRenamer = new RawToDirRenamer();
            }
            return toDirRenamer;
        }
    }
    
    
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
            
            fromDirSelector = FromToFactory.getFromDirSelector(from, patterns);
            toDirRenamer = FromToFactory.getToDirRenamer(to);

            
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
    
    private static void printUsage() {
        System.out.println("Usage: RevertDeltaLogs [<-f | --from> <VERIFIED | PROCESSED | ALL>]  [<-t | --to> <PROCESSED | RAW>]  [pattern [pattern ...]]");
        
        System.out.println("       default values:  -f ALL -t RAW");
        
        System.out.println("Usage: RevertDeltaLogs <-h | --help>");
    }

}

