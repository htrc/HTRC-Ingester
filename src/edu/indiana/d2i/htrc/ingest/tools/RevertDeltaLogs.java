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
    
    
    static class PrefixedFileFilter implements FileFilter {
        private final String[] prefixes;
        
        PrefixedFileFilter(String[] prefixes) {
            this.prefixes = prefixes.clone();
        }

        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            boolean result = true;
            for (String prefix : prefixes) {
                result = result && name.startsWith(prefix);
                if (!result) break;
            }
            return result;
        }
    }
    
    static class PatternedFileFilter implements FileFilter {
        private final String[] patterns;
        
        PatternedFileFilter(String[] patterns) {
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
    
    static class PrefixedPatternedFileFilter implements FileFilter {
        private final PrefixedFileFilter prefixedFileFilter;
        private final PatternedFileFilter patternedFileFilter;
        
        PrefixedPatternedFileFilter(String[] prefixes, String[] patterns) {
            this.prefixedFileFilter = new PrefixedFileFilter(prefixes);
            this.patternedFileFilter = new PatternedFileFilter(patterns);
        }

        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File pathname) {
            return (prefixedFileFilter.accept(pathname) && patternedFileFilter.accept(pathname));
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
        
        private static final String VERIFIED_PREFIX = "_VERIFIED_";
        private static final String PROCESSED_PREFIX = "_PROCESSED_";
        private static final String PARSED_T_PREFIX = "_PARSED_t";
        private static final String PARSED_PREFIX = "_PARSED_";
        private static final String RAW_FILENAME_PATTERN = "t-\\d+-\\d+.txt";
        private static final String RAW_DIRNAME_PATTERN = "dlog-\\d{8}-\\d{9}";

        
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
                
                if (name.startsWith(PARSED_PREFIX)) {
                    rawFilename = name.substring(PARSED_PREFIX.length());
                } else if (name.matches(RAW_FILENAME_PATTERN)) {
                    rawFilename = name;
                } 
                
                return rawFilename;
            }
            
            @Override
            public String getRawDirname(File file) {
                String name = file.getName();
                String rawDirname = null;
                
                if (name.startsWith(PROCESSED_PREFIX)) {
                    rawDirname = name.substring(PROCESSED_PREFIX.length());
                } else if (name.startsWith(VERIFIED_PREFIX)) {
                    rawDirname = name.substring(VERIFIED_PREFIX.length());
                } else if (name.matches(RAW_DIRNAME_PATTERN)) {
                    rawDirname = name;
                }
                return rawDirname;
            }
            
        }
        
        private static class VerifiedFromDirSelector extends AbstractFromDirSelector {
            
            VerifiedFromDirSelector() {
                String[] dirPrefixes = new String[] {VERIFIED_PREFIX};
                String[] filePrefixes = new String[] {PARSED_T_PREFIX};
                this.dirFileFilter = new PrefixedFileFilter(dirPrefixes);
                this.fileFileFilter = new PrefixedFileFilter(filePrefixes);
            }
            
            VerifiedFromDirSelector(String[] patterns) {
                String[] dirPrefixes = new String[] {VERIFIED_PREFIX};
                String[] filePrefixes = new String[] {PARSED_T_PREFIX};
                this.dirFileFilter = new PrefixedPatternedFileFilter(dirPrefixes, patterns);
                this.fileFileFilter = new PrefixedFileFilter(filePrefixes);
            }
            
        }
    
        private static class ProcessedFromDirSelector extends AbstractFromDirSelector {
            
            ProcessedFromDirSelector() {
                String[] dirPrefixes = new String[] {PROCESSED_PREFIX};
                String[] filePrefixes = new String[] {PARSED_T_PREFIX};
                this.dirFileFilter = new PrefixedFileFilter(dirPrefixes);
                this.fileFileFilter = new PrefixedFileFilter(filePrefixes);

            }
            
            ProcessedFromDirSelector(String[] patterns) {
                String[] dirPrefixes = new String[] {PROCESSED_PREFIX};
                String[] filePrefixes = new String[] {PARSED_T_PREFIX};
                this.dirFileFilter = new PrefixedPatternedFileFilter(dirPrefixes, patterns);
                this.fileFileFilter = new PrefixedFileFilter(filePrefixes);
            }

        }
        
        private static class VerifiedOrProcessedFromDirSelector extends AbstractFromDirSelector {
            VerifiedOrProcessedFromDirSelector() {
                String[] dirPrefixes = new String[] {PROCESSED_PREFIX, VERIFIED_PREFIX};
                String[] filePrefixes = new String[] {PARSED_T_PREFIX};
                this.dirFileFilter = new PrefixedFileFilter(dirPrefixes);
                this.fileFileFilter = new PrefixedFileFilter(filePrefixes);
            }
            
            VerifiedOrProcessedFromDirSelector(String[] patterns) {
                String[] dirPrefixes = new String[] {PROCESSED_PREFIX, VERIFIED_PREFIX};
                String[] filePrefixes = new String[] {PARSED_T_PREFIX};
                this.dirFileFilter = new PrefixedPatternedFileFilter(dirPrefixes, patterns);
                this.fileFileFilter = new PrefixedFileFilter(filePrefixes);

            }
        }
    
        private static class ProcessedToDirRenamer implements ToDirRenamer {

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
                }
            } else {
                if (ALL.equals(from)) {
                    fromDirSelector = new VerifiedOrProcessedFromDirSelector();
                } else if (VERIFIED.equals(from)) {
                    fromDirSelector = new VerifiedFromDirSelector();
                } else if (PROCESSED.equals(from)) {
                    fromDirSelector = new ProcessedFromDirSelector();
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
                } else {
                    targetSet.add(args[i]);
                }
                
                i++;
            }
            
            if (!validFromSet.contains(from)) {
                throw new IllegalArgumentException("Invalid \"from\" argument: " + from);
            }
            if (!validToSet.contains(to)) {
                throw new IllegalArgumentException("Invalid \"to\" argument: " + to);
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

