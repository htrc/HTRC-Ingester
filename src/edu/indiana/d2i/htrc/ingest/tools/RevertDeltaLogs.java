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
import java.util.Set;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.PropertyReader;

/**
 * @author Yiming Sun
 *
 */
public class RevertDeltaLogs {
    
    static enum FromEnum {
        TARGETED (null),
        VERIFIED (new VerifiedFileFilter()),
        PROCESSED (new ProcessedFileFilter()),
        ALL (new VerifiedOrProcessedFileFilter());
        
        private final StartPatternFileFilter fileFilter;
        
        FromEnum(StartPatternFileFilter fileFilter) {
            this.fileFilter = fileFilter;
        }
        
        StartPatternFileFilter getFileFilter() {
            return this.fileFilter;
        }
        
        
    }
    
    static enum ToEnum {
        PROCESSED ("_PROCESSED_"),
        RAW ("");
        
        private final String prefix;
        
        ToEnum(String prefix) {
            this.prefix = prefix;
        }
        
        String getPrefix() {
            return this.prefix;
        }
    }


    static interface StartPatternFileFilter extends FileFilter {
        public Set<String> getStartPatternSet();
    }
    

    static class TargetedFileFilter implements StartPatternFileFilter {
        private final Set<String> targetedPatternSet;

        private final Set<String> startPatternSet;
        
        TargetedFileFilter(Set<String> patternSet) {
            this.targetedPatternSet = patternSet;
            this.startPatternSet = new HashSet<String>();
            startPatternSet.add("_PROCESSED_");
            startPatternSet.add("_VERIFIED_");
        }
        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            for (String pattern : targetedPatternSet) {
                if (name.contains(pattern)) {
                    return true;
                }
            }
            return false;
        }
        /**
         * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.StartPatternFileFilter#getStartPattern()
         */
        @Override
        public Set<String> getStartPatternSet() {
            return this.startPatternSet;
        }
        
    }
    
    static class ProcessedFileFilter implements StartPatternFileFilter {
        private static final String PROCESSED_DLOG_PATTERN = "_PROCESSED_dlog-\\d{8}-\\d{9}";
        
        private final Set<String> startPatternSet;
        
        ProcessedFileFilter() {
            this.startPatternSet = new HashSet<String>();
            startPatternSet.add("_PROCESSED_");
        }
        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            if (name.matches(PROCESSED_DLOG_PATTERN)) {
                return true;
            }
            return false;
        }
        
        /**
         * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.StartPatternFileFilter#getStartPattern()
         */
        @Override
        public Set<String> getStartPatternSet() {
            return this.startPatternSet;
        }
    }
    
    static class VerifiedFileFilter implements StartPatternFileFilter {
        private static final String VERIFIED_DLOG_PATTERN = "_VERIFIED_dlog-\\d{8}-\\d{9}";
        
        private final Set<String> startPatternSet;
        
        VerifiedFileFilter() {
            this.startPatternSet = new HashSet<String>();
            startPatternSet.add("_VERIFIED_");
        }
        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            if (name.matches(VERIFIED_DLOG_PATTERN)) {
                return true;
            }
            return false;
        }
        /**
         * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.StartPatternFileFilter#getStartPattern()
         */
        @Override
        public Set<String> getStartPatternSet() {
            return this.startPatternSet;
        }
    }
    
    static class VerifiedOrProcessedFileFilter implements StartPatternFileFilter {
        private static final String PROCESSED_DLOG_PATTERN = "_PROCESSED_dlog-\\d{8}-\\d{9}";
        private static final String VERIFIED_DLOG_PATTERN = "_VERIFIED_dlog-\\d{8}-\\d{9}";
        
        private final Set<String> startPatternSet;
        
        /**
         * 
         */
        VerifiedOrProcessedFileFilter() {
            this.startPatternSet = new HashSet<String>();
            startPatternSet.add("_PROCESSED_");
            startPatternSet.add("_VERIFIED_");
        }
        
        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            if (name.matches(PROCESSED_DLOG_PATTERN) || name.matches(VERIFIED_DLOG_PATTERN)) {
                return true;
            }
            return false;
        }

        /**
         * @see edu.indiana.d2i.htrc.ingest.tools.RevertDeltaLogs.StartPatternFileFilter#getStartPattern()
         */
        @Override
        public Set<String> getStartPatternSet() {
            return startPatternSet;
        }
    }

    
//    static class TargetedDLogDirFileFilter implements FileFilter {
//        private boolean targeted;
//        private HashSet<String> targetedSet;
//
//        // _PROCESSED_dlog-20110906-111750523
//
//        private static final String PROCESSED_DLOG_PATTERN = "_PROCESSED_dlog-\\d{8}-\\d{9}";
//        TargetedDLogDirFileFilter(boolean targeted, HashSet<String> targetedSet) {
//            this.targeted = targeted;
//            this.targetedSet = targetedSet;
//        }
//
//        /**
//         * @see java.io.FileFilter#accept(java.io.File)
//         */
//        @Override
//        public boolean accept(File pathname) {
//            if (pathname.isDirectory()) {
//                String name = pathname.getName();
//                if (!targeted) {
//                    return (name.matches(PROCESSED_DLOG_PATTERN));
//                } else {
//                    return targetedSet.contains(name);
//                }
//            }
//            return false;
//        }
//        
//        
//        
//    } 
    
    static class ParsedTFileFilter implements FileFilter {

        private static final String PARSED_T_PATTERN = "_PARSED_t-\\d+-\\d+.txt";
        /**
         * @see java.io.FileFilter#accept(java.io.File)
         */
        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            return name.matches(PARSED_T_PATTERN);
        }
        
    }
    
    public static void main (String[] args) {
//        boolean targeted = false;

        HashSet<String> targetSet = new HashSet<String>();
        
        FromEnum from = FromEnum.ALL;
        ToEnum to = ToEnum.RAW;
        
        try {
            int i = 0;
            while (i < args.length) {
                if ("-f".equals(args[i]) || "--from".equals(args[i])) {
                    i++;
                    from = FromEnum.valueOf(args[i]);
                } else if ("-t".equals(args[i]) || "--to".equals(args[i])) {
                    i++;
                    to = ToEnum.valueOf(args[i]);
                } else {
                    targetSet.add(args[i]);
                    from = FromEnum.TARGETED;
                }
                
                i++;
            }
                
        
//        if (args != null && args.length > 0) {
//            targeted = true;
//            for (String arg : args) {
//                targetSet.add(arg);
//            }
//        }
        
            PropertyReader propertyReader = PropertyReader.getInstance();
            String deltaLogRootPath = propertyReader.getProperty(Constants.PK_DELTA_LOG_ROOT);
            
            File deltaLogRoot = new File(deltaLogRootPath);
            
            StartPatternFileFilter fileFilter = (FromEnum.TARGETED.equals(from)) ? new TargetedFileFilter(targetSet) : from.getFileFilter();
    
            Set<String> startPatternSet = fileFilter.getStartPatternSet();
            
            String toPrefix = to.getPrefix();
            
            File[] listFiles = deltaLogRoot.listFiles(fileFilter);
            
            FileFilter parsedDlogFilter = new ParsedTFileFilter();
            
            for (File file : listFiles) {
                File[] tfilesList = file.listFiles(parsedDlogFilter);
                for (File tfile : tfilesList) {
                    File originalFile = new File(tfile.getParent(), tfile.getName().substring("_PARSED_".length()));
                    boolean renameTo = tfile.renameTo(originalFile);
                    if (!renameTo) {
                        System.err.println("Failed to restore file " + tfile.getPath());
                    } else {
                        System.out.println("Renamed " + tfile.getPath() + " to " + originalFile.getPath());
                    }
                }
                
                
                for (String startPattern : startPatternSet) {
                    String dirName = file.getName();
                    
                    if (dirName.startsWith(startPattern)) {
                        File renamedDir = new File(file.getParent(), toPrefix + dirName.substring(startPattern.length()));
                        boolean renameTo = file.renameTo(renamedDir);
                        if (!renameTo) {
                            System.err.println("Failed to revert dir " + file.getPath() + " to " + renamedDir.getPath());
                        } else {
                            System.out.println("Reverted " + file.getPath() + " to " + renamedDir.getPath());
                        }
                        break;
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            printUsage();
        }
    }
    
    private static void printUsage() {
        System.out.println("Usage: RevertDeltaLogs [<-f | --from> <VERIFIED | PROCESSED | ALL>]  [<-t | --to> <PROCESSED | RAW>]  [pattern [pattern ...]]");
        System.out.println("       default values:  -f ALL -t RAW");
    }

}

