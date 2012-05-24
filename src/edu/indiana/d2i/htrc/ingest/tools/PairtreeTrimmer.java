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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: HTRC-Ingester
# File:  PairtreeTrimmer.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import edu.indiana.d2i.htrc.ingest.Constants;
import edu.indiana.d2i.htrc.ingest.PropertyReader;
import edu.indiana.d2i.htrc.ingest.Util;

/**
 * @author Yiming Sun
 *
 */
public class PairtreeTrimmer {
    
    static enum ActionEnum {
        KEEP,
        DELETE;
    }
    
    static class ActionableNode {
        private String name;
        private ActionEnum action;
        
        public ActionableNode(String name) {
            this.name = name;
            this.action = ActionEnum.KEEP;
        }
        
        public void setAction(ActionEnum action) {
            this.action = action;
        }
        
        public String getName() {
            return name;
        }
        
        public ActionEnum getAction() {
            return action;
        }
    }
    
    static class Tree<T> {
        private T data;
        private Map<String, Tree<T>> children;
        
        public Tree (T data) {
            this.data = data;
            this.children = new HashMap<String, Tree<T>>();
        }
        
        public T getData() {
            return data;
        }
        
        public Tree<T> getChildByID(String id) {
            return children.get(id);
        }
        
        public void addChild(String id, Tree<T> data) {
            children.put(id, data);
        }
        
        public Iterator<Tree<T>> getAllChildren() {
            return children.values().iterator();
        }
        
        public void removeChild(String id) {
            children.remove(id);
        }
        
        public boolean isLeaf() {
            return children.isEmpty();
        }
    }
    
    private int myHashKey = -1;
    private BigInteger hashSpace = null;
    
    private String rsyncPointsPath = null;
    
    private String destRoot = null;
    
    private String sourceCleanseTarget = null;
//    private String sourceCleanseReplacement = null;
    
    
    private List<String> extraFilePaths = null;
    
    private Tree<String> keepTree = null;
    private Tree<ActionableNode> deleteTree = null;
    
    public PairtreeTrimmer() {
        readProperties();
    }
    
    public void trim() throws Exception {
        buildKeepTree();
        buildDeleteTree();
        trimPairtree();
        
    }

    
    private void readProperties() {
        PropertyReader propertyReader = PropertyReader.getInstance();
        
        int hashSpaceInt = Integer.parseInt(propertyReader.getProperty(Constants.PK_INGEST_HASH_SPACE));
        
        assert(hashSpaceInt > 0);
        
        myHashKey = Integer.parseInt(propertyReader.getProperty(Constants.PK_INGEST_MY_HASH_KEY));
        
        assert(myHashKey >= 0 && myHashKey < hashSpaceInt);
        
        hashSpace = BigInteger.valueOf(hashSpaceInt);
        
        sourceCleanseTarget = propertyReader.getProperty(Constants.PK_SOURCE_CLEANSE_TARGET);
//        sourceCleanseReplacement = propertyReader.getProperty(Constants.PK_SOURCE_CLEANSE_REPLACEMENT);
        
        if (sourceCleanseTarget == null) {
            sourceCleanseTarget = "";
        } else {
            sourceCleanseTarget = sourceCleanseTarget.trim();
        }
        
//        if (sourceCleanseReplacement == null) {
//            sourceCleanseReplacement = "";
//        } else {
//            sourceCleanseReplacement = sourceCleanseReplacement.trim();
//        }

        
        destRoot = Util.addTrailingSlash(propertyReader.getProperty(Constants.PK_RSYNC_DEST_ROOT));
        
        extraFilePaths = new ArrayList<String>();
        
        int extraFilesCount = Integer.parseInt(propertyReader.getProperty(Constants.PK_RSYNC_EXTRA_FILES_COUNT));
        
        for (int i = 0; i < extraFilesCount; i++) {
            String extraFilePath = Util.cleanseSource(propertyReader.getProperty(Constants.PKN_RSYNC_EXTRA_FILE_PATH + (i + 1)), sourceCleanseTarget, null);
            extraFilePaths.add(extraFilePath);
        }

        rsyncPointsPath = Util.cleanseSource(propertyReader.getProperty(Constants.PK_RSYNC_TREE_SOURCE_PATH), sourceCleanseTarget, null);
        
        System.out.println("rysncPoints path: " + rsyncPointsPath);
        
        
    }
    
    private void buildKeepTree() throws FileNotFoundException, IOException {
//        int index = 0;
//        if (sourceCleanseReplacement != null) {
//            index = sourceCleanseReplacement.length();
//        }
        keepTree = new Tree<String>("keep");

        addPathToTree(keepTree, rsyncPointsPath);
        
        for (String extraFile : extraFilePaths) {
            addPathToTree(keepTree, extraFile);
        }
        
        BufferedReader bufferedReader = new BufferedReader(new FileReader(destRoot + rsyncPointsPath));
        String line = null;
        do {
            line = bufferedReader.readLine();
            if (line != null) {
                int hash = Util.hashMD5Checksum(line, hashSpace);
                if (hash == myHashKey) {
                    String cleanedSource = Util.addTrailingSlash(Util.cleanseSource(line, sourceCleanseTarget, null));
//                    sourcePaths.add(cleanedSource);
                    addPathToTree(keepTree, cleanedSource);
                }
            }
        } while (line != null);
        
        bufferedReader.close();
        

    }
    
   
    
    private void addPathToTree(Tree<String> tree, String path) {
        StringTokenizer tokenizer = new StringTokenizer(path, "/");
        Tree<String> current = tree;
        while (tokenizer.hasMoreElements()) {
            String token = tokenizer.nextToken();
            Tree<String> child = tree.getChildByID(token);
            if (child == null) {
                child = new Tree<String>(token);
                current.addChild(token, child);
            }
            current = child;
        }
    }
    
    private void buildDeleteTree() {
        
        deleteTree = new Tree<ActionableNode>(new ActionableNode("delete"));
        
        File destRootFile = new File(destRoot);
        
        buildDeleteTree(keepTree, deleteTree, destRootFile);
        
        
    }
    
    private void buildDeleteTree(Tree<String> currentKeepNode, Tree<ActionableNode> currentDeleteNode, File currentFile) {
        File[] files = currentFile.listFiles();
        
        for (File file : files) {
            String name = file.getName();
            
            Tree<String> child = currentKeepNode.getChildByID(name);
            
            // if this directory/file is not found in the keep tree, mark it to be deleted, and all directory subtree should be deleted as well, so no need to traverse further into the filesystem
            if (child == null) {
                ActionableNode data = new ActionableNode(name);
                data.setAction(ActionEnum.DELETE);
                Tree<ActionableNode> node = new Tree<ActionableNode>(data);
                currentDeleteNode.addChild(name, node);
            } else {
                // if this directory/file is found in the keep tree, then it depends:
                
                // if this directory/file is a leaf node in the keep tree, then all subdirectories below should be kept, no need to add anything to the delete tree
                if (child.isLeaf()) {
                    // do nothing
                } else {
                // if this directory/file is not a leaf node in the keep tree, we need to go further down
                    ActionableNode data = new ActionableNode(name);  // default action is KEEP
                    Tree<ActionableNode> node = new Tree<ActionableNode>(data);
                    currentDeleteNode.addChild(name, node);
                    buildDeleteTree(child, node, file);
                }
            }
        }
        
    }
    
    private void trimPairtree() {
        String pathSoFar = destRoot;
        trimPairtree(pathSoFar, deleteTree);
    }
    
    private void trimPairtree(String pathSoFar, Tree<ActionableNode> currentNode) {
        Iterator<Tree<ActionableNode>> allChildren = currentNode.getAllChildren();

        while (allChildren.hasNext()) {
            Tree<ActionableNode> next = allChildren.next();
            
            ActionableNode data = next.getData();
            
            ActionEnum action = data.getAction();
            
            if (ActionEnum.DELETE.equals(action)) {
                try {
                    rmrf(pathSoFar, data);
                    System.out.println("successfully rm -rf " + pathSoFar + data.getName());
                } catch (IOException e) {
                    System.err.println("IOException trying to rm -rf " + pathSoFar + data.getName());
                    e.printStackTrace(System.err);
                } catch (InterruptedException e) {
                    System.err.println("Interrupted Exception trying to rm -rf " + pathSoFar + data.getName());
                    e.printStackTrace(System.err);
                }
            } else {
                if (!next.isLeaf()) {
                    trimPairtree(pathSoFar + data.getName() + "/", next);
                }
            }
            
        }
    }
    
    
    private void rmrf(String pathSoFar, ActionableNode data) throws IOException, InterruptedException {
        String[] command = {"rm", "-rf", pathSoFar + data.getName()};
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(command);
        process.waitFor();
        
    }
    
    public static void main(String[] args) throws Exception {
        PairtreeTrimmer trimmer = new PairtreeTrimmer();
        trimmer.trim();
    }
}

