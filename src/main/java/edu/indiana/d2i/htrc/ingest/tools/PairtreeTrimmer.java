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
# File:  PairtreeTrimmer.java
# Description: This class is a tool to trim the Pairtree when the number of Ingester instance has changed or the
#    current instance's hash key has changed

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
 * This class is a tool to trim the Pairtree when the number of Ingester instance has changed or the
 * current instance's hash key has changed
 * @author Yiming Sun
 *
 */
public class PairtreeTrimmer {
    
    /**
     * Possible action: keep the subtree or delete the subtree
     * @author Yiming Sun
     *
     */
    static enum ActionEnum {
        KEEP,
        DELETE;
    }
    
    /**
     * This class represents a Pairtree node and the action to be done on the node
     * @author Yiming Sun
     *
     */
    static class ActionableNode {
        private String name;
        private ActionEnum action;
        
        /**
         * Constructor
         * @param name name of the node
         */
        public ActionableNode(String name) {
            this.name = name;
            this.action = ActionEnum.KEEP;
        }
        
        /**
         * Method to set the action to be done to the node
         * @param action the action to be done to the node
         */
        public void setAction(ActionEnum action) {
            this.action = action;
        }
        
        /**
         * Method to get the name of the node
         * @return the name of the node
         */
        public String getName() {
            return name;
        }
        /**
         * Method to get the action to be done to the node
         * @return the action to be done to the node
         */
        public ActionEnum getAction() {
            return action;
        }
    }
    
    /**
     * This class is a generic tree node structure
     * @author Yiming Sun
     *
     * @param <T> parameter type of the tree nodes
     */
    static class Tree<T> {
        private T data;
        private Map<String, Tree<T>> children;
        
        /**
         * Constructor
         * @param data data to be stored in the current node
         */
        public Tree (T data) {
            this.data = data;
            this.children = new HashMap<String, Tree<T>>();
        }
        
        /**
         * Method to get the data stored in the current node
         * @return the data stored in the current node
         */
        public T getData() {
            return data;
        }
        
        /**
         * Method to get a child of this node by its ID
         * @param id ID of a child node
         * @return the child node identified by the ID, or <code>null</code> if no child by that ID
         */
        public Tree<T> getChildByID(String id) {
            return children.get(id);
        }
        
        /**
         * Method to add a child node to the current node
         * @param id ID to identifiy the child
         * @param data data to be stored in the child
         */
        public void addChild(String id, Tree<T> data) {
            children.put(id, data);
        }
        
        /**
         * Method to get a List of children Tree nodes
         * @return a Iterator of Tree objects
         */
        public Iterator<Tree<T>> getAllChildren() {
            return children.values().iterator();
        }
        
        /**
         * Method to remove the child identified by the ID
         * @param id ID of the child to be removed
         */
        public void removeChild(String id) {
            children.remove(id);
        }
        /**
         * Method to check if the current node is a leaf node
         * @return <code>true</code> if the current node has no children (is a leaf node), <code>false</code> otherwise
         */
        public boolean isLeaf() {
            return children.isEmpty();
        }
    }
    
    private int myHashKey = -1;
    private BigInteger hashSpace = null;
    
    private String rsyncPointsPath = null;
    
    private String destRoot = null;
    
    private String sourceCleanseTarget = null;
    
    private List<String> extraFilePaths = null;
    
    private Tree<String> keepTree = null;
    private Tree<ActionableNode> deleteTree = null;

    /**
     * Constructor
     */
    public PairtreeTrimmer() {
        readProperties();
    }
    
    /**
     * Method to trim the Pairtree
     * @throws Exception thrown if anything failed
     */
    public void trim() throws Exception {
        buildKeepTree();
        buildDeleteTree();
        trimPairtree();
        
    }

    /**
     * Method to read properties from properties file
     */
    private void readProperties() {
        PropertyReader propertyReader = PropertyReader.getInstance();
        
        int hashSpaceInt = Integer.parseInt(propertyReader.getProperty(Constants.PK_INGEST_HASH_SPACE));
        
        assert(hashSpaceInt > 0);
        
        myHashKey = Integer.parseInt(propertyReader.getProperty(Constants.PK_INGEST_MY_HASH_KEY));
        
        assert(myHashKey >= 0 && myHashKey < hashSpaceInt);
        
        hashSpace = BigInteger.valueOf(hashSpaceInt);
        
        sourceCleanseTarget = propertyReader.getProperty(Constants.PK_SOURCE_CLEANSE_TARGET);
        
        if (sourceCleanseTarget == null) {
            sourceCleanseTarget = "";
        } else {
            sourceCleanseTarget = sourceCleanseTarget.trim();
        }
        
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

    /**
     * Method to build a Tree representing branches to be kept
     * @throws FileNotFoundException thrown if the special rsync points file does not exist
     * @throws IOException thrown if the reading of the rsync points file failed
     */
    private void buildKeepTree() throws FileNotFoundException, IOException {
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
                    addPathToTree(keepTree, cleanedSource);
                }
            }
        } while (line != null);
        
        bufferedReader.close();
        

    }
    
   
    /**
     * Method to add a path to a Tree
     * @param tree a Tree to which the path is to be added
     * @param path the path to be added to the Tree
     */
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
    
    /**
     * Method to build the Tree representing branches to be deleted. It wraps the more detailed method with the same name
     */
    private void buildDeleteTree() {
        
        deleteTree = new Tree<ActionableNode>(new ActionableNode("delete"));
        
        File destRootFile = new File(destRoot);
        
        buildDeleteTree(keepTree, deleteTree, destRootFile);
        
        
    }
    
    /**
     * Method to check the keep tree and the delete tree and determine if the current directory should be deleted
     * @param currentKeepNode the current keep tree
     * @param currentDeleteNode the current delete tree
     * @param currentFile the current Pairtree node directory to work on
     */
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
    
    /**
     * This is a high level method to carry out the trim operation
     */
    private void trimPairtree() {
        String pathSoFar = destRoot;
        trimPairtree(pathSoFar, deleteTree);
    }
    
    /**
     * This method checks the current path in the Pairtree and the current node to determine if the current node should be trimmed or kept
     * @param pathSoFar the current path
     * @param currentNode the current node in the delete tree
     */
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
    
    /**
     * Method to use linux command "rm -rf" to remove directory and all its subdirectories 
     * @param pathSoFar the current path in the Pairtree
     * @param data the data in the current node
     * @throws IOException thrown if the linux command failed
     * @throws InterruptedException thrown if the linux command failed
     */
    private void rmrf(String pathSoFar, ActionableNode data) throws IOException, InterruptedException {
        String[] command = {"rm", "-rf", pathSoFar + data.getName()};
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(command);
        process.waitFor();
        
    }
    
    /**
     * main method
     * @param args argument list passed in by the system
     * @throws Exception thrown if anything failed
     */
    public static void main(String[] args) throws Exception {
        PairtreeTrimmer trimmer = new PairtreeTrimmer();
        trimmer.trim();
    }
}

