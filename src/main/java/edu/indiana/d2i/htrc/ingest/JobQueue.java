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
# File:  JobQueue.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * @author Yiming Sun
 *
 */
public class JobQueue<T> {
    
    private static Logger log = Logger.getLogger(JobQueue.class);
    
    private String queueName;
    private List<T> queue;
    private boolean isDone;
    
    
    public JobQueue(String queueName) {
        this.queueName = queueName;
        this.queue = new LinkedList<T>();
        this.isDone = false;
    }
    
   
    public void enqueue(T job) {
        
        synchronized(queue) {
            queue.add(job);
            queue.notify();
        }
    }
    
    public T dequeue() {
        T job = null;
        synchronized(queue) {
            if (queue.isEmpty()) {
                if (!isDone) {
                    try {
                        queue.wait();
                    } catch (InterruptedException e) {
                        log.warn("Dequeue blocking wait on JobQueue " + queueName + " interrupted", e);
                    }
                }
            }
            if (!queue.isEmpty()) {
                job = queue.remove(0);
            }
            
        }
        return job;
    }
    
    public boolean isDone() {
        boolean result = false;
        synchronized(queue) {
            result = isDone && (queue.isEmpty());
        }
        return result;
    }
    
    public void markDone() {
        synchronized(queue) {
            isDone = true;
            queue.notifyAll();
        }
    }
    
    public void reset() throws IllegalStateException {
        synchronized(queue) {
            if (this.queue.isEmpty()) {
                isDone = false;
            } else {
                throw new IllegalStateException("Jobs remain in queue.  Cannot reset");
            }
        }
    }
}    

