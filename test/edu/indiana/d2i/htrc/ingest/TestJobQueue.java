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
# File:  TestJobQueue.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/



/**
 * 
 */
package edu.indiana.d2i.htrc.ingest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Yiming Sun
 *
 */
public class TestJobQueue {
    
    private JobQueue<String> jobQueue = null;
    
    @Before
    public void setupJobQueue() {
        this.jobQueue = new JobQueue<String>("String Queue");
    }
    
    @After
    public void teardownJobQueue() {
        this.jobQueue = null;
    }
    
    @Test
    public void testEnqueue() {
        String job = "Job1";
        
        jobQueue.enqueue(job);
        String dequeue = jobQueue.dequeue();
        
        Assert.assertEquals("dequeued mismatch enqueued", job, dequeue);
        
    }
    
    @Test
    public void testMarkDone() {
        String job = "Job2";
        
        jobQueue.enqueue(job);
        jobQueue.markDone();
        
        jobQueue.dequeue();
        boolean done = jobQueue.isDone();
        Assert.assertTrue("should be done", done);
    }
    
    @Test
    public void testMarkDoneNotEmpty() {
        String job = "Job3";
        jobQueue.enqueue(job);
        jobQueue.markDone();
        
        boolean done = jobQueue.isDone();
        Assert.assertFalse("queue not empty, should not be done", done);
    }
 
    @Test
    public void testReset() {
        String job = "Job4";
        jobQueue.enqueue(job);
        jobQueue.markDone();
        jobQueue.dequeue();
        jobQueue.reset();
        boolean done = jobQueue.isDone();
        Assert.assertFalse("queue reset, should not be done", done);
    }
    
    @Test
    public void testEnqueueAfterReset() {
        String job = "Job5";
        String job2 = "Job6";
        
        jobQueue.enqueue(job);
        jobQueue.markDone();
        jobQueue.dequeue();
        jobQueue.reset();
        jobQueue.enqueue(job2);
        
        String dequeue = jobQueue.dequeue();
        Assert.assertEquals("dequeued mismatch enqueued", job2, dequeue);
    }
    
    @Test(expected=IllegalStateException.class)
    public void testResetError() {
        String job = "Job7";
        jobQueue.enqueue(job);
        jobQueue.markDone();
        
        jobQueue.reset();
    }
}

